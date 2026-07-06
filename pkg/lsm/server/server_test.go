package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"lsmengine/pkg/lsm"
	"lsmengine/pkg/lsm/errs"
)

type stubProvider struct{}

func (stubProvider) Stats() lsm.Stats {
	return lsm.Stats{MemtableBytes: 1, MemtableEntries: 2}
}

func (stubProvider) Health() lsm.Health {
	return lsm.Health{Ready: true, Reason: "ok"}
}

type controlStubProvider struct {
	stubProvider
	mu    sync.Mutex
	state struct {
		status lsm.ClusterStatus
		shards []lsm.ShardStatus
		ops    map[string]string
	}
}

type writeStubProvider struct {
	stubProvider
	mu        sync.Mutex
	data      map[string][]byte
	putErr    error
	deleteErr error
	putGate   chan struct{}
}

type writeControlStubProvider struct {
	control *controlStubProvider
	write   *writeStubProvider
}

type cdcStubProvider struct {
	stubProvider
	result  lsm.CDCReadResult
	lastReq struct {
		shardID string
		offset  uint64
		limit   int
	}
}

type raftStubProvider struct {
	stubProvider
	mu       sync.Mutex
	messages []lsm.RaftPeerMessage
	err      error
}

func newWriteControlStubProvider() *writeControlStubProvider {
	return &writeControlStubProvider{
		control: newControlStubProvider(),
		write:   newWriteStubProvider(),
	}
}

func (p *cdcStubProvider) ReadCDCEvents(shardID string, offset uint64, limit int) (lsm.CDCReadResult, error) {
	p.lastReq.shardID = shardID
	p.lastReq.offset = offset
	p.lastReq.limit = limit
	return p.result, nil
}

func (p *raftStubProvider) HandlePeerMessages(_ context.Context, messages []lsm.RaftPeerMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages = append(p.messages, messages...)
	return p.err
}

func (p *raftStubProvider) messagesCopy() []lsm.RaftPeerMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]lsm.RaftPeerMessage, len(p.messages))
	copy(out, p.messages)
	return out
}

func (p *writeControlStubProvider) Stats() lsm.Stats   { return p.write.Stats() }
func (p *writeControlStubProvider) Health() lsm.Health { return p.write.Health() }
func (p *writeControlStubProvider) ClusterStatus() lsm.ClusterStatus {
	return p.control.ClusterStatus()
}
func (p *writeControlStubProvider) Shards() []lsm.ShardStatus { return p.control.Shards() }
func (p *writeControlStubProvider) TransferLeader(shardID, target string) error {
	return p.control.TransferLeader(shardID, target)
}
func (p *writeControlStubProvider) TriggerSplit(shardID string, splitKey []byte) error {
	return p.control.TriggerSplit(shardID, splitKey)
}
func (p *writeControlStubProvider) TriggerRebalance(shardID, target string) error {
	return p.control.TriggerRebalance(shardID, target)
}
func (p *writeControlStubProvider) PrepareDrain(nodeID string) error {
	return p.control.PrepareDrain(nodeID)
}
func (p *writeControlStubProvider) TransferLeaderWithOptions(shardID, target string, opts lsm.ControlWriteOptions) error {
	return p.control.TransferLeaderWithOptions(shardID, target, opts)
}
func (p *writeControlStubProvider) TriggerSplitWithOptions(shardID string, splitKey []byte, opts lsm.ControlWriteOptions) error {
	return p.control.TriggerSplitWithOptions(shardID, splitKey, opts)
}
func (p *writeControlStubProvider) TriggerRebalanceWithOptions(shardID, target string, opts lsm.ControlWriteOptions) error {
	return p.control.TriggerRebalanceWithOptions(shardID, target, opts)
}
func (p *writeControlStubProvider) PrepareDrainWithOptions(nodeID string, opts lsm.ControlWriteOptions) error {
	return p.control.PrepareDrainWithOptions(nodeID, opts)
}
func (p *writeControlStubProvider) Put(key []byte, value []byte) error {
	return p.write.Put(key, value)
}
func (p *writeControlStubProvider) Delete(key []byte) error { return p.write.Delete(key) }

func newWriteStubProvider() *writeStubProvider {
	return &writeStubProvider{
		data: make(map[string][]byte),
	}
}

func (w *writeStubProvider) Put(key []byte, value []byte) error {
	if w.putGate != nil {
		<-w.putGate
	}
	if w.putErr != nil {
		return w.putErr
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.data[string(key)] = append([]byte(nil), value...)
	return nil
}

func (w *writeStubProvider) Delete(key []byte) error {
	if w.deleteErr != nil {
		return w.deleteErr
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.data, string(key))
	return nil
}

func (w *writeStubProvider) Value(key string) ([]byte, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	value, ok := w.data[key]
	return append([]byte(nil), value...), ok
}

type legacyControlStubProvider struct {
	stubProvider
	inner *controlStubProvider
}

func newLegacyControlStubProvider() *legacyControlStubProvider {
	return &legacyControlStubProvider{inner: newControlStubProvider()}
}

func (c *legacyControlStubProvider) ClusterStatus() lsm.ClusterStatus {
	return c.inner.ClusterStatus()
}

func (c *legacyControlStubProvider) Shards() []lsm.ShardStatus {
	return c.inner.Shards()
}

func (c *legacyControlStubProvider) TransferLeader(shardID, target string) error {
	return c.inner.TransferLeader(shardID, target)
}

func (c *legacyControlStubProvider) TriggerSplit(shardID string, splitKey []byte) error {
	return c.inner.TriggerSplit(shardID, splitKey)
}

func (c *legacyControlStubProvider) TriggerRebalance(shardID, target string) error {
	return c.inner.TriggerRebalance(shardID, target)
}

func (c *legacyControlStubProvider) PrepareDrain(nodeID string) error {
	return c.inner.PrepareDrain(nodeID)
}

func newControlStubProvider() *controlStubProvider {
	return &controlStubProvider{
		state: struct {
			status lsm.ClusterStatus
			shards []lsm.ShardStatus
			ops    map[string]string
		}{
			status: lsm.ClusterStatus{
				NodeID:      "node-a",
				ClusterID:   "cluster-dev",
				StorageMode: lsm.StorageModeLocal,
				ShardCount:  1,
				Revision:    0,
			},
			shards: []lsm.ShardStatus{
				{
					ID:       "users",
					StartKey: []byte("a"),
					EndKey:   []byte("z"),
					Leader:   "node-a",
					Replicas: []lsm.ReplicaStatus{
						{NodeID: "node-a", Role: "leader", Healthy: true},
						{NodeID: "node-b", Role: "follower", Healthy: true},
					},
				},
			},
			ops: make(map[string]string),
		},
	}
}

func (c *controlStubProvider) ClusterStatus() lsm.ClusterStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state.status
}

func (c *controlStubProvider) Shards() []lsm.ShardStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]lsm.ShardStatus, len(c.state.shards))
	copy(out, c.state.shards)
	return out
}

func (c *controlStubProvider) TransferLeader(shardID, target string) error {
	return c.TransferLeaderWithOptions(shardID, target, lsm.ControlWriteOptions{})
}

func (c *controlStubProvider) TransferLeaderWithOptions(shardID, target string, opts lsm.ControlWriteOptions) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	applied, err := c.ensureControlWriteOptionsLocked(opts, "transfer:"+shardID+":"+target)
	if err != nil {
		return err
	}
	if applied {
		return nil
	}
	for i := range c.state.shards {
		if c.state.shards[i].ID != shardID {
			continue
		}
		c.state.shards[i].Leader = target
		c.state.status.Revision++
		c.recordOperationLocked(opts, "transfer:"+shardID+":"+target)
		return nil
	}
	return errs.ErrShardNotFound
}

func (c *controlStubProvider) TriggerSplit(shardID string, splitKey []byte) error {
	return c.TriggerSplitWithOptions(shardID, splitKey, lsm.ControlWriteOptions{})
}

func (c *controlStubProvider) TriggerSplitWithOptions(shardID string, splitKey []byte, opts lsm.ControlWriteOptions) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	applied, err := c.ensureControlWriteOptionsLocked(opts, "split:"+shardID+":"+string(splitKey))
	if err != nil {
		return err
	}
	if applied {
		return nil
	}
	for _, shard := range c.state.shards {
		if shard.ID == shardID {
			c.state.shards = append(c.state.shards, lsm.ShardStatus{
				ID:       shardID + "-b",
				StartKey: append([]byte(nil), splitKey...),
				EndKey:   append([]byte(nil), shard.EndKey...),
				Leader:   shard.Leader,
				Replicas: append([]lsm.ReplicaStatus(nil), shard.Replicas...),
			})
			c.state.status.ShardCount = len(c.state.shards)
			c.state.status.Revision++
			c.recordOperationLocked(opts, "split:"+shardID+":"+string(splitKey))
			return nil
		}
	}
	return errs.ErrShardNotFound
}

func (c *controlStubProvider) TriggerRebalance(shardID, target string) error {
	return c.TriggerRebalanceWithOptions(shardID, target, lsm.ControlWriteOptions{})
}

func (c *controlStubProvider) TriggerRebalanceWithOptions(shardID, target string, opts lsm.ControlWriteOptions) error {
	return c.TransferLeaderWithOptions(shardID, target, opts)
}

func (c *controlStubProvider) PrepareDrain(nodeID string) error {
	return c.PrepareDrainWithOptions(nodeID, lsm.ControlWriteOptions{})
}

func (c *controlStubProvider) PrepareDrainWithOptions(nodeID string, opts lsm.ControlWriteOptions) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	applied, err := c.ensureControlWriteOptionsLocked(opts, "drain:"+nodeID)
	if err != nil {
		return err
	}
	if applied {
		return nil
	}
	c.state.status.Draining = true
	c.state.status.Revision++
	c.recordOperationLocked(opts, "drain:"+nodeID)
	return nil
}

func (c *controlStubProvider) ensureControlWriteOptionsLocked(opts lsm.ControlWriteOptions, fingerprint string) (bool, error) {
	if opts.OperationID != "" {
		if existing, ok := c.state.ops[opts.OperationID]; ok {
			if existing == fingerprint {
				return true, nil
			}
			return false, errs.ErrControlOperationConflict
		}
	}
	if opts.ExpectedRevision != nil && *opts.ExpectedRevision != c.state.status.Revision {
		return false, errs.ErrControlRevisionConflict
	}
	return false, nil
}

func (c *controlStubProvider) recordOperationLocked(opts lsm.ControlWriteOptions, fingerprint string) {
	if opts.OperationID != "" {
		c.state.ops[opts.OperationID] = fingerprint
	}
}

func TestHandlerHealth(t *testing.T) {
	handler := NewHandler(stubProvider{})
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	var health lsm.Health
	if err := json.NewDecoder(rec.Body).Decode(&health); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !health.Ready || health.Reason != "ok" {
		t.Fatalf("unexpected health: %+v", health)
	}
}

func TestHandlerStats(t *testing.T) {
	handler := NewHandler(stubProvider{})
	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	var stats lsm.Stats
	if err := json.NewDecoder(rec.Body).Decode(&stats); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if stats.MemtableBytes != 1 || stats.MemtableEntries != 2 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestHandlerClusterStatus(t *testing.T) {
	handler := NewHandler(newControlStubProvider())
	req := httptest.NewRequest(http.MethodGet, "/cluster/status", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	var status lsm.ClusterStatus
	if err := json.NewDecoder(rec.Body).Decode(&status); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if status.NodeID != "node-a" {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestHandlerShards(t *testing.T) {
	handler := NewHandler(newControlStubProvider())
	req := httptest.NewRequest(http.MethodGet, "/cluster/shards", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	var shards []lsm.ShardStatus
	if err := json.NewDecoder(rec.Body).Decode(&shards); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(shards) != 1 || shards[0].ID != "users" {
		t.Fatalf("unexpected shards: %+v", shards)
	}
}

func TestHandlerClusterRoutes(t *testing.T) {
	p := newControlStubProvider()
	handler := NewHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/cluster/routes", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	var routes routingResponse
	if err := json.NewDecoder(rec.Body).Decode(&routes); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if routes.Revision != 0 {
		t.Fatalf("expected revision 0, got %d", routes.Revision)
	}
	if len(routes.Shards) != 1 {
		t.Fatalf("expected one route shard, got %d", len(routes.Shards))
	}
	if routes.Shards[0].ID != "users" {
		t.Fatalf("expected users shard, got %s", routes.Shards[0].ID)
	}
	if routes.Shards[0].Leader != "node-a" {
		t.Fatalf("expected leader node-a, got %s", routes.Shards[0].Leader)
	}
}

func TestHandlerCDCEvents(t *testing.T) {
	provider := &cdcStubProvider{
		result: lsm.CDCReadResult{
			ShardID:       "users",
			FromOffset:    3,
			NextOffset:    5,
			OldestOffset:  2,
			DroppedBefore: true,
			Events: []lsm.CDCEvent{
				{
					Offset:      4,
					ShardID:     "users",
					Operation:   "put",
					Key:         []byte("a"),
					Value:       []byte("1"),
					CommittedAt: time.Unix(1700000000, 0).UTC(),
				},
				{
					Offset:      5,
					ShardID:     "users",
					Operation:   "delete",
					Key:         []byte("a"),
					Tombstone:   true,
					CommittedAt: time.Unix(1700000001, 0).UTC(),
				},
			},
		},
	}
	handler := NewHandler(provider)
	req := httptest.NewRequest(http.MethodGet, "/cdc/events?shard=users&offset=3&limit=2", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	if provider.lastReq.shardID != "users" || provider.lastReq.offset != 3 || provider.lastReq.limit != 2 {
		t.Fatalf("unexpected cdc request: %+v", provider.lastReq)
	}
	var out cdcReadResponse
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.ShardID != "users" || out.NextOffset != 5 || !out.DroppedBefore {
		t.Fatalf("unexpected cdc response metadata: %+v", out)
	}
	if len(out.Events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(out.Events))
	}
	if out.Events[0].KeyBase64 != base64.StdEncoding.EncodeToString([]byte("a")) {
		t.Fatalf("unexpected key base64: %q", out.Events[0].KeyBase64)
	}
	if out.Events[1].Tombstone != true {
		t.Fatalf("expected second event tombstone=true")
	}
}

func TestHandlerRaftPeerMessages(t *testing.T) {
	provider := &raftStubProvider{}
	handler := NewHandler(provider)
	body := `{"messages":[{"from":1,"to":2,"term":3,"type":"MsgApp","payload":"AQID"}]}`
	req := httptest.NewRequest(http.MethodPost, RaftPeerMessagesPath, strings.NewReader(body))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d body=%s", rec.Code, rec.Body.String())
	}
	messages := provider.messagesCopy()
	if len(messages) != 1 {
		t.Fatalf("expected one raft peer message, got %d", len(messages))
	}
	if messages[0].From != 1 || messages[0].To != 2 || messages[0].Type != "MsgApp" {
		t.Fatalf("unexpected raft peer message: %+v", messages[0])
	}
	if string(messages[0].Payload) != string([]byte{1, 2, 3}) {
		t.Fatalf("unexpected raft peer payload: %v", messages[0].Payload)
	}
}

func TestHandlerRaftPeerMessagesUnavailable(t *testing.T) {
	handler := NewHandler(stubProvider{})
	req := httptest.NewRequest(http.MethodPost, RaftPeerMessagesPath, strings.NewReader(`{"messages":[]}`))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestHandlerTransferLeader(t *testing.T) {
	p := newControlStubProvider()
	handler := NewHandler(p)
	body := bytes.NewBufferString(`{"target":"node-b"}`)
	req := httptest.NewRequest(http.MethodPost, "/cluster/shards/users/transfer-leader", body)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	if got := p.Shards()[0].Leader; got != "node-b" {
		t.Fatalf("expected leader node-b, got %q", got)
	}
}

func TestHandlerTransferLeaderWithExpectedRevision(t *testing.T) {
	p := newControlStubProvider()
	handler := NewHandler(p)
	body := bytes.NewBufferString(`{"target":"node-b","operation_id":"op-1","expected_revision":0}`)
	req := httptest.NewRequest(http.MethodPost, "/cluster/shards/users/transfer-leader", body)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	status := p.ClusterStatus()
	if status.Revision != 1 {
		t.Fatalf("expected revision=1, got %d", status.Revision)
	}
}

func TestHandlerTransferLeaderRevisionConflict(t *testing.T) {
	p := newControlStubProvider()
	handler := NewHandler(p)
	first := bytes.NewBufferString(`{"target":"node-b","operation_id":"op-1","expected_revision":0}`)
	firstReq := httptest.NewRequest(http.MethodPost, "/cluster/shards/users/transfer-leader", first)
	firstRec := httptest.NewRecorder()
	handler.ServeHTTP(firstRec, firstReq)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("expected first status 200, got %d", firstRec.Code)
	}

	conflict := bytes.NewBufferString(`{"target":"node-a","operation_id":"op-2","expected_revision":0}`)
	req := httptest.NewRequest(http.MethodPost, "/cluster/shards/users/transfer-leader", conflict)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected status 409, got %d", rec.Code)
	}
}

func TestHandlerTransferLeaderIdempotentRetry(t *testing.T) {
	p := newControlStubProvider()
	handler := NewHandler(p)
	body := `{"target":"node-b","operation_id":"op-1","expected_revision":0}`
	req1 := httptest.NewRequest(http.MethodPost, "/cluster/shards/users/transfer-leader", bytes.NewBufferString(body))
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("expected first status 200, got %d", rec1.Code)
	}

	req2 := httptest.NewRequest(http.MethodPost, "/cluster/shards/users/transfer-leader", bytes.NewBufferString(body))
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected retry status 200, got %d", rec2.Code)
	}
	status := p.ClusterStatus()
	if status.Revision != 1 {
		t.Fatalf("expected revision to stay 1 after idempotent retry, got %d", status.Revision)
	}
}

func TestHandlerTransferLeaderRejectsUnknownFields(t *testing.T) {
	p := newControlStubProvider()
	handler := NewHandler(p)
	body := bytes.NewBufferString(`{"target":"node-b","unexpected":true}`)
	req := httptest.NewRequest(http.MethodPost, "/cluster/shards/users/transfer-leader", body)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
}

func TestHandlerTransferLeaderRejectsTrailingJSON(t *testing.T) {
	p := newControlStubProvider()
	handler := NewHandler(p)
	body := bytes.NewBufferString(`{"target":"node-b"}{"target":"node-a"}`)
	req := httptest.NewRequest(http.MethodPost, "/cluster/shards/users/transfer-leader", body)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
}

func TestHandlerTransferLeaderRejectsUnsupportedControlWriteOptions(t *testing.T) {
	p := newLegacyControlStubProvider()
	handler := NewHandler(p)
	body := bytes.NewBufferString(`{"target":"node-b","operation_id":"op-1","expected_revision":0}`)
	req := httptest.NewRequest(http.MethodPost, "/cluster/shards/users/transfer-leader", body)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), errs.ErrControlWriteOptionsUnsupported.Error()) {
		t.Fatalf("expected unsupported control write options error, got %q", rec.Body.String())
	}
}

func TestHandlerSplit(t *testing.T) {
	p := newControlStubProvider()
	handler := NewHandler(p)
	body := bytes.NewBufferString(`{"split_key_base64":"` + base64.StdEncoding.EncodeToString([]byte("m")) + `"}`)
	req := httptest.NewRequest(http.MethodPost, "/cluster/shards/users/split", body)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	if len(p.Shards()) != 2 {
		t.Fatalf("expected 2 shards after split")
	}
}

func TestHandlerRebalance(t *testing.T) {
	p := newControlStubProvider()
	handler := NewHandler(p)
	body := bytes.NewBufferString(`{"target":"node-b"}`)
	req := httptest.NewRequest(http.MethodPost, "/cluster/shards/users/rebalance", body)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	if got := p.Shards()[0].Leader; got != "node-b" {
		t.Fatalf("expected leader node-b, got %q", got)
	}
}

func TestHandlerDrain(t *testing.T) {
	p := newControlStubProvider()
	handler := NewHandler(p)
	req := httptest.NewRequest(http.MethodPost, "/cluster/nodes/node-a/drain", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	if !p.ClusterStatus().Draining {
		t.Fatalf("expected draining=true")
	}
}

func TestHandlerControlUnavailable(t *testing.T) {
	handler := NewHandler(stubProvider{})
	req := httptest.NewRequest(http.MethodGet, "/cluster/status", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", rec.Code)
	}
}

func TestHandlerPutLocalCommitted(t *testing.T) {
	p := newWriteStubProvider()
	handler := NewHandler(p)
	body := bytes.NewBufferString(`{"key_base64":"` + base64.StdEncoding.EncodeToString([]byte("a")) + `","value_base64":"` + base64.StdEncoding.EncodeToString([]byte("1")) + `","consistency":"local_committed"}`)
	req := httptest.NewRequest(http.MethodPost, "/kv/put", body)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d (%s)", rec.Code, rec.Body.String())
	}
	var status lsm.WriteRequestStatus
	if err := json.NewDecoder(rec.Body).Decode(&status); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if status.State != lsm.WriteRequestCommitted {
		t.Fatalf("expected committed state, got %s", status.State)
	}
	if status.Consistency != lsm.WriteConsistencyLocalCommitted {
		t.Fatalf("expected local_committed consistency, got %s", status.Consistency)
	}
	if got, ok := p.Value("a"); !ok || string(got) != "1" {
		t.Fatalf("expected stored value 1, got %q found=%v", string(got), ok)
	}
}

func TestHandlerPutAcceptedStatusLifecycle(t *testing.T) {
	p := newWriteStubProvider()
	p.putGate = make(chan struct{})
	handler := NewHandler(p)
	body := bytes.NewBufferString(`{"key_base64":"` + base64.StdEncoding.EncodeToString([]byte("a")) + `","value_base64":"` + base64.StdEncoding.EncodeToString([]byte("1")) + `"}`)
	req := httptest.NewRequest(http.MethodPost, "/kv/put", body)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d (%s)", rec.Code, rec.Body.String())
	}
	var accepted lsm.WriteRequestStatus
	if err := json.NewDecoder(rec.Body).Decode(&accepted); err != nil {
		t.Fatalf("decode accepted: %v", err)
	}
	if accepted.State != lsm.WriteRequestPending {
		t.Fatalf("expected pending state, got %s", accepted.State)
	}

	statusReq := httptest.NewRequest(http.MethodGet, "/kv/write-status/"+accepted.RequestID, nil)
	statusRec := httptest.NewRecorder()
	handler.ServeHTTP(statusRec, statusReq)
	if statusRec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", statusRec.Code)
	}
	var pending lsm.WriteRequestStatus
	if err := json.NewDecoder(statusRec.Body).Decode(&pending); err != nil {
		t.Fatalf("decode pending: %v", err)
	}
	if pending.State != lsm.WriteRequestPending {
		t.Fatalf("expected pending state before unblocking writer, got %s", pending.State)
	}

	close(p.putGate)

	var final lsm.WriteRequestStatus
	deadline := time.Now().Add(2 * time.Second)
	for {
		statusReq := httptest.NewRequest(http.MethodGet, "/kv/write-status/"+accepted.RequestID, nil)
		statusRec := httptest.NewRecorder()
		handler.ServeHTTP(statusRec, statusReq)
		if statusRec.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", statusRec.Code)
		}
		if err := json.NewDecoder(statusRec.Body).Decode(&final); err != nil {
			t.Fatalf("decode final: %v", err)
		}
		if final.State == lsm.WriteRequestCommitted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for committed status; last=%+v", final)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got, ok := p.Value("a"); !ok || string(got) != "1" {
		t.Fatalf("expected stored value 1, got %q found=%v", string(got), ok)
	}
}

func TestHandlerDeleteLocalCommittedRejectsNotLeader(t *testing.T) {
	p := newWriteControlStubProvider()
	p.write.deleteErr = errs.ErrNotLeader
	p.control.mu.Lock()
	p.control.state.status.Revision = 7
	p.control.state.shards[0].Leader = "node-b"
	p.control.mu.Unlock()
	handler := NewHandler(p)
	body := bytes.NewBufferString(`{"key_base64":"` + base64.StdEncoding.EncodeToString([]byte("a")) + `","consistency":"local_committed"}`)
	req := httptest.NewRequest(http.MethodPost, "/kv/delete", body)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected status 409, got %d", rec.Code)
	}
	var out writeErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Code != "not_leader" {
		t.Fatalf("expected code not_leader, got %s", out.Code)
	}
	if !out.Retryable {
		t.Fatalf("expected retryable=true")
	}
	if out.Route == nil {
		t.Fatalf("expected route hint")
	}
	if out.Route.Revision != 7 {
		t.Fatalf("expected route revision 7, got %d", out.Route.Revision)
	}
	if out.Route.ShardID != "users" {
		t.Fatalf("expected shard users, got %s", out.Route.ShardID)
	}
	if out.Route.Leader != "node-b" {
		t.Fatalf("expected leader node-b, got %s", out.Route.Leader)
	}
	if !errors.Is(p.write.deleteErr, errs.ErrNotLeader) {
		t.Fatalf("unexpected delete error")
	}
}

func TestHandlerRejectsLinearizableConsistency(t *testing.T) {
	p := newWriteStubProvider()
	handler := NewHandler(p)
	body := bytes.NewBufferString(`{"key_base64":"` + base64.StdEncoding.EncodeToString([]byte("a")) + `","value_base64":"` + base64.StdEncoding.EncodeToString([]byte("1")) + `","consistency":"linearizable"}`)
	req := httptest.NewRequest(http.MethodPost, "/kv/put", body)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d (%s)", rec.Code, rec.Body.String())
	}
	if _, ok := p.Value("a"); ok {
		t.Fatalf("expected invalid consistency to skip write")
	}
}

func TestHandlerPutUsesConfiguredDefaultConsistency(t *testing.T) {
	p := newWriteStubProvider()
	handler := NewHandlerWithOptions(p, HandlerOptions{
		WriteConsistencyDefault: lsm.WriteConsistencyLocalCommitted,
	})
	body := bytes.NewBufferString(`{"key_base64":"` + base64.StdEncoding.EncodeToString([]byte("a")) + `","value_base64":"` + base64.StdEncoding.EncodeToString([]byte("1")) + `"}`)
	req := httptest.NewRequest(http.MethodPost, "/kv/put", body)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d (%s)", rec.Code, rec.Body.String())
	}
	var status lsm.WriteRequestStatus
	if err := json.NewDecoder(rec.Body).Decode(&status); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if status.Consistency != lsm.WriteConsistencyLocalCommitted {
		t.Fatalf("expected default consistency local_committed, got %s", status.Consistency)
	}
}

func TestWriteRequestStoreCompactionKeepsPending(t *testing.T) {
	store := newWriteRequestStore(1)
	s1 := store.New("put", lsm.WriteConsistencyAccepted)
	s2 := store.New("put", lsm.WriteConsistencyAccepted)
	if _, ok := store.Get(s1.RequestID); !ok {
		t.Fatalf("expected pending request %s to be retained", s1.RequestID)
	}
	if _, ok := store.Get(s2.RequestID); !ok {
		t.Fatalf("expected pending request %s to be retained", s2.RequestID)
	}

	store.Commit(s1.RequestID)
	s3 := store.New("put", lsm.WriteConsistencyAccepted)
	if _, ok := store.Get(s1.RequestID); ok {
		t.Fatalf("expected committed request %s to be evicted", s1.RequestID)
	}
	if _, ok := store.Get(s2.RequestID); !ok {
		t.Fatalf("expected pending request %s to be retained", s2.RequestID)
	}
	if _, ok := store.Get(s3.RequestID); !ok {
		t.Fatalf("expected pending request %s to be retained", s3.RequestID)
	}
}
