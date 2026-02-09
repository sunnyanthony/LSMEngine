package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

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
