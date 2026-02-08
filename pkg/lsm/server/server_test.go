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
	mu     sync.Mutex
	status lsm.ClusterStatus
	shards []lsm.ShardStatus
}

func newControlStubProvider() *controlStubProvider {
	return &controlStubProvider{
		status: lsm.ClusterStatus{
			NodeID:      "node-a",
			ClusterID:   "cluster-dev",
			StorageMode: lsm.StorageModeLocal,
			ShardCount:  1,
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
	}
}

func (c *controlStubProvider) ClusterStatus() lsm.ClusterStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status
}

func (c *controlStubProvider) Shards() []lsm.ShardStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]lsm.ShardStatus, len(c.shards))
	copy(out, c.shards)
	return out
}

func (c *controlStubProvider) TransferLeader(shardID, target string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.shards {
		if c.shards[i].ID != shardID {
			continue
		}
		c.shards[i].Leader = target
		return nil
	}
	return errs.ErrShardNotFound
}

func (c *controlStubProvider) TriggerSplit(shardID string, splitKey []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, shard := range c.shards {
		if shard.ID == shardID {
			c.shards = append(c.shards, lsm.ShardStatus{
				ID:       shardID + "-b",
				StartKey: append([]byte(nil), splitKey...),
				EndKey:   append([]byte(nil), shard.EndKey...),
				Leader:   shard.Leader,
				Replicas: append([]lsm.ReplicaStatus(nil), shard.Replicas...),
			})
			c.status.ShardCount = len(c.shards)
			return nil
		}
	}
	return errs.ErrShardNotFound
}

func (c *controlStubProvider) TriggerRebalance(shardID, target string) error {
	return c.TransferLeader(shardID, target)
}

func (c *controlStubProvider) PrepareDrain(nodeID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status.Draining = true
	return nil
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
