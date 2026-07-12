package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"

	"lsmengine/pkg/lsm"
)

type testNodeEndpointResolver struct {
	endpoints map[string]string
}

func (r testNodeEndpointResolver) ResolveNodeEndpoints(_ context.Context) (map[string]string, error) {
	out := make(map[string]string, len(r.endpoints))
	for nodeID, endpoint := range r.endpoints {
		out[nodeID] = endpoint
	}
	return out, nil
}

func TestGatewayPutRetriesOnStaleRoute(t *testing.T) {
	var leader atomic.Value
	leader.Store("node-a")
	var routeReads atomic.Int32
	var writesA atomic.Int32
	var writesB atomic.Int32

	handlerA := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/routes":
			routeReads.Add(1)
			currentLeader := leader.Load().(string)
			revision := uint64(1)
			if currentLeader == "node-b" {
				revision = 2
			}
			writeJSON(w, http.StatusOK, routingResponse{
				Revision: revision,
				Shards: []routingShard{
					{
						ID:             "users",
						StartKeyBase64: "YQ==", // a
						EndKeyBase64:   "eg==", // z
						Leader:         currentLeader,
					},
				},
			})
		case "/kv/put":
			writesA.Add(1)
			if leader.Load().(string) == "node-a" {
				writeJSON(w, http.StatusOK, lsm.WriteRequestStatus{
					RequestID:   "a-1",
					Operation:   "put",
					Consistency: lsm.WriteConsistencyLocalCommitted,
					State:       lsm.WriteRequestCommitted,
				})
				return
			}
			writeJSON(w, http.StatusConflict, writeErrorResponse{
				Error:     "lsm: not leader",
				Code:      "not_leader",
				Retryable: true,
				Route: &writeRouteHint{
					Revision: 2,
					ShardID:  "users",
					Leader:   "node-b",
				},
			})
		default:
			http.NotFound(w, r)
		}
	})

	handlerB := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/kv/put":
			writesB.Add(1)
			if leader.Load().(string) != "node-b" {
				writeJSON(w, http.StatusConflict, writeErrorResponse{
					Error:     "lsm: not leader",
					Code:      "not_leader",
					Retryable: true,
				})
				return
			}
			writeJSON(w, http.StatusOK, lsm.WriteRequestStatus{
				RequestID:   "b-1",
				Operation:   "put",
				Consistency: lsm.WriteConsistencyLocalCommitted,
				State:       lsm.WriteRequestCommitted,
			})
		default:
			http.NotFound(w, r)
		}
	})

	client := newInMemoryHTTPClient(map[string]http.Handler{
		"node-a": handlerA,
		"node-b": handlerB,
	})

	gateway, err := NewGateway(GatewayOptions{
		BootstrapURL: "http://node-a",
		NodeEndpoints: map[string]string{
			"node-a": "http://node-a",
			"node-b": "http://node-b",
		},
		HTTPClient: client,
	})
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}

	if err := gateway.refreshRoutes(context.Background()); err != nil {
		t.Fatalf("initial refresh: %v", err)
	}
	leader.Store("node-b")

	status, err := gateway.Put(context.Background(), []byte("c"), []byte("1"), lsm.WriteConsistencyLocalCommitted)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if status.State != lsm.WriteRequestCommitted {
		t.Fatalf("expected committed status, got %+v", status)
	}
	if writesA.Load() != 1 {
		t.Fatalf("expected one stale write to node-a, got %d", writesA.Load())
	}
	if writesB.Load() != 1 {
		t.Fatalf("expected one retried write to node-b, got %d", writesB.Load())
	}
	if routeReads.Load() != 1 {
		t.Fatalf("expected retry to use route hint without refreshing routes, got %d route reads", routeReads.Load())
	}
}

func TestGatewayPutReturnsWriteRequestError(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/routes":
			writeJSON(w, http.StatusOK, routingResponse{
				Revision: 1,
				Shards: []routingShard{
					{
						ID:             "users",
						StartKeyBase64: "YQ==",
						EndKeyBase64:   "eg==",
						Leader:         "node-a",
					},
				},
			})
		case "/kv/put":
			writeJSON(w, http.StatusServiceUnavailable, writeErrorResponse{
				Error:     "lsm: closed",
				Code:      "closed",
				Retryable: false,
			})
		default:
			http.NotFound(w, r)
		}
	})

	gateway, err := NewGateway(GatewayOptions{
		BootstrapURL: "http://node-a",
		NodeEndpoints: map[string]string{
			"node-a": "http://node-a",
		},
		HTTPClient: newInMemoryHTTPClient(map[string]http.Handler{"node-a": handler}),
	})
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}

	_, err = gateway.Put(context.Background(), []byte("c"), []byte("1"), lsm.WriteConsistencyLocalCommitted)
	if err == nil {
		t.Fatalf("expected error")
	}
	reqErr, ok := err.(*WriteRequestError)
	if !ok {
		t.Fatalf("expected WriteRequestError, got %T", err)
	}
	if reqErr.Status != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", reqErr.Status)
	}
	if reqErr.Response.Code != "closed" {
		t.Fatalf("expected closed code, got %s", reqErr.Response.Code)
	}
}

func TestGatewayDeleteRoutesToLeader(t *testing.T) {
	var nodeBDeleteCalls atomic.Int32
	handlerA := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cluster/routes" {
			writeJSON(w, http.StatusOK, routingResponse{
				Revision: 1,
				Shards: []routingShard{
					{
						ID:             "users",
						StartKeyBase64: "YQ==",
						EndKeyBase64:   "eg==",
						Leader:         "node-b",
					},
				},
			})
			return
		}
		http.NotFound(w, r)
	})

	handlerB := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/kv/delete" {
			http.NotFound(w, r)
			return
		}
		nodeBDeleteCalls.Add(1)
		var in deleteRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Fatalf("decode: %v", err)
		}
		writeJSON(w, http.StatusOK, lsm.WriteRequestStatus{
			RequestID:   "d-1",
			Operation:   "delete",
			Consistency: in.Consistency,
			State:       lsm.WriteRequestCommitted,
		})
	})

	gateway, err := NewGateway(GatewayOptions{
		BootstrapURL: "http://node-a",
		NodeEndpoints: map[string]string{
			"node-a": "http://node-a",
			"node-b": "http://node-b",
		},
		HTTPClient: newInMemoryHTTPClient(map[string]http.Handler{
			"node-a": handlerA,
			"node-b": handlerB,
		}),
	})
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}
	status, err := gateway.Delete(context.Background(), []byte("c"), lsm.WriteConsistencyLocalCommitted)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if status.State != lsm.WriteRequestCommitted {
		t.Fatalf("expected committed delete, got %+v", status)
	}
	if nodeBDeleteCalls.Load() != 1 {
		t.Fatalf("expected delete to be routed to node-b")
	}
}

func TestGatewayUsesNodeEndpointResolver(t *testing.T) {
	var nodeBWrites atomic.Int32
	handlerA := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cluster/routes" {
			writeJSON(w, http.StatusOK, routingResponse{
				Revision: 1,
				Shards: []routingShard{
					{
						ID:             "users",
						StartKeyBase64: "YQ==",
						EndKeyBase64:   "eg==",
						Leader:         "node-b",
					},
				},
			})
			return
		}
		http.NotFound(w, r)
	})
	handlerB := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/kv/put" {
			http.NotFound(w, r)
			return
		}
		nodeBWrites.Add(1)
		writeJSON(w, http.StatusOK, lsm.WriteRequestStatus{
			RequestID:   "resolver-1",
			Operation:   "put",
			Consistency: lsm.WriteConsistencyLocalCommitted,
			State:       lsm.WriteRequestCommitted,
		})
	})

	gateway, err := NewGateway(GatewayOptions{
		BootstrapURL: "http://node-a",
		NodeEndpointResolver: testNodeEndpointResolver{
			endpoints: map[string]string{
				"node-a": "http://node-a",
				"node-b": "http://node-b",
			},
		},
		HTTPClient: newInMemoryHTTPClient(map[string]http.Handler{
			"node-a": handlerA,
			"node-b": handlerB,
		}),
	})
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}
	status, err := gateway.Put(context.Background(), []byte("c"), []byte("1"), lsm.WriteConsistencyLocalCommitted)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if status.State != lsm.WriteRequestCommitted {
		t.Fatalf("expected committed status, got %+v", status)
	}
	if nodeBWrites.Load() != 1 {
		t.Fatalf("expected resolver-routed write to node-b")
	}
}

func TestGatewayUsesReloadedNodeEndpointFileResolver(t *testing.T) {
	path := t.TempDir() + "/endpoints.yaml"
	if err := os.WriteFile(path, []byte(`
node-a: "http://node-a"
node-b: "http://old-b"
`), 0o644); err != nil {
		t.Fatalf("write endpoints: %v", err)
	}
	resolver, err := NewNodeEndpointFileResolver(NodeEndpointFileResolverOptions{Path: path})
	if err != nil {
		t.Fatalf("new file resolver: %v", err)
	}
	var oldBWrites atomic.Int32
	var newBWrites atomic.Int32
	handlerA := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cluster/routes" {
			writeJSON(w, http.StatusOK, routingResponse{
				Revision: 1,
				Shards: []routingShard{
					{
						ID:             "users",
						StartKeyBase64: "YQ==",
						EndKeyBase64:   "eg==",
						Leader:         "node-b",
					},
				},
			})
			return
		}
		http.NotFound(w, r)
	})
	handlerOldB := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		oldBWrites.Add(1)
		http.NotFound(w, r)
	})
	handlerNewB := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/kv/put" {
			http.NotFound(w, r)
			return
		}
		newBWrites.Add(1)
		writeJSON(w, http.StatusOK, lsm.WriteRequestStatus{
			RequestID:   "reloaded-1",
			Operation:   "put",
			Consistency: lsm.WriteConsistencyLocalCommitted,
			State:       lsm.WriteRequestCommitted,
		})
	})
	gateway, err := NewGateway(GatewayOptions{
		BootstrapURL:         "http://node-a",
		NodeEndpointResolver: resolver,
		HTTPClient: newInMemoryHTTPClient(map[string]http.Handler{
			"node-a": handlerA,
			"old-b":  handlerOldB,
			"new-b":  handlerNewB,
		}),
	})
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}
	if err := os.WriteFile(path, []byte(`
node-a: "http://node-a"
node-b: "http://new-b"
`), 0o644); err != nil {
		t.Fatalf("rewrite endpoints: %v", err)
	}

	status, err := gateway.Put(context.Background(), []byte("c"), []byte("1"), lsm.WriteConsistencyLocalCommitted)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if status.State != lsm.WriteRequestCommitted {
		t.Fatalf("expected committed status, got %+v", status)
	}
	if oldBWrites.Load() != 0 || newBWrites.Load() != 1 {
		t.Fatalf("expected write through reloaded endpoint, old=%d new=%d", oldBWrites.Load(), newBWrites.Load())
	}
}

func TestGatewayAlignsShardLeaderToCommitLogWriteLeader(t *testing.T) {
	var transfers atomic.Int32
	var writes atomic.Int32
	shardLeader := atomic.Value{}
	shardLeader.Store("node-a")

	handlerA := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			writeJSON(w, http.StatusOK, lsm.ClusterStatus{
				NodeID: "node-a",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         false,
					WriteAvailable: false,
					Health:         "follower",
					LeaderKnown:    true,
				},
			})
		default:
			http.NotFound(w, r)
		}
	})
	handlerB := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			writeJSON(w, http.StatusOK, lsm.ClusterStatus{
				NodeID: "node-b",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         true,
					WriteAvailable: true,
					Health:         "ready",
					LeaderKnown:    true,
				},
			})
		case "/cluster/shards":
			writeJSON(w, http.StatusOK, []lsm.ShardStatus{
				{
					ID:       "users",
					StartKey: []byte("a"),
					EndKey:   []byte("z"),
					Leader:   shardLeader.Load().(string),
				},
			})
		case "/cluster/shards/users/transfer-leader":
			transfers.Add(1)
			var req targetRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode transfer: %v", err)
			}
			if req.Target != "node-b" {
				t.Fatalf("expected transfer target node-b, got %q", req.Target)
			}
			shardLeader.Store("node-b")
			writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		case "/kv/put":
			writes.Add(1)
			if shardLeader.Load().(string) != "node-b" {
				t.Fatalf("write happened before shard leader aligned")
			}
			writeJSON(w, http.StatusOK, lsm.WriteRequestStatus{
				RequestID:   "aligned-1",
				Operation:   "put",
				Consistency: lsm.WriteConsistencyLocalCommitted,
				State:       lsm.WriteRequestCommitted,
			})
		default:
			http.NotFound(w, r)
		}
	})
	gateway, err := NewGateway(GatewayOptions{
		BootstrapURL: "http://node-a",
		NodeEndpoints: map[string]string{
			"node-a": "http://node-a",
			"node-b": "http://node-b",
		},
		HTTPClient: newInMemoryHTTPClient(map[string]http.Handler{
			"node-a": handlerA,
			"node-b": handlerB,
		}),
		AlignWriteLeader: true,
	})
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}

	status, err := gateway.Put(context.Background(), []byte("c"), []byte("1"), lsm.WriteConsistencyLocalCommitted)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if status.State != lsm.WriteRequestCommitted {
		t.Fatalf("expected committed status, got %+v", status)
	}
	if transfers.Load() != 1 {
		t.Fatalf("expected one shard leader transfer, got %d", transfers.Load())
	}
	if writes.Load() != 1 {
		t.Fatalf("expected one write, got %d", writes.Load())
	}
}

func TestGatewayStopsAfterMaxWriteAttempts(t *testing.T) {
	var writes atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/routes":
			writeJSON(w, http.StatusOK, routingResponse{
				Revision: 1,
				Shards: []routingShard{
					{
						ID:             "users",
						StartKeyBase64: "YQ==",
						EndKeyBase64:   "eg==",
						Leader:         "node-a",
					},
				},
			})
		case "/kv/put":
			writes.Add(1)
			writeJSON(w, http.StatusServiceUnavailable, writeErrorResponse{
				Error:     "commit log unavailable",
				Code:      "commit_log_unavailable",
				Retryable: true,
			})
		default:
			http.NotFound(w, r)
		}
	})

	gateway, err := NewGateway(GatewayOptions{
		BootstrapURL: "http://node-a",
		NodeEndpoints: map[string]string{
			"node-a": "http://node-a",
		},
		HTTPClient:       newInMemoryHTTPClient(map[string]http.Handler{"node-a": handler}),
		MaxWriteAttempts: 3,
	})
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}

	_, err = gateway.Put(context.Background(), []byte("c"), []byte("1"), lsm.WriteConsistencyLocalCommitted)
	if err == nil {
		t.Fatalf("expected error")
	}
	reqErr, ok := err.(*WriteRequestError)
	if !ok {
		t.Fatalf("expected WriteRequestError, got %T", err)
	}
	if reqErr.Response.Code != "commit_log_unavailable" {
		t.Fatalf("expected commit_log_unavailable, got %s", reqErr.Response.Code)
	}
	if writes.Load() != 3 {
		t.Fatalf("expected 3 bounded attempts, got %d", writes.Load())
	}
}

func newInMemoryHTTPClient(hostHandlers map[string]http.Handler) *http.Client {
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		handler, ok := hostHandlers[req.URL.Host]
		if !ok {
			return nil, fmt.Errorf("no handler for host %q", req.URL.Host)
		}
		clone := req.Clone(req.Context())
		if req.Body != nil {
			body, err := io.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}
			_ = req.Body.Close()
			req.Body = io.NopCloser(bytes.NewReader(body))
			clone.Body = io.NopCloser(bytes.NewReader(body))
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, clone)
		return rec.Result(), nil
	})
	return &http.Client{Transport: transport}
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
