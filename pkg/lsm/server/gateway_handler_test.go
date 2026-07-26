package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"lsmengine/pkg/lsm"
)

func TestGatewayHandlerRoutesPutToLeader(t *testing.T) {
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
		var in putRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Fatalf("decode put: %v", err)
		}
		if in.Consistency != lsm.WriteConsistencyLocalCommitted {
			t.Fatalf("expected local committed consistency, got %q", in.Consistency)
		}
		writeJSON(w, http.StatusOK, lsm.WriteRequestStatus{
			RequestID:   "gateway-put-1",
			Operation:   "put",
			Consistency: in.Consistency,
			State:       lsm.WriteRequestCommitted,
		})
	})
	gateway, err := NewGateway(GatewayOptions{
		BootstrapURL: "http://node-a",
		NodeEndpoints: map[string]string{
			"bootstrap": "http://node-a",
			"node-a":    "http://node-a",
			"node-b":    "http://node-b",
		},
		HTTPClient: newInMemoryHTTPClient(map[string]http.Handler{
			"node-a": handlerA,
			"node-b": handlerB,
		}),
	})
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}

	body, err := json.Marshal(putRequest{
		KeyBase64:   base64.StdEncoding.EncodeToString([]byte("c")),
		ValueBase64: base64.StdEncoding.EncodeToString([]byte("1")),
		Consistency: lsm.WriteConsistencyLocalCommitted,
	})
	if err != nil {
		t.Fatalf("marshal put: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/kv/put", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	NewGatewayHandler(gateway, HandlerOptions{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var out lsm.WriteRequestStatus
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.State != lsm.WriteRequestCommitted {
		t.Fatalf("expected committed status, got %+v", out)
	}
	if nodeBWrites.Load() != 1 {
		t.Fatalf("expected one routed write to node-b")
	}
}

func TestGatewayHandlerGetFallsBackAcrossEndpoints(t *testing.T) {
	handlerA := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	})
	handlerB := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/kv/get" {
			http.NotFound(w, r)
			return
		}
		if got := r.URL.Query().Get("key_base64"); got != base64.StdEncoding.EncodeToString([]byte("c")) {
			t.Fatalf("unexpected key query %q", got)
		}
		writeJSON(w, http.StatusOK, getResponse{
			Found:       true,
			KeyBase64:   base64.StdEncoding.EncodeToString([]byte("c")),
			ValueBase64: base64.StdEncoding.EncodeToString([]byte("1")),
			Seq:         7,
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
	req := httptest.NewRequest(
		http.MethodGet,
		"/kv/get?key_base64="+base64.StdEncoding.EncodeToString([]byte("c")),
		nil,
	)
	rec := httptest.NewRecorder()
	NewGatewayHandler(gateway, HandlerOptions{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var out getResponse
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !out.Found || out.Seq != 7 {
		t.Fatalf("unexpected get response: %+v", out)
	}
}

func TestGatewayHandlerGetRotatesHealthyEndpoints(t *testing.T) {
	var nodeAReads atomic.Int32
	var nodeBReads atomic.Int32
	handlerA := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/kv/get" {
			http.NotFound(w, r)
			return
		}
		nodeAReads.Add(1)
		writeJSON(w, http.StatusOK, getResponse{Found: true, Seq: 1})
	})
	handlerB := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/kv/get" {
			http.NotFound(w, r)
			return
		}
		nodeBReads.Add(1)
		writeJSON(w, http.StatusOK, getResponse{Found: true, Seq: 2})
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
	handler := NewGatewayHandler(gateway, HandlerOptions{})
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/kv/get?key_base64=Yw==", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d expected 200, got %d body=%s", i, rec.Code, rec.Body.String())
		}
	}
	if nodeAReads.Load() != 1 || nodeBReads.Load() != 1 {
		t.Fatalf("expected reads to rotate across healthy endpoints, node-a=%d node-b=%d", nodeAReads.Load(), nodeBReads.Load())
	}
}

func TestGatewayHandlerGetLeaderReadModeUsesWriteLeader(t *testing.T) {
	var nodeAReads atomic.Int32
	var nodeBReads atomic.Int32
	handlerA := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			writeJSON(w, http.StatusOK, lsm.ClusterStatus{
				NodeID: "node-a",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Health:      "follower",
					LeaderKnown: true,
				},
			})
		case "/kv/get":
			nodeAReads.Add(1)
			http.Error(w, "follower read should not be used", http.StatusInternalServerError)
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
					Health:         "ready",
					Leader:         true,
					LeaderKnown:    true,
					WriteAvailable: true,
				},
			})
		case "/kv/get":
			nodeBReads.Add(1)
			writeJSON(w, http.StatusOK, getResponse{Found: true, Seq: 8})
		default:
			http.NotFound(w, r)
		}
	})
	gateway, err := NewGateway(GatewayOptions{
		BootstrapURL: "http://node-a",
		ReadMode:     GatewayReadModeLeader,
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
	req := httptest.NewRequest(http.MethodGet, "/kv/get?key_base64=Yw==", nil)
	rec := httptest.NewRecorder()
	NewGatewayHandler(gateway, HandlerOptions{}).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if nodeAReads.Load() != 0 || nodeBReads.Load() != 1 {
		t.Fatalf("expected read only through leader, node-a=%d node-b=%d", nodeAReads.Load(), nodeBReads.Load())
	}
}

func TestGatewayHandlerRangeLeaderReadModeUsesWriteLeader(t *testing.T) {
	var nodeAReads atomic.Int32
	var nodeBReads atomic.Int32
	handlerA := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			writeJSON(w, http.StatusOK, lsm.ClusterStatus{
				NodeID: "node-a",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Health:      "follower",
					LeaderKnown: true,
				},
			})
		case "/kv/range":
			nodeAReads.Add(1)
			http.Error(w, "follower range read should not be used", http.StatusInternalServerError)
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
					Health:         "ready",
					Leader:         true,
					LeaderKnown:    true,
					WriteAvailable: true,
				},
			})
		case "/kv/range":
			nodeBReads.Add(1)
			writeJSON(w, http.StatusOK, rangeResponse{
				Entries: []rangeEntryResponse{{KeyBase64: "Yw==", ValueBase64: "MQ==", Seq: 8}},
			})
		default:
			http.NotFound(w, r)
		}
	})
	gateway, err := NewGateway(GatewayOptions{
		BootstrapURL: "http://node-a",
		ReadMode:     GatewayReadModeLeader,
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
	req := httptest.NewRequest(http.MethodGet, "/kv/range?start_key_base64=Yw==", nil)
	rec := httptest.NewRecorder()
	NewGatewayHandler(gateway, HandlerOptions{}).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if nodeAReads.Load() != 0 || nodeBReads.Load() != 1 {
		t.Fatalf("expected range read only through leader, node-a=%d node-b=%d", nodeAReads.Load(), nodeBReads.Load())
	}
}

func TestGatewayRejectsInvalidReadMode(t *testing.T) {
	if _, err := NewGateway(GatewayOptions{
		BootstrapURL: "http://node-a",
		ReadMode:     "strict",
		NodeEndpoints: map[string]string{
			"node-a": "http://node-a",
		},
	}); err == nil {
		t.Fatalf("expected invalid read mode error")
	}
}

func TestGatewayHandlerGetDefersRecentlyFailedEndpoint(t *testing.T) {
	var nodeAReads atomic.Int32
	var nodeBReads atomic.Int32
	handlerA := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nodeAReads.Add(1)
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	})
	handlerB := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/kv/get" {
			http.NotFound(w, r)
			return
		}
		nodeBReads.Add(1)
		writeJSON(w, http.StatusOK, getResponse{Found: true, Seq: 7})
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
		EndpointFailureCooldown: time.Hour,
	})
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}
	handler := NewGatewayHandler(gateway, HandlerOptions{})
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/kv/get?key_base64=Yw==", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d expected 200, got %d body=%s", i, rec.Code, rec.Body.String())
		}
	}
	if nodeAReads.Load() != 1 {
		t.Fatalf("expected recently failed node-a to be deferred on second read, got %d reads", nodeAReads.Load())
	}
	if nodeBReads.Load() != 2 {
		t.Fatalf("expected node-b to serve both reads after fallback, got %d reads", nodeBReads.Load())
	}
}

func TestGatewayHandlerWriteStatusFallsBackOnNotFound(t *testing.T) {
	handlerA := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/kv/write-status/req-1" {
			http.NotFound(w, r)
			return
		}
		http.NotFound(w, r)
	})
	handlerB := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/kv/write-status/req-1" {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, http.StatusOK, lsm.WriteRequestStatus{
			RequestID:   "req-1",
			Operation:   "put",
			Consistency: lsm.WriteConsistencyAccepted,
			State:       lsm.WriteRequestCommitted,
		})
	})
	gateway, err := NewGateway(GatewayOptions{
		BootstrapURL: "http://node-a",
		ReadMode:     GatewayReadModeLeader,
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
	req := httptest.NewRequest(http.MethodGet, "/kv/write-status/req-1", nil)
	rec := httptest.NewRecorder()
	NewGatewayHandler(gateway, HandlerOptions{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var out lsm.WriteRequestStatus
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if out.RequestID != "req-1" || out.State != lsm.WriteRequestCommitted {
		t.Fatalf("unexpected write status: %+v", out)
	}
}

func TestGatewayHandlerHealth(t *testing.T) {
	resolver, err := NewStaticNodeEndpointResolver(map[string]string{"node-a": "http://node-a"})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	gateway, err := NewGateway(GatewayOptions{
		BootstrapURL:         "http://node-a",
		NodeEndpointResolver: resolver,
		HTTPClient:           newInMemoryHTTPClient(map[string]http.Handler{"node-a": http.NotFoundHandler()}),
	})
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	NewGatewayHandler(gateway, HandlerOptions{}).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestGatewayHandlerReadyRequiresWriteLeader(t *testing.T) {
	handlerA := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cluster/status" {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, http.StatusOK, lsm.ClusterStatus{
			NodeID: "node-a",
			CommitLogRuntime: lsm.CommitLogRuntimeStatus{
				Health:         "ready",
				Leader:         true,
				LeaderKnown:    true,
				WriteAvailable: true,
			},
		})
	})
	gateway, err := NewGateway(GatewayOptions{
		BootstrapURL: "http://node-a",
		NodeEndpoints: map[string]string{
			"node-a": "http://node-a",
		},
		HTTPClient: newInMemoryHTTPClient(map[string]http.Handler{
			"node-a": handlerA,
		}),
	})
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	NewGatewayHandler(gateway, HandlerOptions{}).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestGatewayHandlerReadyUnavailableWithoutWriteLeader(t *testing.T) {
	handlerA := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cluster/status" {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, http.StatusOK, lsm.ClusterStatus{
			NodeID: "node-a",
			CommitLogRuntime: lsm.CommitLogRuntimeStatus{
				Health:         "follower",
				LeaderKnown:    true,
				WriteAvailable: false,
			},
		})
	})
	gateway, err := NewGateway(GatewayOptions{
		BootstrapURL: "http://node-a",
		NodeEndpoints: map[string]string{
			"node-a": "http://node-a",
		},
		HTTPClient: newInMemoryHTTPClient(map[string]http.Handler{
			"node-a": handlerA,
		}),
	})
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	NewGatewayHandler(gateway, HandlerOptions{}).ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d body=%s", rec.Code, rec.Body.String())
	}
	var health lsm.Health
	if err := json.NewDecoder(rec.Body).Decode(&health); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	if health.Ready || health.Reason != "write_leader_unavailable" {
		t.Fatalf("unexpected readiness response: %+v", health)
	}
}

func TestGatewayHandlerReadyUnavailableWhenNil(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	NewGatewayHandler(nil, HandlerOptions{}).ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
	var health lsm.Health
	if err := json.NewDecoder(rec.Body).Decode(&health); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	if health.Ready || health.Reason != "gateway_unavailable" {
		t.Fatalf("unexpected readiness response: %+v", health)
	}
}

func TestGatewayHandlerStatusAggregatesBackendNodes(t *testing.T) {
	handlerA := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cluster/status" {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, http.StatusOK, lsm.ClusterStatus{
			NodeID: "node-a",
			CommitLogRuntime: lsm.CommitLogRuntimeStatus{
				Health:         "follower",
				LeaderKnown:    true,
				WriteAvailable: false,
			},
		})
	})
	handlerB := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cluster/status" {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, http.StatusOK, lsm.ClusterStatus{
			NodeID: "node-b",
			CommitLogRuntime: lsm.CommitLogRuntimeStatus{
				Health:         "ready",
				Leader:         true,
				LeaderKnown:    true,
				WriteAvailable: true,
			},
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

	req := httptest.NewRequest(http.MethodGet, "/gateway/status", nil)
	rec := httptest.NewRecorder()
	NewGatewayHandler(gateway, HandlerOptions{}).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var out GatewayClusterStatus
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if !out.Ready || out.NodeCount != 2 || out.ReachableNodes != 2 {
		t.Fatalf("unexpected gateway status: %+v", out)
	}
	if out.ReadMode != string(GatewayReadModeAny) {
		t.Fatalf("unexpected read mode: %+v", out)
	}
	if out.WriteLeader != "node-b" || out.WriteLeaderEndpoint != "http://node-b" {
		t.Fatalf("unexpected write leader: %+v", out)
	}
	if out.Routing.RouteRefreshes != 0 || out.Routing.WriteAttempts != 0 {
		t.Fatalf("expected idle routing stats in gateway status, got %+v", out.Routing)
	}
	if len(out.Nodes) != 2 || !out.Nodes[0].OK || !out.Nodes[1].OK {
		t.Fatalf("expected two successful node statuses: %+v", out.Nodes)
	}
	for _, node := range out.Nodes {
		if node.Degraded || node.DegradedUntil != "" {
			t.Fatalf("successful status probe should clear degraded state: %+v", node)
		}
	}
}

func TestGatewayHandlerStatusUnavailableWhenBackendsFail(t *testing.T) {
	gateway, err := NewGateway(GatewayOptions{
		BootstrapURL: "http://node-a",
		NodeEndpoints: map[string]string{
			"node-a": "http://node-a",
		},
		HTTPClient: newInMemoryHTTPClient(map[string]http.Handler{
			"node-a": http.NotFoundHandler(),
		}),
	})
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/gateway/status", nil)
	rec := httptest.NewRecorder()
	NewGatewayHandler(gateway, HandlerOptions{}).ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d body=%s", rec.Code, rec.Body.String())
	}
	var out GatewayClusterStatus
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if out.Ready || out.ReachableNodes != 0 || len(out.Nodes) != 1 || out.Nodes[0].Error == "" {
		t.Fatalf("unexpected unavailable status: %+v", out)
	}
	if !out.Nodes[0].Degraded || out.Nodes[0].DegradedUntil == "" {
		t.Fatalf("expected failed backend to report degraded cooldown: %+v", out.Nodes[0])
	}
}

func TestGatewayHandlerStatusClearsDegradedStateOnSuccess(t *testing.T) {
	handlerA := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cluster/status" {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, http.StatusOK, lsm.ClusterStatus{
			NodeID: "node-a",
			CommitLogRuntime: lsm.CommitLogRuntimeStatus{
				Health:         "ready",
				Leader:         true,
				LeaderKnown:    true,
				WriteAvailable: true,
			},
		})
	})
	gateway, err := NewGateway(GatewayOptions{
		BootstrapURL: "http://node-a",
		NodeEndpoints: map[string]string{
			"node-a": "http://node-a",
		},
		HTTPClient: newInMemoryHTTPClient(map[string]http.Handler{
			"node-a": handlerA,
		}),
		EndpointFailureCooldown: time.Hour,
	})
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}
	gateway.markEndpointFailure("http://node-a")

	req := httptest.NewRequest(http.MethodGet, "/gateway/status", nil)
	rec := httptest.NewRecorder()
	NewGatewayHandler(gateway, HandlerOptions{}).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var out GatewayClusterStatus
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if len(out.Nodes) != 1 || !out.Nodes[0].OK {
		t.Fatalf("unexpected gateway status: %+v", out)
	}
	if out.Nodes[0].Degraded || out.Nodes[0].DegradedUntil != "" {
		t.Fatalf("expected successful status probe to clear degraded state: %+v", out.Nodes[0])
	}
}

func TestGatewayHandlerUnavailableWhenNil(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	NewGatewayHandler(nil, HandlerOptions{}).ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestGatewayHandlerHonorsContextCancellation(t *testing.T) {
	resolver, err := NewStaticNodeEndpointResolver(map[string]string{"node-a": "http://node-a"})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	gateway, err := NewGateway(GatewayOptions{
		BootstrapURL:         "http://node-a",
		NodeEndpointResolver: resolver,
		HTTPClient:           newInMemoryHTTPClient(map[string]http.Handler{"node-a": http.NotFoundHandler()}),
	})
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/kv/get?key_base64=Yw==", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	NewGatewayHandler(gateway, HandlerOptions{}).ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}
