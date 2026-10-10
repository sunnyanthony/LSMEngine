package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"lsmengine/pkg/lsm"
)

func TestGatewayDiscoveryFallsBackFromSlowOrUnknownBootstrap(t *testing.T) {
	for _, mode := range []string{"slow", "unknown-leader"} {
		t.Run(mode, func(t *testing.T) {
			bootstrap := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "slow" {
					<-r.Context().Done()
					return
				}
				writeJSON(w, http.StatusOK, routingResponse{Shards: []routingShard{{ID: "shared"}}})
			}))
			defer bootstrap.Close()
			var writes atomic.Int32
			healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/cluster/routes" {
					writeJSON(w, http.StatusOK, routingResponse{Shards: []routingShard{{ID: "shared", Leader: "healthy"}}})
					return
				}
				writes.Add(1)
				writeJSON(w, http.StatusOK, lsm.WriteRequestStatus{State: lsm.WriteRequestCommitted})
			}))
			defer healthy.Close()
			g, err := NewGateway(GatewayOptions{BootstrapURL: bootstrap.URL, NodeEndpoints: map[string]string{"healthy": healthy.URL}, RouteProbeTimeout: 50 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := g.Put(ctx, []byte("k"), []byte("v"), lsm.WriteConsistencyLocalCommitted); err != nil {
				t.Fatal(err)
			}
			if writes.Load() != 1 {
				t.Fatal("unexpected write attempts")
			}
		})
	}
}

func TestGatewayNeverRetriesAmbiguousWriteOrRedirect(t *testing.T) {
	for _, mode := range []string{"lost-response", "backpressure", "legacy-not-leader", "redirect"} {
		t.Run(mode, func(t *testing.T) {
			var attempts, redirected atomic.Int32
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1); w.WriteHeader(http.StatusOK) }))
			defer target.Close()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/cluster/routes" {
					writeJSON(w, http.StatusOK, routingResponse{Shards: []routingShard{{ID: "shared", Leader: "node"}}})
					return
				}
				attempts.Add(1)
				switch mode {
				case "lost-response":
					_, _ = io.Copy(io.Discard, r.Body)
					conn, _, err := w.(http.Hijacker).Hijack()
					if err == nil {
						_ = conn.Close()
					}
				case "backpressure":
					writeJSON(w, http.StatusTooManyRequests, writeErrorResponse{Code: "backpressure", Retryable: true})
				case "legacy-not-leader":
					writeJSON(w, http.StatusConflict, writeErrorResponse{Code: "not_leader", Retryable: true})
				case "redirect":
					w.Header().Set("Location", target.URL)
					w.WriteHeader(http.StatusTemporaryRedirect)
				}
			}))
			defer server.Close()
			client := &http.Client{Timeout: time.Second}
			g, err := NewGateway(GatewayOptions{BootstrapURL: server.URL, NodeEndpoints: map[string]string{"node": server.URL}, HTTPClient: client})
			if err != nil {
				t.Fatal(err)
			}
			if client.CheckRedirect != nil {
				t.Fatal("caller client was mutated")
			}
			if _, err := g.Put(context.Background(), []byte("k"), []byte("v"), lsm.WriteConsistencyLocalCommitted); err == nil {
				t.Fatal("expected ambiguous failure")
			}
			if attempts.Load() != 1 || redirected.Load() != 0 {
				t.Fatalf("unsafe resend: attempts=%d redirected=%d", attempts.Load(), redirected.Load())
			}
		})
	}
}
