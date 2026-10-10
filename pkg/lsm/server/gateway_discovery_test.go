package server

import (
	"context"
	"errors"
	"io"
	"net"
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

func TestGatewayCannotCertifyCallerTransportDialErrors(t *testing.T) {
	for _, mode := range []string{"default-transport", "registered-protocol"} {
		t.Run(mode, func(t *testing.T) {
			var attempts atomic.Int32
			fake := roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path == "/cluster/routes" {
					rec := httptest.NewRecorder()
					writeJSON(rec, http.StatusOK, routingResponse{Shards: []routingShard{{ID: "shared", Leader: "b"}}})
					return rec.Result(), nil
				}
				attempts.Add(1)
				return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("failure after custom submission")}
			})
			client := &http.Client{}
			if mode == "default-transport" {
				previous := http.DefaultTransport
				http.DefaultTransport = fake
				defer func() { http.DefaultTransport = previous }()
			} else {
				transport := &http.Transport{}
				transport.RegisterProtocol("http", fake)
				client.Transport = transport
			}
			g, err := NewGateway(GatewayOptions{BootstrapURL: "http://a", NodeEndpoints: map[string]string{"a": "http://a", "b": "http://b"}, HTTPClient: client})
			if err != nil {
				t.Fatal(err)
			}
			g.routes = cachedRoutes{shards: []cachedRouteShard{{id: "shared", leader: "a"}}}
			if _, err := g.Put(context.Background(), []byte("k"), []byte("v"), lsm.WriteConsistencyLocalCommitted); err == nil {
				t.Fatal("expected failure")
			}
			if attempts.Load() != 1 {
				t.Fatalf("unsafe retry through caller transport: %d", attempts.Load())
			}
		})
	}
}

func TestGatewayConcurrentRejectionsKeepAttemptedLeader(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	var attemptsA, attemptsB atomic.Int32
	client := newInMemoryHTTPClient(map[string]http.Handler{
		"a": http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/cluster/routes" {
				writeJSON(w, http.StatusOK, routingResponse{Shards: []routingShard{{ID: "shared", Leader: "b"}}})
				return
			}
			if attemptsA.Add(1) == 1 {
				close(entered)
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
			}
			writeJSON(w, http.StatusConflict, writeErrorResponse{Code: "not_leader", Retryable: true, SafeToRetry: true})
		}),
		"b": http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/cluster/routes" {
				writeJSON(w, http.StatusOK, routingResponse{Shards: []routingShard{{ID: "shared", Leader: "b"}}})
				return
			}
			attemptsB.Add(1)
			writeJSON(w, http.StatusOK, lsm.WriteRequestStatus{State: lsm.WriteRequestCommitted})
		}),
	})
	g, err := NewGateway(GatewayOptions{BootstrapURL: "http://a", NodeEndpoints: map[string]string{"a": "http://a", "b": "http://b"}, HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	g.routes = cachedRoutes{shards: []cachedRouteShard{{id: "shared", leader: "a"}}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	first := make(chan error, 1)
	go func() {
		_, err := g.Put(ctx, []byte("first"), []byte("v"), lsm.WriteConsistencyLocalCommitted)
		first <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("first request did not reach old leader")
	}
	if _, err := g.Put(ctx, []byte("second"), []byte("v"), lsm.WriteConsistencyLocalCommitted); err != nil {
		t.Fatal(err)
	}
	// Release via a send so deferred close also releases the waiter on failure.
	select {
	case release <- struct{}{}:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-first:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if attemptsA.Load() != 2 || attemptsB.Load() != 2 {
		t.Fatalf("attempts A=%d B=%d", attemptsA.Load(), attemptsB.Load())
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
