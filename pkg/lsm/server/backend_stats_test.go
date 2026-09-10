package server

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"lsmengine/pkg/lsm"
)

func TestGatewayBackendWriteStatsRequireDecodedResponse(t *testing.T) {
	for _, code := range []int{http.StatusOK, http.StatusAccepted} {
		for _, body := range []string{"{", "{}"} {
			t.Run(fmt.Sprintf("%d/%s", code, body), func(t *testing.T) {
				g, err := NewGateway(GatewayOptions{
					BootstrapURL:  "http://node-a",
					NodeEndpoints: map[string]string{"node-a": "http://node-a"},
					HTTPClient: newInMemoryHTTPClient(map[string]http.Handler{
						"node-a": http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							w.WriteHeader(code)
							fmt.Fprint(w, body)
						}),
					}),
				})
				if err != nil {
					t.Fatal(err)
				}
				_, err = g.postWrite(context.Background(), "http://node-a", "put", []byte("k"), []byte("v"), lsm.WriteConsistencyLocalCommitted)
				stats := g.endpointRoutingStats("http://node-a")
				wantSuccess := uint64(0)
				if body == "{}" {
					wantSuccess = 1
				}
				if (err == nil) != (wantSuccess == 1) || stats.WriteAttempts != 1 || stats.WriteSuccesses != wantSuccess || stats.WriteFailures != 1-wantSuccess {
					t.Fatalf("err=%v stats=%+v", err, stats)
				}
			})
		}
	}
}

func TestGatewayBackendProbeStatsStopAfterCancellation(t *testing.T) {
	for _, mode := range []string{"cluster", "leader", "filtered"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				cancel()
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
			})
			endpoints := map[string]string{"node-a": "http://node-a", "node-b": "http://node-b"}
			g, err := NewGateway(GatewayOptions{
				BootstrapURL: "http://node-a", NodeEndpoints: endpoints,
				ReadBalancePolicy: GatewayReadBalanceFreshest,
				HTTPClient:        newInMemoryHTTPClient(map[string]http.Handler{"node-a": backend, "node-b": backend}),
			})
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "cluster":
				_, err = g.ClusterStatus(ctx)
			case "leader":
				_, _, err = g.currentWriteLeader(ctx, endpoints)
			case "filtered":
				_, err = g.readTargets(ctx, endpoints, true)
			}
			a, b := g.endpointRoutingStats("http://node-a"), g.endpointRoutingStats("http://node-b")
			if err == nil || calls != 1 || a.StatusProbeAttempts != 1 || a.StatusProbeFailures != 1 || b != (GatewayBackendStats{}) {
				t.Fatalf("err=%v calls=%d a=%+v b=%+v", err, calls, a, b)
			}
			if degraded, _ := g.endpointHealth("http://node-a"); degraded {
				t.Fatal("caller cancellation degraded backend")
			}
		})
	}
}

func TestGatewayUnknownLagCountsFailedProbe(t *testing.T) {
	g, err := NewGateway(GatewayOptions{
		BootstrapURL:  "http://node-a",
		NodeEndpoints: map[string]string{"node-a": "http://node-a"},
		HTTPClient: newInMemoryHTTPClient(map[string]http.Handler{
			"node-a": http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "{}") }),
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = g.backendStatus(context.Background(), "http://node-a")
	stats := g.endpointRoutingStats("http://node-a")
	if err == nil || stats.StatusProbeAttempts != 1 || stats.StatusProbeFailures != 1 || stats.StatusProbeSuccesses != 0 {
		t.Fatalf("err=%v stats=%+v", err, stats)
	}
}
