package server

import (
	"context"
	"fmt"
	"net/http"
	"testing"
)

func TestGatewayAdaptiveReadTargetsRespectReadConstraints(t *testing.T) {
	for _, mode := range []GatewayReadMode{GatewayReadModeAny, GatewayReadModeLeader} {
		t.Run(string(mode), func(t *testing.T) {
			limit := uint64(0)
			endpoints := map[string]string{"node-a": "http://node-a", "node-b": "http://node-b"}
			backend := func(leader bool, lag uint64) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					fmt.Fprintf(w, `{"commit_log_runtime":{"leader":%t,"write_available":%t,"apply_lag":%d}}`, leader, leader, lag)
				})
			}
			g, err := NewGateway(GatewayOptions{
				BootstrapURL: "http://node-a", NodeEndpoints: endpoints,
				ReadMode: mode, ReadBalancePolicy: GatewayReadBalanceAdaptive, MaxReadApplyLag: &limit,
				HTTPClient: newInMemoryHTTPClient(map[string]http.Handler{
					"node-a": backend(false, 5), "node-b": backend(true, 0),
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			// Prefer node-a by score, but it is neither caught up nor the leader.
			g.recordEndpointReadAttempt("http://node-b", false)
			targets, err := g.readTargets(context.Background(), endpoints, true)
			if err != nil || len(targets) != 1 || targets[0].endpoint != "http://node-b" {
				t.Fatalf("adaptive routing bypassed read constraints: targets=%+v err=%v", targets, err)
			}
		})
	}
}

func TestGatewayAdaptiveCooldownPrecedesCumulativeScore(t *testing.T) {
	g, err := NewGateway(GatewayOptions{
		BootstrapURL:      "http://node-a",
		NodeEndpoints:     map[string]string{"node-a": "http://node-a", "node-b": "http://node-b"},
		ReadBalancePolicy: GatewayReadBalanceAdaptive,
	})
	if err != nil {
		t.Fatal(err)
	}
	endpoints := map[string]string{"node-a": "http://node-a", "node-b": "http://node-b"}
	g.recordEndpointReadAttempt("http://node-b", false)
	g.markEndpointFailure("http://node-a")
	if ids := g.readNodeEndpointIDs(endpoints); ids[0] != "node-b" {
		t.Fatalf("cooldown must precede lower failure score: %v", ids)
	}
	g.markEndpointSuccess("http://node-a")
	if ids := g.readNodeEndpointIDs(endpoints); ids[0] != "node-a" {
		t.Fatalf("cleared cooldown must restore cumulative ordering: %v", ids)
	}
}
