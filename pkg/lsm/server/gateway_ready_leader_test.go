package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGatewayReadyLeaderLagCannotBeMaskedByFollower(t *testing.T) {
	for _, mode := range []GatewayReadMode{GatewayReadModeLeader, GatewayReadModeAny} {
		t.Run(string(mode), func(t *testing.T) {
			leaderLag := uint64(5)
			backend := func(leader bool) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					lag, health := uint64(0), "follower"
					if leader {
						lag, health = leaderLag, "ready"
					}
					fmt.Fprintf(w, `{"commit_log_runtime":{"leader":%t,"write_available":%t,"health":%q,"apply_lag":%d}}`, leader, leader, health, lag)
				})
			}
			g, err := NewGateway(GatewayOptions{
				BootstrapURL: "http://node-a", ReadMode: mode,
				NodeEndpoints: map[string]string{"node-a": "http://node-a", "node-b": "http://node-b"},
				HTTPClient: newInMemoryHTTPClient(map[string]http.Handler{
					"node-a": backend(true), "node-b": backend(false),
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			limit := uint64(0)
			handler := NewGatewayHandler(g, HandlerOptions{GatewayReadyMaxReadApplyLag: &limit})
			check := func(want int) {
				t.Helper()
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
				if rec.Code != want {
					t.Fatalf("readiness=%d want=%d body=%s", rec.Code, want, rec.Body.String())
				}
			}
			want := http.StatusOK
			if mode == GatewayReadModeLeader {
				want = http.StatusServiceUnavailable
			}
			check(want)
			leaderLag = 0
			check(http.StatusOK)
		})
	}
}
