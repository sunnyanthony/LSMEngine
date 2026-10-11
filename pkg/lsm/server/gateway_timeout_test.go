package server

import (
	"net/http"
	"testing"
	"time"
)

func TestGatewayOwnedRequestTimeout(t *testing.T) {
	for _, requested := range []time.Duration{0, 7 * time.Second} {
		g, err := NewGateway(GatewayOptions{BootstrapURL: "http://localhost", NodeEndpoints: map[string]string{"a": "http://localhost"}, RequestTimeout: requested})
		if err != nil {
			t.Fatal(err)
		}
		want := requested
		if want == 0 {
			want = 3 * time.Second
		}
		if g.client.Timeout != want || !g.dialRetry {
			t.Fatalf("owned timeout=%v dialRetry=%v", g.client.Timeout, g.dialRetry)
		}
	}
	client := &http.Client{Timeout: 2 * time.Second}
	for _, opts := range []GatewayOptions{
		{BootstrapURL: "http://localhost", NodeEndpoints: map[string]string{"a": "http://localhost"}, RequestTimeout: -time.Second},
		{BootstrapURL: "http://localhost", NodeEndpoints: map[string]string{"a": "http://localhost"}, RequestTimeout: time.Second, HTTPClient: client},
	} {
		if _, err := NewGateway(opts); err == nil {
			t.Fatal("invalid timeout policy accepted")
		}
	}
	if client.Timeout != 2*time.Second || client.CheckRedirect != nil {
		t.Fatal("caller client mutated")
	}
}
