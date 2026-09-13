package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"lsmengine/pkg/lsm"
)

func TestRaftHTTPDeliveryReportsEveryPeerIndependently(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/ok/"):
			w.WriteHeader(http.StatusAccepted)
		case strings.HasPrefix(r.URL.Path, "/redirect/"):
			w.WriteHeader(http.StatusTemporaryRedirect)
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer peer.Close()
	type result struct {
		id  uint64
		err error
	}
	results := make(chan result, 4)
	diagnostics := make(chan error, 4)
	transport, err := NewRaftHTTPTransport(RaftHTTPTransportOptions{
		PeerResolver: testRaftPeerResolver{endpoints: map[uint64]string{1: peer.URL + "/ok", 2: peer.URL + "/fail", 3: peer.URL + "/redirect"}},
		HTTPClient:   &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		OnError:      func(err error) { diagnostics <- err },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := transport.SendWithResult(context.Background(), []lsm.CommitLogPeerMessage{
		{To: 4}, {To: 2}, {To: 1}, {To: 3}, {To: 1},
	}, func(id uint64, err error) { results <- result{id, err} }); err != nil {
		t.Fatalf("per-peer errors must be reported, not reject the batch: %v", err)
	}
	seen := make(map[uint64]bool)
	for i := 0; i < 4; i++ {
		select {
		case got := <-results:
			if seen[got.id] || (got.id == 1) != (got.err == nil) {
				t.Fatalf("unexpected/duplicate delivery result: %+v seen=%v", got, seen)
			}
			seen[got.id] = true
		case <-time.After(3 * time.Second):
			t.Fatalf("missing peer results: %v", seen)
		}
	}
	for i := 0; i < 3; i++ {
		select {
		case <-diagnostics:
		case <-time.After(time.Second):
			t.Fatal("delivery errors must retain OnError diagnostics")
		}
	}
}
