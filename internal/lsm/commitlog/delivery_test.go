package commitlog

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

type failingDeliveryTransport struct{ err error }

func (t failingDeliveryTransport) Send(context.Context, []PeerMessage) error { return t.err }

func TestDeliveryClassificationIsDirectAndBatchScoped(t *testing.T) {
	peer := stableRaftNodeID("node-b")
	for _, tc := range []struct {
		name        string
		failure     error
		recoverable bool
	}{
		{"direct", &DeliveryError{Peers: []uint64{peer}}, true},
		{"wrapped", fmt.Errorf("other failure: %w", &DeliveryError{Peers: []uint64{peer}}), false},
		{"empty", &DeliveryError{}, false},
		{"wrong-destination", &DeliveryError{Peers: []uint64{peer, 123}}, false},
		{"ordinary", errors.New("local failure"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := newEtcdRaftConsensus(Config{DataDir: t.TempDir(), NodeID: "node-a", Peers: []string{"node-a", "node-b"}, Transport: failingDeliveryTransport{err: tc.failure}})
			if err != nil {
				t.Fatal(err)
			}
			c.mu.Lock()
			if err := c.rawNode.Campaign(); err != nil {
				c.mu.Unlock()
				t.Fatal(err)
			}
			err = c.advanceUntilStableLocked(context.Background())
			c.mu.Unlock()
			if (err == nil) != tc.recoverable {
				t.Fatalf("classification: %v", err)
			}
			want := uint64(0)
			if tc.recoverable {
				want = 1
			}
			if c.RuntimeStatus().DeliveryFailures != want {
				t.Fatal("delivery telemetry differs")
			}
		})
	}
}
