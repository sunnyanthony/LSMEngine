package commitlog

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

func TestReadyPersistenceFailureStopsProviderUntilReopen(t *testing.T) {
	cfg := Config{DataDir: t.TempDir(), NodeID: "node-a"}
	c, err := newEtcdRaftConsensus(cfg)
	if err != nil {
		t.Fatal(err)
	}
	before := c.RuntimeStatus()
	// A directory at the temporary-file path rejects the actual file write,
	// without modifying the last durable Raft state.
	if err := os.Mkdir(c.storage.path+".tmp", 0700); err != nil {
		t.Fatal(err)
	}
	waiting := &pendingRaftProposal{ready: make(chan struct{})}
	c.pending[999] = waiting
	_, failure := c.CommitData(context.Background(), DataMutation{Kind: "put", Key: []byte("lost"), Value: []byte("unpersisted")})
	if failure == nil {
		t.Fatal("write unexpectedly succeeded")
	}
	select {
	case <-waiting.ready:
	default:
		t.Error("failure left another proposer asleep")
	}
	if err := os.Remove(c.storage.path + ".tmp"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := c.Tick(context.Background()); err != failure {
			t.Errorf("tick did not preserve terminal error: %v", err)
		}
		if _, err := c.CommitData(context.Background(), DataMutation{Kind: "put", Key: []byte("later"), Value: []byte("value")}); err != failure {
			t.Fatalf("data proposal did not preserve terminal error: %v", err)
		}
		if _, err := c.CommitControl(context.Background(), ControlMutation{Kind: "drain"}); err != failure {
			t.Fatalf("control proposal did not preserve terminal error: %v", err)
		}
		if err := c.HandlePeerMessages(context.Background(), []PeerMessage{{From: 2, To: c.nodeID}}); err != failure {
			t.Fatalf("ingress did not preserve terminal error: %v", err)
		}
	}
	if after := c.RuntimeStatus(); after.Index != before.Index || after.Term != before.Term {
		t.Fatalf("failed Ready advanced commitment: before=%+v after=%+v", before, after)
	}
	if len(c.RecoveredEntries()) != 0 {
		t.Fatal("failed entry was exposed as committed")
	}
	reopened, err := newEtcdRaftConsensus(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(reopened.RecoveredEntries()) != 0 {
		t.Fatal("unpersisted entry survived reopen")
	}
	if _, err := reopened.CommitData(context.Background(), DataMutation{Kind: "put", Key: []byte("recovered"), Value: []byte("value")}); err != nil {
		t.Fatalf("reopened provider did not resume: %v", err)
	}
}

type blockedReadyTransport struct {
	entered chan struct{}
	release chan struct{}
	err     error
}

func (b *blockedReadyTransport) Send(ctx context.Context, _ []PeerMessage) error {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	select {
	case <-b.release:
		return b.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestReadyFailureWinsOverConcurrentTransportResult(t *testing.T) {
	for _, delivery := range []bool{false, true} {
		name := "generic-error"
		if delivery {
			name = "delivery-error"
		}
		t.Run(name, func(t *testing.T) {
			other := stableRaftNodeID("node-b")
			transport := &blockedReadyTransport{entered: make(chan struct{}, 1), release: make(chan struct{}), err: errors.New("transport failed")}
			if delivery {
				transport.err = &DeliveryError{Peers: []uint64{other}, Cause: transport.err}
			}
			c, err := newEtcdRaftConsensus(Config{DataDir: t.TempDir(), NodeID: "node-a", Peers: []string{"node-a", "node-b"}, Transport: transport})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			var release sync.Once
			defer func() {
				release.Do(func() { close(transport.release) })
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("blocked transport did not exit")
				}
			}()
			go func() {
				c.mu.Lock()
				defer c.mu.Unlock()
				if err := c.rawNode.Campaign(); err != nil {
					done <- err
					return
				}
				done <- c.advanceUntilStableLocked(context.Background())
			}()
			select {
			case <-transport.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("campaign did not enter transport")
			}
			if err := os.Mkdir(c.storage.path+".tmp", 0700); err != nil {
				t.Fatal(err)
			}
			messages, err := encodeRaftPeerMessages([]raftpb.Message{{Type: raftpb.MsgVote, From: other, To: c.nodeID, Term: 10, LogTerm: 1, Index: 2}})
			if err != nil {
				t.Fatal(err)
			}
			failure := c.HandlePeerMessages(context.Background(), messages)
			if failure == nil {
				t.Fatal("concurrent persistence unexpectedly succeeded")
			}
			release.Do(func() { close(transport.release) })
			select {
			case result := <-done:
				// Leave a completion signal for deferred goroutine cleanup.
				done <- result
				if result != failure {
					t.Fatalf("transport replaced terminal failure: got %v, want %v", result, failure)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("transport did not return")
			}
			if c.RuntimeStatus().DeliveryFailures != 0 {
				t.Fatal("transport mutated terminal provider delivery state")
			}
		})
	}
}
