package commitlog

import (
	"context"
	"os"
	"testing"
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
