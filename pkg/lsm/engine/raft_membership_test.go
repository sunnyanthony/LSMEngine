package engine

import (
	"strings"
	"testing"
)

func TestLocalRaftPeerMembershipChangeIsNoop(t *testing.T) {
	store, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()
	if err := store.AddRaftPeer("node-b"); err != nil {
		t.Fatalf("add local raft peer: %v", err)
	}
	if err := store.RemoveRaftPeer("node-b"); err != nil {
		t.Fatalf("remove local raft peer: %v", err)
	}
}

func TestCustomCommitLogRaftPeerMembershipRequiresOptionalContract(t *testing.T) {
	consensus := &testCommitLogConsensus{provider: "custom"}
	store, err := New(Options{
		DataDir: t.TempDir(),
		CommitLog: &CommitLogOptions{
			Factory: &testCommitLogFactory{consensus: consensus},
		},
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()
	err = store.AddRaftPeer("node-b")
	if err == nil {
		t.Fatalf("expected unsupported membership change error")
	}
	if !strings.Contains(err.Error(), "does not support raft membership changes") {
		t.Fatalf("expected unsupported membership change error, got %v", err)
	}
}
