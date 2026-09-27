package engine

import (
	"context"
	"testing"
	"time"
)

func TestRaftRecoveryDuplicateOperationAtLaterIndex(t *testing.T) {
	opts := Options{DataDir: t.TempDir(), NodeID: "node-a", CommitLog: &CommitLogOptions{Provider: CommitLogProviderEtcdRaft},
		ShardMap: []ShardConfig{{ID: "shared", Leader: "node-a", Replicas: []string{"node-a", "node-b"}}}}
	db, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.TransferLeaderWithOptions("shared", "node-b", ControlWriteOptions{OperationID: "transfer-1"}); err != nil {
		t.Fatal(err)
	}
	first := db.control.commitLogAppliedIndex
	duplicate, err := db.commitLog.CommitControl(context.Background(), controlMutation{Kind: "transfer-leader", ShardID: "shared", Target: "node-b", OperationID: "transfer-1"})
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.Commit.Index <= first {
		t.Fatal("fixture needs a later committed index")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		reopened, err := New(opts)
		if err != nil {
			t.Fatal(err)
		}
		if reopened.control.commitLogAppliedIndex != duplicate.Commit.Index || reopened.control.revision != 1 || reopened.Shards()[0].Leader != "node-b" {
			reopened.Close()
			t.Fatal("duplicate changed state or failed to durably advance")
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCommittedDrainDoesNotAcquireProposalLock(t *testing.T) {
	for _, node := range []string{"node-a", "node-b"} {
		t.Run(node, func(t *testing.T) {
			opts := Options{DataDir: t.TempDir(), NodeID: node,
				ShardMap: []ShardConfig{{ID: "shared", Leader: "node-a", Replicas: []string{"node-a", "node-b"}}}}
			db, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			entry := controlCommittedEntry{Commit: CommitLogCommit{Index: 10, Term: 1}, Mutation: controlMutation{Kind: "prepare-drain", NodeID: "node-a", OperationID: "drain-a"}}
			db.control.commitMu.Lock()
			done := make(chan error, 1)
			go func() { done <- db.control.applyCommittedControlFromLog(entry) }()
			select {
			case err := <-done:
				db.control.commitMu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				db.control.commitMu.Unlock()
				<-done
				t.Fatal("committed execution waited for proposal lock")
			}
			if db.control.draining != (node == "node-a") {
				t.Fatal("drain changed wrong node's local state")
			}
			if got := db.Shards(); len(got) != 1 || got[0].Leader != "node-b" {
				t.Fatalf("routing not replicated: %+v", got)
			}
			if db.control.commitLogAppliedIndex != 10 || db.control.revision != 1 {
				t.Fatal("control progress not recorded")
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if reopened.control.draining != (node == "node-a") || reopened.control.commitLogAppliedIndex != 10 {
				t.Fatal("checkpoint lost replicated drain")
			}
		})
	}
}
