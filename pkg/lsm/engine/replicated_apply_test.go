package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"lsmengine/pkg/lsm/errs"
)

type applyLoopbackTransport struct{ nodes map[uint64]*LSM }

func (t *applyLoopbackTransport) Send(ctx context.Context, messages []CommitLogPeerMessage) error {
	for _, message := range messages {
		node := t.nodes[message.To]
		if node == nil {
			return fmt.Errorf("unknown target %d", message.To)
		}
		if err := node.HandlePeerMessages(ctx, []CommitLogPeerMessage{message}); err != nil {
			return err
		}
	}
	return nil
}

func newApplyPair(t *testing.T) ([]*LSM, []Options, *applyLoopbackTransport) {
	t.Helper()
	transport := &applyLoopbackTransport{nodes: make(map[uint64]*LSM)}
	var stores []*LSM
	var configs []Options
	for _, node := range []string{"node-a", "node-b"} {
		opts := Options{DataDir: t.TempDir(), NodeID: node, CommitLog: &CommitLogOptions{Provider: CommitLogProviderEtcdRaft, Transport: transport},
			Raft: &RaftOptions{Peers: []string{"node-a", "node-b"}}, ShardMap: []ShardConfig{{ID: "shared", Leader: "node-a", Replicas: []string{"node-a", "node-b"}}}}
		store, err := New(opts)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { store.Close() })
		stores = append(stores, store)
		configs = append(configs, opts)
		transport.nodes[RaftPeerID(node)] = store
	}
	return stores, configs, transport
}

func TestReplicatedRevisionRejectionSurvivesBothRestarts(t *testing.T) {
	stores, configs, transport := newApplyPair(t)
	zero := uint64(0)
	var rejected controlCommittedEntry
	for _, target := range []string{"node-b", "node-a"} {
		var err error
		rejected, err = stores[0].commitLog.CommitControl(context.Background(), controlMutation{Kind: "transfer-leader", ShardID: "shared", Target: target, OperationID: target, ExpectedRevision: &zero})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, store := range stores {
		if err := store.committedApply.drain(rejected.Commit.Index); !errors.Is(err, errs.ErrControlRevisionConflict) {
			t.Fatalf("revision outcome: %v", err)
		}
		if store.control.revision != 1 || store.Shards()[0].Leader != "node-b" || store.control.commitLogAppliedIndex != rejected.Commit.Index {
			t.Fatal("replica rejection state differs")
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for i, opts := range configs {
		store, err := New(opts)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { store.Close() })
		stores[i] = store
		transport.nodes[RaftPeerID(opts.NodeID)] = store
		if store.control.revision != 1 || store.Shards()[0].Leader != "node-b" || store.control.commitLogAppliedIndex != rejected.Commit.Index {
			t.Fatal("restart lost rejection progress")
		}
	}
}

func TestAdmittedProposalDoesNotBlockInboundControlApplication(t *testing.T) {
	stores, _, _ := newApplyPair(t)
	original := stores[0].commitLog
	blocked := &blockedProposalConsensus{commitLogConsensus: original, entered: make(chan struct{}), release: make(chan struct{})}
	stores[0].commitLog = blocked
	released := false
	defer func() {
		if !released {
			close(blocked.release)
		}
	}()
	written := make(chan error, 1)
	go func() { written <- stores[0].Put([]byte("key"), []byte("value")) }()
	select {
	case <-blocked.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("proposal admission did not start")
	}
	// A committed control entry arrives through peer replies while the local
	// data caller holds its proposal mutex but has not submitted its payload.
	entry, err := original.CommitControl(context.Background(), controlMutation{Kind: "transfer-leader", ShardID: "shared", Target: "node-a", OperationID: "interleave"})
	if err != nil {
		t.Fatal(err)
	}
	for _, store := range stores {
		if err := store.committedApply.drain(entry.Commit.Index); err != nil {
			t.Fatal(err)
		}
		if store.control.revision != 1 {
			t.Fatal("inbound control could not apply ahead of unsubmitted data")
		}
	}
	close(blocked.release)
	released = true
	select {
	case err := <-written:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("data proposal did not finish")
	}
	for _, store := range stores {
		value, ok := store.Get([]byte("key"))
		if !ok || value.Seq <= entry.Commit.Index {
			t.Fatal("data did not follow committed control")
		}
	}
}
