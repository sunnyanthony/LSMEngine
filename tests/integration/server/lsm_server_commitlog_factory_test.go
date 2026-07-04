//go:build test

package integration_test

import (
	"context"
	"sync/atomic"
	"testing"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"lsmengine/pkg/lsm"
)

type integrationCommitLogFactory struct {
	consensus lsm.CommitLogConsensus
}

func (f integrationCommitLogFactory) New(_ lsm.Options) (lsm.CommitLogConsensus, error) {
	return f.consensus, nil
}

type integrationCommitLogConsensus struct{}

func (integrationCommitLogConsensus) CommitControl(_ context.Context, mutation lsm.CommitLogControlMutation) (lsm.CommitLogControlCommittedEntry, error) {
	return lsm.CommitLogControlCommittedEntry{
		Commit:   lsm.CommitLogCommit{Index: 1, Term: 1},
		Mutation: mutation,
	}, nil
}

func (integrationCommitLogConsensus) CommitData(_ context.Context, mutation lsm.CommitLogDataMutation) (lsm.CommitLogDataCommittedEntry, error) {
	return lsm.CommitLogDataCommittedEntry{
		Commit:   lsm.CommitLogCommit{Index: 2, Term: 1},
		Mutation: mutation,
		Seq:      2,
	}, nil
}

func (integrationCommitLogConsensus) HandlePeerMessages(_ context.Context, _ []raftpb.Message) error {
	return nil
}

func (integrationCommitLogConsensus) Provider() lsm.CommitLogProvider {
	return "integration-custom"
}

func (integrationCommitLogConsensus) RuntimeStatus() lsm.CommitLogRuntimeStatus {
	return lsm.CommitLogRuntimeStatus{Mode: "custom", Index: 2, Term: 1, Leader: true, Replicas: 1}
}

type integrationPeerIngressConsensus struct {
	peerCalls atomic.Int64
	peerMsgs  atomic.Int64
}

func (c *integrationPeerIngressConsensus) CommitControl(_ context.Context, mutation lsm.CommitLogControlMutation) (lsm.CommitLogControlCommittedEntry, error) {
	return lsm.CommitLogControlCommittedEntry{
		Commit:   lsm.CommitLogCommit{Index: 1, Term: 1},
		Mutation: mutation,
	}, nil
}

func (c *integrationPeerIngressConsensus) CommitData(_ context.Context, mutation lsm.CommitLogDataMutation) (lsm.CommitLogDataCommittedEntry, error) {
	return lsm.CommitLogDataCommittedEntry{
		Commit:   lsm.CommitLogCommit{Index: 2, Term: 1},
		Mutation: mutation,
		Seq:      2,
	}, nil
}

func (c *integrationPeerIngressConsensus) HandlePeerMessages(_ context.Context, messages []raftpb.Message) error {
	c.peerCalls.Add(1)
	c.peerMsgs.Add(int64(len(messages)))
	return nil
}

func (c *integrationPeerIngressConsensus) Provider() lsm.CommitLogProvider {
	return "integration-custom"
}

func (c *integrationPeerIngressConsensus) RuntimeStatus() lsm.CommitLogRuntimeStatus {
	return lsm.CommitLogRuntimeStatus{Mode: "custom", Index: 2, Term: 1, Leader: true, Replicas: 1}
}

func TestServerCanUseCustomCommitLogFactory(t *testing.T) {
	store, err := lsm.New(lsm.Options{
		DataDir: t.TempDir(),
		CommitLog: &lsm.CommitLogOptions{
			Factory: integrationCommitLogFactory{
				consensus: integrationCommitLogConsensus{},
			},
		},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}()

	if err := store.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	status := store.ClusterStatus()
	if status.CommitLog != "integration-custom" {
		t.Fatalf("expected custom commit log provider, got %q", status.CommitLog)
	}
}

func TestServerCommitLogFactorySupportsPeerIngress(t *testing.T) {
	consensus := &integrationPeerIngressConsensus{}
	store, err := lsm.New(lsm.Options{
		DataDir: t.TempDir(),
		CommitLog: &lsm.CommitLogOptions{
			Factory: integrationCommitLogFactory{consensus: consensus},
		},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}()

	if err := store.HandlePeerMessages(context.Background(), []raftpb.Message{
		{Type: raftpb.MsgHeartbeat, From: 2, To: 1, Term: 1},
		{Type: raftpb.MsgApp, From: 2, To: 1, Term: 1},
	}); err != nil {
		t.Fatalf("handle peer messages: %v", err)
	}
	if got := consensus.peerCalls.Load(); got != 1 {
		t.Fatalf("expected one peer ingress call, got %d", got)
	}
	if got := consensus.peerMsgs.Load(); got != 2 {
		t.Fatalf("expected two peer messages, got %d", got)
	}
}
