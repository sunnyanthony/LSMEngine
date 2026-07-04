//go:build test

package integration_test

import (
	"context"
	"testing"

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

func (integrationCommitLogConsensus) Provider() lsm.CommitLogProvider {
	return "integration-custom"
}

func (integrationCommitLogConsensus) RuntimeStatus() lsm.CommitLogRuntimeStatus {
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
