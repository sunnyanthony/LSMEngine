//go:build test

package integration_test

import (
	"context"
	"strings"
	"testing"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"lsmengine/pkg/lsm"
)

type noopRaftTransport struct{}

func (noopRaftTransport) Send(_ context.Context, _ []raftpb.Message) error {
	return nil
}

func TestEtcdRaftMultiPeerRequiresTransport(t *testing.T) {
	_, err := lsm.New(lsm.Options{
		DataDir: t.TempDir(),
		NodeID:  "node-a",
		CommitLog: &lsm.CommitLogOptions{
			Provider: lsm.CommitLogProviderEtcdRaft,
		},
		Raft: &lsm.RaftOptions{
			Peers: []string{"node-a", "node-b"},
		},
	})
	if err == nil {
		t.Fatalf("expected transport requirement error")
	}
	if !strings.Contains(err.Error(), "transport is required") {
		t.Fatalf("expected transport requirement error, got %v", err)
	}
}

func TestEtcdRaftMultiPeerWithTransportCanBootstrap(t *testing.T) {
	store, err := lsm.New(lsm.Options{
		DataDir: t.TempDir(),
		NodeID:  "node-a",
		CommitLog: &lsm.CommitLogOptions{
			Provider:  lsm.CommitLogProviderEtcdRaft,
			Transport: noopRaftTransport{},
		},
		Raft: &lsm.RaftOptions{
			Peers: []string{"node-a", "node-b"},
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

	status := store.ClusterStatus()
	if status.CommitLog != string(lsm.CommitLogProviderEtcdRaft) {
		t.Fatalf("expected etcd-raft provider, got %q", status.CommitLog)
	}
	if status.CommitLogRuntime.Mode != "raft_transport_foundation" {
		t.Fatalf("expected transport foundation mode, got %q", status.CommitLogRuntime.Mode)
	}
	if status.CommitLogRuntime.Replicas != 2 {
		t.Fatalf("expected two configured raft peers, got %d", status.CommitLogRuntime.Replicas)
	}
}
