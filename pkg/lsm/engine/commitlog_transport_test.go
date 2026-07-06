package engine

import (
	"context"
	"strings"
	"sync"
	"testing"
)

type recordingRaftTransport struct {
	mu       sync.Mutex
	messages []RaftPeerMessage
}

func (r *recordingRaftTransport) Send(_ context.Context, messages []RaftPeerMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, messages...)
	return nil
}

func (r *recordingRaftTransport) Messages() []RaftPeerMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneRaftPeerMessages(r.messages)
}

func TestEtcdRaftCommitLogBootstrapWithTransport(t *testing.T) {
	transport := &recordingRaftTransport{}
	_, err := newEtcdRaftCommitLogConsensus(Options{
		DataDir: t.TempDir(),
		NodeID:  "node-a",
		CommitLog: &CommitLogOptions{
			Provider:  CommitLogProviderEtcdRaft,
			Transport: transport,
		},
		Raft: &RaftOptions{
			Peers: []string{"node-a", "node-b"},
		},
	})
	if err != nil {
		t.Fatalf("new etcd raft consensus: %v", err)
	}
}

func TestEtcdRaftCommitLogPeerMessagesRequireTransport(t *testing.T) {
	_, err := newEtcdRaftCommitLogConsensus(Options{
		DataDir: t.TempDir(),
		NodeID:  "node-a",
		CommitLog: &CommitLogOptions{
			Provider: CommitLogProviderEtcdRaft,
		},
		Raft: &RaftOptions{
			Peers: []string{"node-a", "node-b"},
		},
	})
	if err == nil {
		t.Fatalf("expected transport error")
	}
	if !strings.Contains(err.Error(), "transport") {
		t.Fatalf("expected transport error, got %v", err)
	}
}
