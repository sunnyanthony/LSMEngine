package engine

import (
	"context"
	"strings"
	"sync"
	"testing"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

type recordingRaftTransport struct {
	mu       sync.Mutex
	messages []raftpb.Message
}

func (r *recordingRaftTransport) Send(_ context.Context, messages []raftpb.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, messages...)
	return nil
}

func (r *recordingRaftTransport) Messages() []raftpb.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]raftpb.Message, len(r.messages))
	copy(out, r.messages)
	return out
}

func TestEtcdRaftCommitLogCampaignUsesTransportForPeerMessages(t *testing.T) {
	transport := &recordingRaftTransport{}
	consensus, err := newEtcdRaftCommitLogConsensus(Options{
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

	consensus.mu.Lock()
	defer consensus.mu.Unlock()
	err = consensus.rawNode.Campaign()
	if err == nil {
		err = consensus.advanceUntilStableLocked(context.Background())
	}
	if err != nil {
		t.Fatalf("campaign/advance: %v", err)
	}

	messages := transport.Messages()
	if len(messages) == 0 {
		t.Fatalf("expected raft transport to receive peer messages")
	}
	foundPeerMessage := false
	for _, msg := range messages {
		if msg.To == stableRaftNodeID("node-b") {
			foundPeerMessage = true
			break
		}
	}
	if !foundPeerMessage {
		t.Fatalf("expected transport message addressed to node-b")
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
