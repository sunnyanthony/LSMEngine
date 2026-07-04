package commitlog

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

func (r *recordingRaftTransport) messagesCopy() []raftpb.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]raftpb.Message, len(r.messages))
	copy(out, r.messages)
	return out
}

func TestEtcdRaftConsensusSendsPeerMessagesViaTransport(t *testing.T) {
	transport := &recordingRaftTransport{}
	consensus, err := newEtcdRaftConsensus(Config{
		Provider:  ProviderEtcdRaft,
		NodeID:    "node-a",
		Peers:     []string{"node-a", "node-b"},
		Transport: transport,
	})
	if err != nil {
		t.Fatalf("new etcd raft consensus: %v", err)
	}

	consensus.mu.Lock()
	defer consensus.mu.Unlock()
	if err := consensus.rawNode.Campaign(); err != nil {
		t.Fatalf("campaign: %v", err)
	}
	if err := consensus.advanceUntilStableLocked(context.Background()); err != nil {
		t.Fatalf("advance: %v", err)
	}

	messages := transport.messagesCopy()
	if len(messages) == 0 {
		t.Fatalf("expected transport to receive raft peer messages")
	}
	for _, msg := range messages {
		if msg.To == consensus.nodeID || msg.To == 0 {
			t.Fatalf("expected only peer-targeted outbound messages, got To=%d", msg.To)
		}
	}
}

func TestEtcdRaftConsensusRequiresTransportForMultiPeer(t *testing.T) {
	_, err := newEtcdRaftConsensus(Config{
		Provider: ProviderEtcdRaft,
		NodeID:   "node-a",
		Peers:    []string{"node-a", "node-b"},
	})
	if err == nil {
		t.Fatalf("expected transport requirement error")
	}
	if !strings.Contains(err.Error(), "transport") {
		t.Fatalf("expected transport error, got %v", err)
	}
}

func TestEtcdRaftConsensusHandlePeerMessagesIgnoresOtherTargets(t *testing.T) {
	transport := &recordingRaftTransport{}
	consensus, err := newEtcdRaftConsensus(Config{
		Provider:  ProviderEtcdRaft,
		NodeID:    "node-a",
		Peers:     []string{"node-a", "node-b"},
		Transport: transport,
	})
	if err != nil {
		t.Fatalf("new etcd raft consensus: %v", err)
	}
	other := stableRaftNodeID("node-b")
	if err := consensus.HandlePeerMessages(context.Background(), []raftpb.Message{
		{
			Type: raftpb.MsgHeartbeat,
			From: other,
			To:   other,
			Term: 1,
		},
	}); err != nil {
		t.Fatalf("handle peer messages: %v", err)
	}
}

func TestEtcdRaftConsensusHandlePeerMessagesReturnsStepError(t *testing.T) {
	consensus, err := newEtcdRaftConsensus(Config{
		Provider: ProviderEtcdRaft,
		NodeID:   "node-a",
	})
	if err != nil {
		t.Fatalf("new etcd raft consensus: %v", err)
	}
	err = consensus.HandlePeerMessages(context.Background(), []raftpb.Message{
		{
			Type: raftpb.MsgHup,
			To:   consensus.nodeID,
		},
	})
	if err == nil {
		t.Fatalf("expected step error")
	}
	if !strings.Contains(err.Error(), "step") {
		t.Fatalf("expected step error, got %v", err)
	}
}
