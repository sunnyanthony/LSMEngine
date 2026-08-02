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
	messages []PeerMessage
}

func (r *recordingRaftTransport) Send(_ context.Context, messages []PeerMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, messages...)
	return nil
}

func (r *recordingRaftTransport) messagesCopy() []PeerMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]PeerMessage, len(r.messages))
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
		if len(msg.Payload) == 0 {
			t.Fatalf("expected encoded payload")
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
	messages, err := encodeRaftPeerMessages([]raftpb.Message{
		{Type: raftpb.MsgHeartbeat, From: other, To: other, Term: 1},
	})
	if err != nil {
		t.Fatalf("encode peer messages: %v", err)
	}
	if err := consensus.HandlePeerMessages(context.Background(), messages); err != nil {
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
	messages, err := encodeRaftPeerMessages([]raftpb.Message{
		{Type: raftpb.MsgHup, To: consensus.nodeID},
	})
	if err != nil {
		t.Fatalf("encode peer messages: %v", err)
	}
	err = consensus.HandlePeerMessages(context.Background(), messages)
	if err == nil {
		t.Fatalf("expected step error")
	}
	if !strings.Contains(err.Error(), "step") {
		t.Fatalf("expected step error, got %v", err)
	}
}

func TestEtcdRaftConsensusRejectsInvalidPeerMessageEnvelope(t *testing.T) {
	consensus, err := newEtcdRaftConsensus(Config{
		Provider: ProviderEtcdRaft,
		NodeID:   "node-a",
	})
	if err != nil {
		t.Fatalf("new etcd raft consensus: %v", err)
	}
	messages, err := encodeRaftPeerMessages([]raftpb.Message{
		{Type: raftpb.MsgHeartbeat, From: 2, To: consensus.nodeID, Term: 1},
	})
	if err != nil {
		t.Fatalf("encode peer messages: %v", err)
	}
	messages[0].To = consensus.nodeID + 1
	err = consensus.HandlePeerMessages(context.Background(), messages)
	if err == nil {
		t.Fatalf("expected envelope mismatch error")
	}
	if !strings.Contains(err.Error(), "to mismatch") {
		t.Fatalf("expected envelope mismatch error, got %v", err)
	}
}
