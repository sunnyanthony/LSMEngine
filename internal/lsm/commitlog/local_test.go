package commitlog

import (
	"context"
	"testing"
)

func TestLocalConsensusHandlePeerMessagesNoop(t *testing.T) {
	consensus := newLocalConsensus()
	if err := consensus.HandlePeerMessages(context.Background(), []PeerMessage{
		{From: 1, To: 1, Type: "ignored", Payload: []byte("ignored")},
	}); err != nil {
		t.Fatalf("handle peer messages: %v", err)
	}
	status := consensus.RuntimeStatus()
	if status.Index != 0 {
		t.Fatalf("expected noop ingress to keep index at 0, got %d", status.Index)
	}
	if !status.WriteAvailable || !status.LeaderKnown || status.Health != "ready" {
		t.Fatalf("expected local runtime ready, got %+v", status)
	}
}
