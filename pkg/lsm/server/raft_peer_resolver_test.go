package server

import (
	"context"
	"testing"

	"lsmengine/pkg/lsm"
)

func TestStaticRaftPeerResolverCopiesAndNormalizesPeerURLs(t *testing.T) {
	peerID := lsm.RaftPeerID("node-b")
	peerURLs := map[uint64]string{
		peerID: " http://127.0.0.1:9091/ ",
	}
	resolver, err := NewStaticRaftPeerResolver(peerURLs)
	if err != nil {
		t.Fatalf("new static raft peer resolver: %v", err)
	}
	peerURLs[peerID] = "http://127.0.0.1:9999"

	endpoint, err := resolver.ResolveRaftPeer(context.Background(), peerID)
	if err != nil {
		t.Fatalf("resolve raft peer: %v", err)
	}
	if endpoint != "http://127.0.0.1:9091" {
		t.Fatalf("expected normalized copied endpoint, got %q", endpoint)
	}
}

func TestStaticRaftPeerResolverRejectsInvalidMappings(t *testing.T) {
	tests := []struct {
		name     string
		peerURLs map[uint64]string
	}{
		{name: "empty", peerURLs: nil},
		{name: "zero peer", peerURLs: map[uint64]string{0: "http://127.0.0.1:9091"}},
		{name: "empty endpoint", peerURLs: map[uint64]string{lsm.RaftPeerID("node-b"): " "}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewStaticRaftPeerResolver(tt.peerURLs); err == nil {
				t.Fatalf("expected invalid mapping error")
			}
		})
	}
}

func TestStaticRaftPeerResolverRejectsUnknownPeer(t *testing.T) {
	resolver, err := NewStaticRaftPeerResolver(map[uint64]string{
		lsm.RaftPeerID("node-b"): "http://127.0.0.1:9091",
	})
	if err != nil {
		t.Fatalf("new static raft peer resolver: %v", err)
	}
	if _, err := resolver.ResolveRaftPeer(context.Background(), lsm.RaftPeerID("node-c")); err == nil {
		t.Fatalf("expected unknown peer error")
	}
}
