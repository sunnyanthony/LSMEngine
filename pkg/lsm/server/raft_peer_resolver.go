package server

import (
	"context"
	"fmt"
	"strings"
)

// RaftPeerResolver resolves raft peer ids to server endpoints for outbound
// HTTP peer-message delivery.
type RaftPeerResolver interface {
	ResolveRaftPeer(ctx context.Context, peerID uint64) (string, error)
}

// StaticRaftPeerResolver resolves raft peers from a fixed endpoint map.
type StaticRaftPeerResolver struct {
	peerURLs map[uint64]string
}

// NewStaticRaftPeerResolver returns a resolver backed by a copied peer URL map.
func NewStaticRaftPeerResolver(peerURLs map[uint64]string) (*StaticRaftPeerResolver, error) {
	if len(peerURLs) == 0 {
		return nil, fmt.Errorf("raft peer urls required")
	}
	resolved := make(map[uint64]string, len(peerURLs))
	for id, rawURL := range peerURLs {
		endpoint := strings.TrimSuffix(strings.TrimSpace(rawURL), "/")
		if id == 0 || endpoint == "" {
			return nil, fmt.Errorf("invalid raft peer url mapping")
		}
		resolved[id] = endpoint
	}
	return &StaticRaftPeerResolver{peerURLs: resolved}, nil
}

// ResolveRaftPeer returns the configured endpoint for peerID.
func (r *StaticRaftPeerResolver) ResolveRaftPeer(_ context.Context, peerID uint64) (string, error) {
	if r == nil {
		return "", fmt.Errorf("raft peer resolver is unavailable")
	}
	endpoint, ok := r.peerURLs[peerID]
	if !ok {
		return "", fmt.Errorf("raft peer url not configured for node id %d", peerID)
	}
	return endpoint, nil
}
