package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"

	"lsmengine/pkg/lsm"
)

// RaftPeerURLFileResolverOptions configures a file-backed raft peer resolver.
type RaftPeerURLFileResolverOptions struct {
	Path             string
	FallbackPeerURLs map[uint64]string
}

// RaftPeerURLFileResolver resolves peers from a YAML/JSON node-to-URL file.
// The file is reloaded on each resolve so operators can add replacement or join
// endpoints without restarting the process.
type RaftPeerURLFileResolver struct {
	path     string
	fallback map[uint64]string

	mu       sync.Mutex
	lastGood map[uint64]string
}

// NewRaftPeerURLFileResolver returns a resolver backed by an absolute endpoint
// file. Fallback endpoints are copied and used when the file does not contain a
// requested peer.
func NewRaftPeerURLFileResolver(opts RaftPeerURLFileResolverOptions) (*RaftPeerURLFileResolver, error) {
	path := strings.TrimSpace(opts.Path)
	if path == "" {
		return nil, fmt.Errorf("raft peer url file required")
	}
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("raft peer url file must be an absolute path")
	}
	fallback, err := normalizeRaftPeerURLMap(opts.FallbackPeerURLs)
	if err != nil && len(opts.FallbackPeerURLs) > 0 {
		return nil, err
	}
	return &RaftPeerURLFileResolver{
		path:     path,
		fallback: fallback,
	}, nil
}

// ResolveRaftPeer returns the endpoint for peerID from the latest valid file,
// falling back to configured static endpoints when needed.
func (r *RaftPeerURLFileResolver) ResolveRaftPeer(ctx context.Context, peerID uint64) (string, error) {
	if r == nil {
		return "", fmt.Errorf("raft peer resolver is unavailable")
	}
	if peerID == 0 {
		return "", fmt.Errorf("raft peer message missing target")
	}
	endpoints, loadErr := r.load(ctx)
	if endpoint, ok := endpoints[peerID]; ok {
		return endpoint, nil
	}
	if endpoint, ok := r.fallback[peerID]; ok {
		return endpoint, nil
	}
	if loadErr != nil {
		return "", loadErr
	}
	return "", fmt.Errorf("raft peer url not configured for node id %d", peerID)
}

func (r *RaftPeerURLFileResolver) load(ctx context.Context) (map[uint64]string, error) {
	if err := ctx.Err(); err != nil {
		return r.cached(), err
	}
	data, err := os.ReadFile(r.path)
	if err != nil {
		return r.cached(), fmt.Errorf("read raft peer url file: %w", err)
	}
	loaded, err := parseRaftPeerURLFile(data)
	if err != nil {
		return r.cached(), err
	}
	r.mu.Lock()
	r.lastGood = loaded
	r.mu.Unlock()
	return loaded, nil
}

func (r *RaftPeerURLFileResolver) cached() map[uint64]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return clonePeerURLMap(r.lastGood)
}

func parseRaftPeerURLFile(data []byte) (map[uint64]string, error) {
	var byNode map[string]string
	if err := yaml.Unmarshal(data, &byNode); err != nil {
		return nil, fmt.Errorf("parse raft peer url file: %w", err)
	}
	byPeerID := make(map[uint64]string, len(byNode))
	for nodeID, endpoint := range byNode {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" {
			return nil, fmt.Errorf("raft peer url file contains empty node id")
		}
		byPeerID[lsm.RaftPeerID(nodeID)] = endpoint
	}
	return normalizeRaftPeerURLMap(byPeerID)
}
