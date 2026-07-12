package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"lsmengine/pkg/lsm"
)

func TestRaftPeerURLFileResolverReloadsEndpointFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.yaml")
	peerID := lsm.RaftPeerID("node-b")
	if err := os.WriteFile(path, []byte(`node-b: "http://127.0.0.1:9091/"`), 0o644); err != nil {
		t.Fatalf("write peers: %v", err)
	}
	resolver, err := NewRaftPeerURLFileResolver(RaftPeerURLFileResolverOptions{Path: path})
	if err != nil {
		t.Fatalf("new file resolver: %v", err)
	}
	endpoint, err := resolver.ResolveRaftPeer(context.Background(), peerID)
	if err != nil {
		t.Fatalf("resolve first endpoint: %v", err)
	}
	if endpoint != "http://127.0.0.1:9091" {
		t.Fatalf("unexpected first endpoint %q", endpoint)
	}

	if err := os.WriteFile(path, []byte(`node-b: "http://127.0.0.1:9191"`), 0o644); err != nil {
		t.Fatalf("rewrite peers: %v", err)
	}
	endpoint, err = resolver.ResolveRaftPeer(context.Background(), peerID)
	if err != nil {
		t.Fatalf("resolve reloaded endpoint: %v", err)
	}
	if endpoint != "http://127.0.0.1:9191" {
		t.Fatalf("unexpected reloaded endpoint %q", endpoint)
	}
}

func TestRaftPeerURLFileResolverUsesFallbackForMissingPeer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.yaml")
	if err := os.WriteFile(path, []byte(`node-c: "http://127.0.0.1:9092"`), 0o644); err != nil {
		t.Fatalf("write peers: %v", err)
	}
	peerID := lsm.RaftPeerID("node-b")
	resolver, err := NewRaftPeerURLFileResolver(RaftPeerURLFileResolverOptions{
		Path: path,
		FallbackPeerURLs: map[uint64]string{
			peerID: "http://127.0.0.1:9091/",
		},
	})
	if err != nil {
		t.Fatalf("new file resolver: %v", err)
	}
	endpoint, err := resolver.ResolveRaftPeer(context.Background(), peerID)
	if err != nil {
		t.Fatalf("resolve fallback endpoint: %v", err)
	}
	if endpoint != "http://127.0.0.1:9091" {
		t.Fatalf("unexpected fallback endpoint %q", endpoint)
	}
}

func TestRaftPeerURLFileResolverRequiresAbsolutePath(t *testing.T) {
	_, err := NewRaftPeerURLFileResolver(RaftPeerURLFileResolverOptions{Path: "peers.yaml"})
	if err == nil {
		t.Fatalf("expected absolute path error")
	}
}

func TestRaftPeerURLFileResolverKeepsLastGoodFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.yaml")
	peerID := lsm.RaftPeerID("node-b")
	if err := os.WriteFile(path, []byte(`node-b: "http://127.0.0.1:9091"`), 0o644); err != nil {
		t.Fatalf("write peers: %v", err)
	}
	resolver, err := NewRaftPeerURLFileResolver(RaftPeerURLFileResolverOptions{Path: path})
	if err != nil {
		t.Fatalf("new file resolver: %v", err)
	}
	if _, err := resolver.ResolveRaftPeer(context.Background(), peerID); err != nil {
		t.Fatalf("resolve first endpoint: %v", err)
	}
	if err := os.WriteFile(path, []byte(`: bad`), 0o644); err != nil {
		t.Fatalf("write invalid peers: %v", err)
	}
	endpoint, err := resolver.ResolveRaftPeer(context.Background(), peerID)
	if err != nil {
		t.Fatalf("expected last good endpoint after invalid file, got %v", err)
	}
	if endpoint != "http://127.0.0.1:9091" {
		t.Fatalf("unexpected cached endpoint %q", endpoint)
	}
}
