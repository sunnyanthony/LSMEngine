package config

import (
	"net"
	"path/filepath"
	"reflect"
	"testing"
)

func TestThreeNodeExampleTopology(t *testing.T) {
	names := []string{"node-a", "node-b", "node-c"}
	configs := make(map[string]Config)
	addresses, directories := map[string]bool{}, map[string]bool{}
	for _, name := range names {
		cfg, err := Load(filepath.Join("../../../../examples/three-node", name+".yaml"))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.NodeID != name || cfg.ClusterID != "three-node-demo" || cfg.CommitLog.Provider != "etcd-raft" || cfg.WriteConsistencyDefault != "local_committed" {
			t.Fatalf("invalid identity or provider: %+v", cfg)
		}
		if !reflect.DeepEqual(cfg.Raft.Peers, names) {
			t.Fatalf("%s peers=%v", name, cfg.Raft.Peers)
		}
		host, _, err := net.SplitHostPort(cfg.Addr)
		if err != nil || host != "127.0.0.1" || addresses[cfg.Addr] {
			t.Fatalf("invalid/duplicate loopback address: %s", cfg.Addr)
		}
		addresses[cfg.Addr] = true
		wantDir := filepath.Join("data", "three-node", name)
		if filepath.Clean(cfg.DataDir) != wantDir || directories[filepath.Clean(cfg.DataDir)] || cfg.ControlStatePath != "" {
			t.Fatalf("invalid storage isolation for %s", name)
		}
		directories[filepath.Clean(cfg.DataDir)] = true
		if len(cfg.Shards) != 1 || cfg.Shards[0].ID != "shared" || cfg.Shards[0].StartKey != "" || cfg.Shards[0].EndKey != "" || !reflect.DeepEqual(cfg.Shards[0].Replicas, names) {
			t.Fatalf("invalid shard: %+v", cfg.Shards)
		}
		configs[name] = cfg
	}
	for name, cfg := range configs {
		if len(cfg.Raft.PeerURLs) != len(names) {
			t.Fatalf("%s peer URLs: %v", name, cfg.Raft.PeerURLs)
		}
		for _, peer := range names {
			if cfg.Raft.PeerURLs[peer] != "http://"+configs[peer].Addr {
				t.Fatalf("%s peer %s URL mismatch", name, peer)
			}
		}
	}
}
