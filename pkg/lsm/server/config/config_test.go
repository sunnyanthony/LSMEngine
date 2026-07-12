package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	path := writeConfig(t, `
data_dir: "./data"
node_id: "node-a"
cluster_id: "dev-cluster"
storage_mode: "pvc"
control_state_path: "/var/lib/lsm/control_state.json"
commitlog:
  provider: "local"
  snapshot_policy:
    applied_entries: 1024
    retain_entries: 128
raft:
  replicas: 3
  election_timeout: "2s"
  heartbeat_interval: "500ms"
  join: true
  peers: ["node-a", "node-b", "node-c"]
  peer_urls:
    node-b: "http://127.0.0.1:9091"
    node-c: "http://127.0.0.1:9092"
  join_peer_urls:
    node-d: "http://127.0.0.1:9093"
  peer_url_file: "/var/lib/lsm/raft-peers.yaml"
shards:
  - id: "users-a-m"
    start_key: "a"
    end_key: "m"
    replicas: ["node-a", "node-b", "node-c"]
    leader: "node-a"
  - id: "users-m-z"
    start_key: "m"
    end_key: ""
    replicas: ["node-a", "node-b", "node-c"]
    leader: "node-b"
addr: "127.0.0.1:9090"
read_timeout: "2s"
write_timeout: "3s"
write_consistency_default: "accepted"
io_backend: "async"
io_backend_strict: true
io_async_max_in_flight: 8
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.DataDir != "./data" {
		t.Fatalf("expected data dir, got %q", cfg.DataDir)
	}
	if cfg.NodeID != "node-a" {
		t.Fatalf("expected node id, got %q", cfg.NodeID)
	}
	if cfg.ClusterID != "dev-cluster" {
		t.Fatalf("expected cluster id, got %q", cfg.ClusterID)
	}
	if cfg.StorageMode != "pvc" {
		t.Fatalf("expected storage mode, got %q", cfg.StorageMode)
	}
	if cfg.ControlStatePath != "/var/lib/lsm/control_state.json" {
		t.Fatalf("expected control state path, got %q", cfg.ControlStatePath)
	}
	if cfg.CommitLog.Provider != "local" {
		t.Fatalf("expected commit log provider, got %q", cfg.CommitLog.Provider)
	}
	if cfg.CommitLog.SnapshotPolicy.AppliedEntries != 1024 {
		t.Fatalf("expected commit log snapshot applied entries, got %d", cfg.CommitLog.SnapshotPolicy.AppliedEntries)
	}
	if cfg.CommitLog.SnapshotPolicy.RetainEntries != 128 {
		t.Fatalf("expected commit log snapshot retain entries, got %d", cfg.CommitLog.SnapshotPolicy.RetainEntries)
	}
	if cfg.Raft.Replicas != 3 {
		t.Fatalf("expected raft replicas, got %d", cfg.Raft.Replicas)
	}
	if cfg.Raft.ElectionTimeout != 2*time.Second {
		t.Fatalf("expected raft election timeout, got %v", cfg.Raft.ElectionTimeout)
	}
	if cfg.Raft.HeartbeatInterval != 500*time.Millisecond {
		t.Fatalf("expected raft heartbeat interval, got %v", cfg.Raft.HeartbeatInterval)
	}
	if !cfg.Raft.Join {
		t.Fatalf("expected raft join mode")
	}
	if len(cfg.Raft.Peers) != 3 {
		t.Fatalf("expected raft peers, got %d", len(cfg.Raft.Peers))
	}
	if cfg.Raft.Peers[1] != "node-b" {
		t.Fatalf("expected raft peer node-b, got %q", cfg.Raft.Peers[1])
	}
	if cfg.Raft.PeerURLs["node-c"] != "http://127.0.0.1:9092" {
		t.Fatalf("expected node-c peer url, got %q", cfg.Raft.PeerURLs["node-c"])
	}
	if cfg.Raft.JoinPeerURLs["node-d"] != "http://127.0.0.1:9093" {
		t.Fatalf("expected node-d join peer url, got %q", cfg.Raft.JoinPeerURLs["node-d"])
	}
	if cfg.Raft.PeerURLFile != "/var/lib/lsm/raft-peers.yaml" {
		t.Fatalf("expected raft peer url file, got %q", cfg.Raft.PeerURLFile)
	}
	if len(cfg.Shards) != 2 {
		t.Fatalf("expected shard configs, got %d", len(cfg.Shards))
	}
	if cfg.Addr != "127.0.0.1:9090" {
		t.Fatalf("expected addr, got %q", cfg.Addr)
	}
	if cfg.ReadTimeout != 2*time.Second {
		t.Fatalf("expected read timeout, got %v", cfg.ReadTimeout)
	}
	if cfg.WriteTimeout != 3*time.Second {
		t.Fatalf("expected write timeout, got %v", cfg.WriteTimeout)
	}
	if cfg.WriteConsistencyDefault != "accepted" {
		t.Fatalf("expected write consistency default, got %q", cfg.WriteConsistencyDefault)
	}
	if cfg.IOBackend != "async" {
		t.Fatalf("expected io backend, got %q", cfg.IOBackend)
	}
	if !cfg.IOBackendStrict {
		t.Fatalf("expected io backend strict to be true")
	}
	if cfg.IOAsyncMaxInFlight != 8 {
		t.Fatalf("expected io async max in flight, got %d", cfg.IOAsyncMaxInFlight)
	}
	if err := Validate(cfg); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func TestValidateEtcdRaftRequiresLocalPeer(t *testing.T) {
	cfg := Config{
		NodeID: "node-a",
		CommitLog: CommitLogConfig{
			Provider: "etcd-raft",
		},
		Raft: RaftConfig{
			Peers: []string{"node-b", "node-c"},
			PeerURLs: map[string]string{
				"node-b": "http://127.0.0.1:8081",
				"node-c": "http://127.0.0.1:8082",
			},
		},
	}
	if err := Validate(cfg); err == nil {
		t.Fatalf("expected local peer validation error")
	}
}

func TestValidateEtcdRaftRequiresPeerURLs(t *testing.T) {
	cfg := Config{
		NodeID: "node-a",
		CommitLog: CommitLogConfig{
			Provider: "etcd-raft",
		},
		Raft: RaftConfig{
			Peers: []string{"node-a", "node-b", "node-c"},
			PeerURLs: map[string]string{
				"node-a": "http://127.0.0.1:8080",
				"node-b": "http://127.0.0.1:8081",
			},
		},
	}
	if err := Validate(cfg); err == nil {
		t.Fatalf("expected missing peer url validation error")
	}
}

func TestValidateEtcdRaftAllowsPeerURLFile(t *testing.T) {
	cfg := Config{
		NodeID: "node-a",
		CommitLog: CommitLogConfig{
			Provider: "etcd-raft",
		},
		Raft: RaftConfig{
			Peers:       []string{"node-a", "node-b", "node-c"},
			PeerURLFile: "/var/lib/lsm/raft-peers.yaml",
		},
	}
	if err := Validate(cfg); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func TestValidateEtcdRaftRejectsRelativePeerURLFile(t *testing.T) {
	cfg := Config{
		NodeID: "node-a",
		CommitLog: CommitLogConfig{
			Provider: "etcd-raft",
		},
		Raft: RaftConfig{
			Peers:       []string{"node-a", "node-b", "node-c"},
			PeerURLFile: "raft-peers.yaml",
		},
	}
	if err := Validate(cfg); err == nil {
		t.Fatalf("expected relative peer url file validation error")
	}
}

func TestLoadPeerURLFile(t *testing.T) {
	path := writeConfig(t, `
node-a: "http://127.0.0.1:8080/"
node-b: "http://127.0.0.1:8081"
`)
	got, err := LoadPeerURLFile(path)
	if err != nil {
		t.Fatalf("load peer url file: %v", err)
	}
	if got["node-a"] != "http://127.0.0.1:8080/" {
		t.Fatalf("expected node-a url, got %+v", got)
	}
	if got["node-b"] != "http://127.0.0.1:8081" {
		t.Fatalf("expected node-b url, got %+v", got)
	}
}

func TestLoadPeerURLFileRejectsInvalidURL(t *testing.T) {
	path := writeConfig(t, `node-a: "127.0.0.1:8080"`)
	if _, err := LoadPeerURLFile(path); err == nil {
		t.Fatalf("expected invalid peer url error")
	}
}

func TestValidateEtcdRaftRejectsUnknownPeerURL(t *testing.T) {
	cfg := Config{
		NodeID: "node-a",
		CommitLog: CommitLogConfig{
			Provider: "etcd-raft",
		},
		Raft: RaftConfig{
			Peers: []string{"node-a", "node-b"},
			PeerURLs: map[string]string{
				"node-a": "http://127.0.0.1:8080",
				"node-b": "http://127.0.0.1:8081",
				"node-c": "http://127.0.0.1:8082",
			},
		},
	}
	if err := Validate(cfg); err == nil {
		t.Fatalf("expected unknown peer url validation error")
	}
}

func TestValidateEtcdRaftRejectsDuplicatePeer(t *testing.T) {
	cfg := Config{
		NodeID: "node-a",
		CommitLog: CommitLogConfig{
			Provider: "etcd-raft",
		},
		Raft: RaftConfig{
			Peers: []string{"node-a", "node-a"},
		},
	}
	if err := Validate(cfg); err == nil {
		t.Fatalf("expected duplicate peer validation error")
	}
}

func TestValidateEtcdRaftAllowsJoinPeerURLs(t *testing.T) {
	cfg := Config{
		NodeID: "node-a",
		CommitLog: CommitLogConfig{
			Provider: "etcd-raft",
		},
		Raft: RaftConfig{
			Peers: []string{"node-a", "node-b"},
			PeerURLs: map[string]string{
				"node-a": "http://127.0.0.1:8080",
				"node-b": "http://127.0.0.1:8081",
			},
			JoinPeerURLs: map[string]string{
				"node-c": "http://127.0.0.1:8082",
			},
		},
	}
	if err := Validate(cfg); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func TestValidateEtcdRaftRejectsJoinPeerURLForExistingPeer(t *testing.T) {
	cfg := Config{
		NodeID: "node-a",
		CommitLog: CommitLogConfig{
			Provider: "etcd-raft",
		},
		Raft: RaftConfig{
			Peers: []string{"node-a", "node-b"},
			PeerURLs: map[string]string{
				"node-a": "http://127.0.0.1:8080",
				"node-b": "http://127.0.0.1:8081",
			},
			JoinPeerURLs: map[string]string{
				"node-b": "http://127.0.0.1:8081",
			},
		},
	}
	if err := Validate(cfg); err == nil {
		t.Fatalf("expected join peer existing peer validation error")
	}
}

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := t.TempDir() + "/cfg.yaml"
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
