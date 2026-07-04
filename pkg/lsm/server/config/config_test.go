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
raft:
  replicas: 3
  election_timeout: "2s"
  heartbeat_interval: "500ms"
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
	if cfg.Raft.Replicas != 3 {
		t.Fatalf("expected raft replicas, got %d", cfg.Raft.Replicas)
	}
	if cfg.Raft.ElectionTimeout != 2*time.Second {
		t.Fatalf("expected raft election timeout, got %v", cfg.Raft.ElectionTimeout)
	}
	if cfg.Raft.HeartbeatInterval != 500*time.Millisecond {
		t.Fatalf("expected raft heartbeat interval, got %v", cfg.Raft.HeartbeatInterval)
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
	if cfg.IOBackend != "async" {
		t.Fatalf("expected io backend, got %q", cfg.IOBackend)
	}
	if !cfg.IOBackendStrict {
		t.Fatalf("expected io backend strict to be true")
	}
	if cfg.IOAsyncMaxInFlight != 8 {
		t.Fatalf("expected io async max in flight, got %d", cfg.IOAsyncMaxInFlight)
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
