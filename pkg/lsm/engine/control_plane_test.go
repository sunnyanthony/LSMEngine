package engine

import (
	"errors"
	"testing"

	"lsmengine/pkg/lsm/errs"
)

func TestControlPlaneDefaults(t *testing.T) {
	store, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer store.Close()

	status := store.ClusterStatus()
	if status.NodeID != "node-0" {
		t.Fatalf("expected default node id, got %q", status.NodeID)
	}
	if status.ClusterID != "cluster-local" {
		t.Fatalf("expected default cluster id, got %q", status.ClusterID)
	}
	if status.StorageMode != StorageModeLocal {
		t.Fatalf("expected local storage mode, got %q", status.StorageMode)
	}
	if status.ShardCount != 1 {
		t.Fatalf("expected one default shard, got %d", status.ShardCount)
	}
}

func TestControlPlaneRejectsUnknownStorageMode(t *testing.T) {
	_, err := New(Options{
		DataDir:     t.TempDir(),
		StorageMode: "unknown",
	})
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestWriteDeniedWhenNotLeader(t *testing.T) {
	store, err := New(Options{
		DataDir:   t.TempDir(),
		NodeID:    "node-a",
		ClusterID: "cluster-dev",
		ShardMap: []ShardConfig{
			{
				ID:       "users",
				StartKey: []byte("a"),
				EndKey:   []byte("z"),
				Replicas: []string{"node-a", "node-b"},
				Leader:   "node-b",
			},
		},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer store.Close()

	err = store.Put([]byte("c"), []byte("1"))
	if !errors.Is(err, errs.ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func TestTransferLeaderEnablesWrite(t *testing.T) {
	store, err := New(Options{
		DataDir:   t.TempDir(),
		NodeID:    "node-a",
		ClusterID: "cluster-dev",
		ShardMap: []ShardConfig{
			{
				ID:       "users",
				StartKey: []byte("a"),
				EndKey:   []byte("z"),
				Replicas: []string{"node-a", "node-b"},
				Leader:   "node-b",
			},
		},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer store.Close()

	if err := store.TransferLeader("users", "node-a"); err != nil {
		t.Fatalf("transfer leader: %v", err)
	}
	if err := store.Put([]byte("c"), []byte("1")); err != nil {
		t.Fatalf("put after transfer: %v", err)
	}
}

func TestTriggerSplitAndDrain(t *testing.T) {
	store, err := New(Options{
		DataDir:   t.TempDir(),
		NodeID:    "node-a",
		ClusterID: "cluster-dev",
		ShardMap: []ShardConfig{
			{
				ID:       "users",
				StartKey: []byte("a"),
				EndKey:   []byte("z"),
				Replicas: []string{"node-a", "node-b"},
				Leader:   "node-a",
			},
		},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer store.Close()

	if err := store.TriggerSplit("users", []byte("m")); err != nil {
		t.Fatalf("split: %v", err)
	}
	shards := store.Shards()
	if len(shards) != 2 {
		t.Fatalf("expected 2 shards after split, got %d", len(shards))
	}
	if err := store.PrepareDrain("node-a"); err != nil {
		t.Fatalf("drain: %v", err)
	}
	status := store.ClusterStatus()
	if !status.Draining {
		t.Fatalf("expected draining=true")
	}
	for _, shard := range store.Shards() {
		if shard.Leader == "node-a" {
			t.Fatalf("expected leader moved away from node-a for shard %q", shard.ID)
		}
	}
}

func TestMissingShardErrors(t *testing.T) {
	store, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer store.Close()
	if err := store.TransferLeader("missing", "node-a"); !errors.Is(err, errs.ErrShardNotFound) {
		t.Fatalf("expected ErrShardNotFound, got %v", err)
	}
}
