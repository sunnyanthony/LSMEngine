package engine

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
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

func TestControlPlaneRejectsOverlappingRanges(t *testing.T) {
	_, err := New(Options{
		DataDir: t.TempDir(),
		NodeID:  "node-a",
		ShardMap: []ShardConfig{
			{
				ID:       "s1",
				StartKey: []byte("a"),
				EndKey:   []byte("m"),
				Leader:   "node-a",
			},
			{
				ID:       "s2",
				StartKey: []byte("k"),
				EndKey:   []byte("z"),
				Leader:   "node-a",
			},
		},
	})
	if err == nil {
		t.Fatalf("expected overlap error")
	}
	if !strings.Contains(err.Error(), "overlapping shard ranges") {
		t.Fatalf("expected overlap error, got %v", err)
	}
}

func TestControlPlaneRejectsInvalidRangeBounds(t *testing.T) {
	_, err := New(Options{
		DataDir: t.TempDir(),
		NodeID:  "node-a",
		ShardMap: []ShardConfig{
			{
				ID:       "s1",
				StartKey: []byte("m"),
				EndKey:   []byte("m"),
				Leader:   "node-a",
			},
		},
	})
	if err == nil {
		t.Fatalf("expected invalid range error")
	}
	if !strings.Contains(err.Error(), "start key must be < end key") {
		t.Fatalf("expected invalid range error, got %v", err)
	}
}

func TestControlPlaneRejectsOpenEndedRangeBeforeLast(t *testing.T) {
	_, err := New(Options{
		DataDir: t.TempDir(),
		NodeID:  "node-a",
		ShardMap: []ShardConfig{
			{
				ID:       "s1",
				StartKey: []byte("a"),
				EndKey:   nil,
				Leader:   "node-a",
			},
			{
				ID:       "s2",
				StartKey: []byte("m"),
				EndKey:   []byte("z"),
				Leader:   "node-a",
			},
		},
	})
	if err == nil {
		t.Fatalf("expected open-ended layout error")
	}
	if !strings.Contains(err.Error(), "open-ended shard") {
		t.Fatalf("expected open-ended layout error, got %v", err)
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

func TestWriteWithGapReturnsShardNotFound(t *testing.T) {
	store, err := New(Options{
		DataDir:   t.TempDir(),
		NodeID:    "node-a",
		ClusterID: "cluster-dev",
		ShardMap: []ShardConfig{
			{
				ID:       "users-a-m",
				StartKey: []byte("a"),
				EndKey:   []byte("m"),
				Replicas: []string{"node-a"},
				Leader:   "node-a",
			},
			{
				ID:       "users-t-z",
				StartKey: []byte("t"),
				EndKey:   []byte("z"),
				Replicas: []string{"node-a"},
				Leader:   "node-a",
			},
		},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer store.Close()

	err = store.Put([]byte("q"), []byte("1"))
	if !errors.Is(err, errs.ErrShardNotFound) {
		t.Fatalf("expected ErrShardNotFound, got %v", err)
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

func TestControlWriteOptionsRevisionConflict(t *testing.T) {
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

	revision := uint64(0)
	if err := store.TransferLeaderWithOptions("users", "node-b", ControlWriteOptions{
		OperationID:      "op-1",
		ExpectedRevision: &revision,
	}); err != nil {
		t.Fatalf("first transfer: %v", err)
	}
	if got := store.ClusterStatus().Revision; got != 1 {
		t.Fatalf("expected revision 1, got %d", got)
	}
	if err := store.TransferLeaderWithOptions("users", "node-a", ControlWriteOptions{
		OperationID:      "op-2",
		ExpectedRevision: &revision,
	}); !errors.Is(err, errs.ErrControlRevisionConflict) {
		t.Fatalf("expected revision conflict, got %v", err)
	}
}

func TestControlWriteOptionsIdempotentRetry(t *testing.T) {
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

	revision := uint64(0)
	opts := ControlWriteOptions{
		OperationID:      "op-1",
		ExpectedRevision: &revision,
	}
	if err := store.TransferLeaderWithOptions("users", "node-b", opts); err != nil {
		t.Fatalf("first transfer: %v", err)
	}
	if err := store.TransferLeaderWithOptions("users", "node-b", opts); err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
	if got := store.ClusterStatus().Revision; got != 1 {
		t.Fatalf("expected revision 1 after idempotent retry, got %d", got)
	}
}

func TestControlWriteOptionsOperationConflict(t *testing.T) {
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

	revision := uint64(0)
	if err := store.TransferLeaderWithOptions("users", "node-b", ControlWriteOptions{
		OperationID:      "op-1",
		ExpectedRevision: &revision,
	}); err != nil {
		t.Fatalf("first transfer: %v", err)
	}
	if err := store.TransferLeaderWithOptions("users", "node-a", ControlWriteOptions{
		OperationID: "op-1",
	}); !errors.Is(err, errs.ErrControlOperationConflict) {
		t.Fatalf("expected operation conflict, got %v", err)
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

func TestControlPlaneStatePersistsAcrossRestart(t *testing.T) {
	dataDir := t.TempDir()
	initialOpts := Options{
		DataDir:   dataDir,
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
	}

	store, err := New(initialOpts)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := store.TriggerSplit("users", []byte("m")); err != nil {
		t.Fatalf("split: %v", err)
	}
	if err := store.TransferLeader("users-a", "node-b"); err != nil {
		t.Fatalf("transfer leader: %v", err)
	}
	if err := store.PrepareDrain("node-a"); err != nil {
		t.Fatalf("prepare drain: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	restartOpts := Options{
		DataDir:   dataDir,
		NodeID:    "node-a",
		ClusterID: "cluster-dev",
	}
	restarted, err := New(restartOpts)
	if err != nil {
		t.Fatalf("re-open: %v", err)
	}
	defer restarted.Close()

	status := restarted.ClusterStatus()
	if !status.Draining {
		t.Fatalf("expected draining=true after restart")
	}
	if status.Revision == 0 {
		t.Fatalf("expected revision to persist across restart")
	}
	shards := restarted.Shards()
	if len(shards) != 2 {
		t.Fatalf("expected 2 shards after restart, got %d", len(shards))
	}
	for _, shard := range shards {
		if shard.Leader == "node-a" {
			t.Fatalf("expected leaders moved from node-a, shard=%q", shard.ID)
		}
	}
	if err := restarted.TransferLeaderWithOptions("users-a", "node-b", ControlWriteOptions{
		OperationID: "op-retry",
	}); err != nil {
		t.Fatalf("seed operation id: %v", err)
	}
	if err := restarted.TransferLeaderWithOptions("users-a", "node-b", ControlWriteOptions{
		OperationID: "op-retry",
	}); err != nil {
		t.Fatalf("expected idempotent retry after restart, got %v", err)
	}
}

func TestControlPlaneRejectsCorruptStateFile(t *testing.T) {
	dataDir := t.TempDir()
	statePath := filepath.Join(dataDir, "control_state.json")
	if err := os.WriteFile(statePath, []byte("{invalid"), 0o644); err != nil {
		t.Fatalf("write state: %v", err)
	}
	_, err := New(Options{
		DataDir:   dataDir,
		NodeID:    "node-a",
		ClusterID: "cluster-dev",
	})
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "load control state") {
		t.Fatalf("expected load control state error, got %v", err)
	}
}

func TestControlPlaneRejectsStateWithoutShards(t *testing.T) {
	dataDir := t.TempDir()
	statePath := filepath.Join(dataDir, "control_state.json")
	payload := `{"version":1,"node_id":"node-a","cluster_id":"cluster-dev","order":["users"],"shards":[]}`
	if err := os.WriteFile(statePath, []byte(payload), 0o644); err != nil {
		t.Fatalf("write state: %v", err)
	}
	_, err := New(Options{
		DataDir:   dataDir,
		NodeID:    "node-a",
		ClusterID: "cluster-dev",
	})
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "state contains no shards") {
		t.Fatalf("expected no shards error, got %v", err)
	}
}

func TestControlPlaneRejectsStateWithoutOrder(t *testing.T) {
	dataDir := t.TempDir()
	statePath := filepath.Join(dataDir, "control_state.json")
	payload := `{"version":1,"node_id":"node-a","cluster_id":"cluster-dev","shards":[{"id":"users","leader":"node-a","replicas":[{"node_id":"node-a","role":"leader","healthy":true}]}]}`
	if err := os.WriteFile(statePath, []byte(payload), 0o644); err != nil {
		t.Fatalf("write state: %v", err)
	}
	_, err := New(Options{
		DataDir:   dataDir,
		NodeID:    "node-a",
		ClusterID: "cluster-dev",
	})
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "state order is required") {
		t.Fatalf("expected missing order error, got %v", err)
	}
}

func TestControlPlaneRejectsStateIdentityMismatch(t *testing.T) {
	dataDir := t.TempDir()
	store, err := New(Options{
		DataDir:   dataDir,
		NodeID:    "node-a",
		ClusterID: "cluster-dev",
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	_, err = New(Options{
		DataDir:   dataDir,
		NodeID:    "node-b",
		ClusterID: "cluster-dev",
	})
	if err == nil {
		t.Fatalf("expected identity mismatch")
	}
	if !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatalf("expected identity mismatch error, got %v", err)
	}
}
