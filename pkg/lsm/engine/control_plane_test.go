package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"lsmengine/internal/lsm/iofs"
	"lsmengine/pkg/lsm/errs"
)

var errInjectedControlStateWrite = errors.New("injected control state write failure")

type failingControlStateFS struct {
	iofs.OSFS
	failWrite bool
}

func (f *failingControlStateFS) WriteFile(path string, data []byte, perm os.FileMode) error {
	if f.failWrite && strings.HasSuffix(path, "control_state.json.tmp") {
		return errInjectedControlStateWrite
	}
	return f.OSFS.WriteFile(path, data, perm)
}

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
	if status.CommitLog != string(CommitLogProviderLocal) {
		t.Fatalf("expected local commit log provider, got %q", status.CommitLog)
	}
	if status.CommitLogRuntime.Mode != "local" {
		t.Fatalf("expected local commit log runtime mode, got %q", status.CommitLogRuntime.Mode)
	}
	if status.CommitLogRuntime.Replicas != 1 {
		t.Fatalf("expected local commit log runtime replicas=1, got %d", status.CommitLogRuntime.Replicas)
	}
	if !status.CommitLogRuntime.Leader {
		t.Fatalf("expected local provider to report leader=true")
	}
	if status.CommitLogRuntime.Term == 0 {
		t.Fatalf("expected local provider term > 0")
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

func TestControlPlaneRejectsUnknownCommitLogProvider(t *testing.T) {
	_, err := New(Options{
		DataDir: t.TempDir(),
		CommitLog: &CommitLogOptions{
			Provider: "unknown",
		},
	})
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "unknown commit log provider") {
		t.Fatalf("expected unknown commit log provider error, got %v", err)
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

func TestControlPlaneEtcdRaftAppliesControlMutation(t *testing.T) {
	store, err := New(Options{
		DataDir: t.TempDir(),
		CommitLog: &CommitLogOptions{
			Provider: CommitLogProviderEtcdRaft,
		},
		ShardMap: []ShardConfig{
			{
				ID:       "users",
				StartKey: []byte("a"),
				EndKey:   []byte("z"),
				Replicas: []string{"node-0", "node-1"},
				Leader:   "node-0",
			},
		},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer store.Close()

	if got := store.ClusterStatus().CommitLog; got != string(CommitLogProviderEtcdRaft) {
		t.Fatalf("expected etcd-raft provider, got %q", got)
	}
	if err := store.TransferLeader("users", "node-1"); err != nil {
		t.Fatalf("transfer leader: %v", err)
	}
	shards := store.Shards()
	if len(shards) != 1 {
		t.Fatalf("expected one shard, got %d", len(shards))
	}
	if shards[0].Leader != "node-1" {
		t.Fatalf("expected leader node-1, got %q", shards[0].Leader)
	}
	status := store.ClusterStatus()
	if status.CommitLogRuntime.Mode != "raft_single_node" {
		t.Fatalf("expected raft_single_node commit log runtime mode, got %q", status.CommitLogRuntime.Mode)
	}
	if status.CommitLogRuntime.Replicas != 1 {
		t.Fatalf("expected commit log runtime replicas=1, got %d", status.CommitLogRuntime.Replicas)
	}
	if !status.CommitLogRuntime.Leader {
		t.Fatalf("expected commit log runtime leader=true")
	}
	if status.CommitLogRuntime.Term == 0 {
		t.Fatalf("expected commit log runtime term > 0")
	}
	if status.CommitLogRuntime.Index == 0 {
		t.Fatalf("expected commit log runtime index > 0")
	}
}

func TestDataWriteEtcdRaftAppliesMutation(t *testing.T) {
	store, err := New(Options{
		DataDir: t.TempDir(),
		CommitLog: &CommitLogOptions{
			Provider: CommitLogProviderEtcdRaft,
		},
		ShardMap: []ShardConfig{
			{
				ID:       "users",
				StartKey: []byte("a"),
				EndKey:   []byte("z"),
				Replicas: []string{"node-0", "node-1"},
				Leader:   "node-0",
			},
		},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer store.Close()

	if err := store.Put([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, ok := store.Get([]byte("a"))
	if !ok {
		t.Fatalf("expected key to exist")
	}
	if string(got.Value) != "b" {
		t.Fatalf("expected value b, got %q", string(got.Value))
	}
	status := store.ClusterStatus()
	if status.CommitLogRuntime.Index == 0 {
		t.Fatalf("expected commit log runtime index > 0 after put")
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

func TestControlPlaneReplicaMembershipLifecyclePersists(t *testing.T) {
	dataDir := t.TempDir()
	opts := Options{
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
	store, err := New(opts)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := store.AddReplicaWithOptions("users", "node-c", ControlWriteOptions{OperationID: "add-c"}); err != nil {
		t.Fatalf("add replica: %v", err)
	}
	if err := store.RemoveReplicaWithOptions("users", "node-b", ControlWriteOptions{OperationID: "remove-b"}); err != nil {
		t.Fatalf("remove replica: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	restarted, err := New(opts)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	defer restarted.Close()
	shards := restarted.Shards()
	if len(shards) != 1 {
		t.Fatalf("expected one shard, got %d", len(shards))
	}
	if !hasReplica(shards[0].Replicas, "node-c") {
		t.Fatalf("expected node-c replica after restart: %+v", shards[0].Replicas)
	}
	if hasReplica(shards[0].Replicas, "node-b") {
		t.Fatalf("expected node-b removed after restart: %+v", shards[0].Replicas)
	}
	if got := restarted.ClusterStatus().Revision; got != 2 {
		t.Fatalf("expected revision 2, got %d", got)
	}
}

func TestControlPlaneRemoveLeaderReplicaRejected(t *testing.T) {
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

	if err := store.RemoveReplica("users", "node-a"); err == nil {
		t.Fatalf("expected leader removal error")
	}
	if got := store.ClusterStatus().Revision; got != 0 {
		t.Fatalf("expected revision to remain 0, got %d", got)
	}
}

func TestControlWriteOptionsIdempotencyIsBoundedByRetentionWindow(t *testing.T) {
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

	if err := store.TransferLeaderWithOptions("users", "node-b", ControlWriteOptions{
		OperationID: "op-0",
	}); err != nil {
		t.Fatalf("first transfer: %v", err)
	}

	for i := 1; i <= maxAppliedControlOps; i++ {
		target := "node-a"
		if i%2 == 0 {
			target = "node-b"
		}
		if err := store.TransferLeaderWithOptions("users", target, ControlWriteOptions{
			OperationID: fmt.Sprintf("op-%d", i),
		}); err != nil {
			t.Fatalf("transfer %d: %v", i, err)
		}
	}

	if got := store.ClusterStatus().Revision; got != uint64(maxAppliedControlOps+1) {
		t.Fatalf("expected revision %d after filling retention window, got %d", maxAppliedControlOps+1, got)
	}

	if err := store.TransferLeaderWithOptions("users", "node-b", ControlWriteOptions{
		OperationID: "op-0",
	}); err != nil {
		t.Fatalf("retry after eviction: %v", err)
	}
	if got := store.ClusterStatus().Revision; got != uint64(maxAppliedControlOps+2) {
		t.Fatalf("expected revision %d after retrying evicted operation, got %d", maxAppliedControlOps+2, got)
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

func TestControlMutationRollsBackWhenStateSaveFails(t *testing.T) {
	fs := &failingControlStateFS{}
	store, err := New(Options{
		DataDir:   t.TempDir(),
		NodeID:    "node-a",
		ClusterID: "cluster-dev",
		IOFS:      fs,
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
		OperationID:      "op-fail",
		ExpectedRevision: &revision,
	}
	fs.failWrite = true
	if err := store.TransferLeaderWithOptions("users", "node-c", opts); !errors.Is(err, errInjectedControlStateWrite) {
		t.Fatalf("expected injected write failure, got %v", err)
	}
	if got := store.ClusterStatus().Revision; got != 0 {
		t.Fatalf("expected revision rollback to 0, got %d", got)
	}
	shards := store.Shards()
	if len(shards) != 1 {
		t.Fatalf("expected one shard, got %d", len(shards))
	}
	if shards[0].Leader != "node-a" {
		t.Fatalf("expected leader rollback to node-a, got %q", shards[0].Leader)
	}
	if hasReplica(shards[0].Replicas, "node-c") {
		t.Fatalf("expected rollback to remove implicitly added replica node-c")
	}

	fs.failWrite = false
	if err := store.TransferLeaderWithOptions("users", "node-c", opts); err != nil {
		t.Fatalf("retry after failed save: %v", err)
	}
	if got := store.ClusterStatus().Revision; got != 1 {
		t.Fatalf("expected revision 1 after retry, got %d", got)
	}
	shards = store.Shards()
	if shards[0].Leader != "node-c" {
		t.Fatalf("expected retry to move leader to node-c, got %q", shards[0].Leader)
	}
	if !hasReplica(shards[0].Replicas, "node-c") {
		t.Fatalf("expected retry to add replica node-c")
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
	if err := store.ResumeDrain("node-a"); err != nil {
		t.Fatalf("resume drain: %v", err)
	}
	if status := store.ClusterStatus(); status.Draining {
		t.Fatalf("expected draining=false after resume")
	}
	for _, shard := range store.Shards() {
		if shard.Leader == "node-a" {
			t.Fatalf("expected leader moved away from node-a for shard %q", shard.ID)
		}
	}
}

func TestPrepareDrainDoesNotPartiallyMutateWhenShardCannotMove(t *testing.T) {
	store, err := New(Options{
		DataDir:   t.TempDir(),
		NodeID:    "node-a",
		ClusterID: "cluster-dev",
		ShardMap: []ShardConfig{
			{
				ID:       "users-a-m",
				StartKey: []byte("a"),
				EndKey:   []byte("m"),
				Replicas: []string{"node-a", "node-b"},
				Leader:   "node-a",
			},
			{
				ID:       "users-m-z",
				StartKey: []byte("m"),
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

	err = store.PrepareDrain("node-a")
	if err == nil {
		t.Fatalf("expected drain error")
	}
	if !strings.Contains(err.Error(), "no alternate healthy replica") {
		t.Fatalf("expected alternate replica error, got %v", err)
	}
	if status := store.ClusterStatus(); status.Draining || status.Revision != 0 {
		t.Fatalf("expected no drain/revision mutation, got draining=%v revision=%d", status.Draining, status.Revision)
	}
	for _, shard := range store.Shards() {
		if shard.Leader != "node-a" {
			t.Fatalf("expected shard %q leader to remain node-a, got %q", shard.ID, shard.Leader)
		}
	}
}

func TestPrepareDrainRemoteNodeTransfersLeadershipWithoutLocalDraining(t *testing.T) {
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

	if err := store.PrepareDrain("node-b"); err != nil {
		t.Fatalf("remote drain: %v", err)
	}
	status := store.ClusterStatus()
	if status.Draining {
		t.Fatalf("expected node-a to stay non-draining after remote drain")
	}
	shards := store.Shards()
	if len(shards) != 1 {
		t.Fatalf("expected one shard, got %d", len(shards))
	}
	if shards[0].Leader != "node-a" {
		t.Fatalf("expected leader to move to node-a, got %q", shards[0].Leader)
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
