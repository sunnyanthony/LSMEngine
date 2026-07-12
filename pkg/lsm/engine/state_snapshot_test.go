package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestStateSnapshotRestoresVisibleDataAndControlToEmptyEngine(t *testing.T) {
	opts := func(dir string) Options {
		return Options{
			DataDir:               dir,
			NodeID:                "node-a",
			ClusterID:             "cluster-dev",
			CompactionL0Threshold: 0,
			ShardMap: []ShardConfig{
				{
					ID:       "users",
					StartKey: []byte("a"),
					EndKey:   []byte("z"),
					Replicas: []string{"node-a"},
					Leader:   "node-a",
				},
			},
		}
	}

	source, err := New(opts(t.TempDir()))
	if err != nil {
		t.Fatalf("new source: %v", err)
	}
	defer source.Close()
	if err := source.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if err := source.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("put b: %v", err)
	}
	if err := source.Delete([]byte("b")); err != nil {
		t.Fatalf("delete b: %v", err)
	}
	if err := source.AddReplica("users", "node-b"); err != nil {
		t.Fatalf("add replica: %v", err)
	}
	payload, err := source.exportStateSnapshot()
	if err != nil {
		t.Fatalf("export snapshot: %v", err)
	}

	targetDir := t.TempDir()
	target, err := New(opts(targetDir))
	if err != nil {
		t.Fatalf("new target: %v", err)
	}
	if err := target.applyStateSnapshotToEmpty(payload); err != nil {
		t.Fatalf("apply snapshot: %v", err)
	}
	if entry, ok := target.Get([]byte("a")); !ok || string(entry.Value) != "1" {
		t.Fatalf("expected restored a=1, got %q found=%v", string(entry.Value), ok)
	}
	if _, ok := target.Get([]byte("b")); ok {
		t.Fatalf("expected deleted key b to remain absent")
	}
	if got := target.ClusterStatus().Revision; got == 0 {
		t.Fatalf("expected restored control revision")
	}
	if !stateSnapshotShardHasReplica(target.Shards(), "users", "node-b") {
		t.Fatalf("expected restored node-b replica, got %+v", target.Shards())
	}
	if target.commitLogAppliedIndex != source.commitLogAppliedIndex {
		t.Fatalf("expected applied index %d, got %d", source.commitLogAppliedIndex, target.commitLogAppliedIndex)
	}
	if err := target.Close(); err != nil {
		t.Fatalf("close target: %v", err)
	}

	restarted, err := New(opts(targetDir))
	if err != nil {
		t.Fatalf("restart target: %v", err)
	}
	defer restarted.Close()
	if entry, ok := restarted.Get([]byte("a")); !ok || string(entry.Value) != "1" {
		t.Fatalf("expected restarted a=1, got %q found=%v", string(entry.Value), ok)
	}
	if !stateSnapshotShardHasReplica(restarted.Shards(), "users", "node-b") {
		t.Fatalf("expected restarted node-b replica, got %+v", restarted.Shards())
	}
}

func TestStateSnapshotRestoreRejectsNonEmptyEngine(t *testing.T) {
	source, err := New(Options{DataDir: t.TempDir(), CompactionL0Threshold: 0})
	if err != nil {
		t.Fatalf("new source: %v", err)
	}
	defer source.Close()
	if err := source.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("put source: %v", err)
	}
	payload, err := source.exportStateSnapshot()
	if err != nil {
		t.Fatalf("export snapshot: %v", err)
	}

	target, err := New(Options{DataDir: t.TempDir(), CompactionL0Threshold: 0})
	if err != nil {
		t.Fatalf("new target: %v", err)
	}
	defer target.Close()
	if err := target.Put([]byte("existing"), []byte("value")); err != nil {
		t.Fatalf("put target: %v", err)
	}
	if err := target.applyStateSnapshotToEmpty(payload); err == nil {
		t.Fatalf("expected non-empty restore rejection")
	}
}

func TestStateSnapshotRestoreRejectsMismatchedRaftIndex(t *testing.T) {
	source, err := New(Options{DataDir: t.TempDir(), CompactionL0Threshold: 0})
	if err != nil {
		t.Fatalf("new source: %v", err)
	}
	defer source.Close()
	if err := source.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("put source: %v", err)
	}
	payload, err := source.exportStateSnapshot()
	if err != nil {
		t.Fatalf("export snapshot: %v", err)
	}

	target, err := New(Options{DataDir: t.TempDir(), CompactionL0Threshold: 0})
	if err != nil {
		t.Fatalf("new target: %v", err)
	}
	defer target.Close()
	if err := target.applyStateSnapshotToEmptyAt(source.commitLogAppliedIndex+1, payload); err == nil {
		t.Fatalf("expected mismatched raft index rejection")
	}
}

func TestEtcdRaftSnapshotPersistsLSMStatePayloadAfterEngineApply(t *testing.T) {
	dataDir := t.TempDir()
	db, err := New(Options{
		DataDir:               dataDir,
		NodeID:                "node-a",
		ClusterID:             "cluster-dev",
		CompactionL0Threshold: 0,
		CommitLog: &CommitLogOptions{
			Provider: CommitLogProviderEtcdRaft,
			SnapshotPolicy: CommitLogSnapshotPolicy{
				AppliedEntries: 1,
			},
		},
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer db.Close()
	if err := db.Put([]byte("alpha"), []byte("1")); err != nil {
		t.Fatalf("put: %v", err)
	}

	snapshotPath := filepath.Join(
		dataDir,
		"raft",
		fmt.Sprintf("commitlog-%016x", RaftPeerID("node-a")),
		"snapshot.json",
	)
	data, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("read raft snapshot: %v", err)
	}
	var disk struct {
		Snapshot raftpb.Snapshot `json:"snapshot"`
	}
	if err := json.Unmarshal(data, &disk); err != nil {
		t.Fatalf("decode raft snapshot: %v", err)
	}
	if disk.Snapshot.Metadata.Index != db.commitLogAppliedIndex {
		t.Fatalf("expected snapshot index %d, got %d", db.commitLogAppliedIndex, disk.Snapshot.Metadata.Index)
	}
	if len(disk.Snapshot.Data) == 0 {
		t.Fatalf("expected raft snapshot data to contain LSM state payload")
	}
	var payload lsmStateSnapshot
	if err := json.Unmarshal(disk.Snapshot.Data, &payload); err != nil {
		t.Fatalf("decode lsm snapshot payload: %v", err)
	}
	if payload.CommitLogAppliedIndex != db.commitLogAppliedIndex {
		t.Fatalf("expected payload applied index %d, got %d", db.commitLogAppliedIndex, payload.CommitLogAppliedIndex)
	}
	if !stateSnapshotHasEntry(payload, "alpha", "1") {
		t.Fatalf("expected payload to include alpha=1, got %+v", payload.Entries)
	}
}

func stateSnapshotShardHasReplica(shards []ShardStatus, shardID string, nodeID string) bool {
	for _, shard := range shards {
		if shard.ID != shardID {
			continue
		}
		for _, replica := range shard.Replicas {
			if replica.NodeID == nodeID {
				return true
			}
		}
	}
	return false
}

func stateSnapshotHasEntry(snapshot lsmStateSnapshot, key string, value string) bool {
	for _, entry := range snapshot.Entries {
		if string(entry.Key) == key && string(entry.Value) == value && !entry.Tombstone {
			return true
		}
	}
	return false
}
