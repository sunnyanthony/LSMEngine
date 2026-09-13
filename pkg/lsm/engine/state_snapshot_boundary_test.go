package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"lsmengine/pkg/lsm/types"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestStateSnapshotBoundaryRequiresExactAppliedMutation(t *testing.T) {
	db, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	applied, seq := db.commitLogApplied(), db.Stats().Seq
	snapshotter := lsmStateSnapshotter{l: db}
	for _, mutation := range []uint64{applied - 1, applied + 1} {
		if data, ready, err := snapshotter.CaptureStateSnapshotBoundary(applied+2, mutation); err != nil || ready || data != nil {
			t.Fatalf("mismatched mutation %d must defer capture: ready=%t err=%v", mutation, ready, err)
		}
	}
	if _, _, err := snapshotter.CaptureStateSnapshotBoundary(applied, applied+1); err == nil {
		t.Fatal("mutation beyond snapshot boundary must be rejected")
	}
	payload, ready, err := snapshotter.CaptureStateSnapshotBoundary(applied+2, applied)
	if err != nil || !ready {
		t.Fatalf("capture provider-only tail: ready=%t err=%v", ready, err)
	}
	snapshot, err := decodeStateSnapshotForRaftIndex(applied+2, payload)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Seq != seq || snapshot.CommitLogAppliedIndex != applied+2 || db.commitLogApplied() != applied || db.Stats().Seq != seq {
		t.Fatal("boundary capture changed sequence or live replay floor")
	}
	if _, err := decodeStateSnapshotForRaftIndex(applied, payload); err == nil {
		t.Fatal("boundary capture must retain exact payload/index validation")
	}
}

func TestRaftRestartPreservesWALTailNewerThanSnapshot(t *testing.T) {
	opts := Options{DataDir: t.TempDir(), NodeID: "node-a", CommitLog: &CommitLogOptions{
		Provider: CommitLogProviderEtcdRaft, SnapshotPolicy: CommitLogSnapshotPolicy{AppliedEntries: 4},
	}}
	db, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, value := range []string{"first", "snapshot", "tail"} {
		if err := db.Put([]byte("key"), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	applied := db.commitLogApplied()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if entry, ok := restarted.Get([]byte("key")); !ok || string(entry.Value) != "tail" || restarted.commitLogApplied() < applied {
		t.Fatalf("snapshot restore rolled back newer WAL tail: value=%q found=%t applied=%d", entry.Value, ok, restarted.commitLogApplied())
	}
}

func TestSnapshotRestoreCompletesPartialMaterializationAtEqualSeq(t *testing.T) {
	db, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, entry := range []dataCommittedEntry{
		{Commit: CommitLogCommit{Index: 6, Term: 1}, Seq: 6, Mutation: dataMutation{Kind: "put", Key: []byte("stale"), Value: []byte("remove")}},
		{Commit: CommitLogCommit{Index: 7, Term: 1}, Seq: 7, Mutation: dataMutation{Kind: "put", Key: []byte("first"), Value: []byte("value")}},
	} {
		if err := db.applyCommittedDataFromLog(entry); err != nil {
			t.Fatal(err)
		}
	}
	payload, err := json.Marshal(lsmStateSnapshot{Version: lsmStateSnapshotVersion, Seq: 7, CommitLogAppliedIndex: 8,
		Entries: []types.Entry{{Key: []byte("first"), Value: []byte("value"), Seq: 7}, {Key: []byte("missing"), Value: []byte("restored"), Seq: 7}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := (lsmStateSnapshotter{l: db}).RestoreStateSnapshot(8, payload); err != nil {
		t.Fatal(err)
	}
	if entry, ok := db.Get([]byte("missing")); !ok || string(entry.Value) != "restored" {
		t.Fatal("equal-sequence restore skipped missing snapshot record")
	}
	if _, ok := db.Get([]byte("stale")); ok || db.commitLogApplied() != 8 {
		t.Fatal("snapshot restore did not remove stale data or install boundary")
	}
}

func TestSnapshotRestorePreservesNewerDataAndControl(t *testing.T) {
	db, err := New(Options{DataDir: t.TempDir(), NodeID: "node-a",
		ShardMap: []ShardConfig{{ID: "users", Replicas: []string{"node-a"}, Leader: "node-a"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Put([]byte("key"), []byte("snapshot")); err != nil {
		t.Fatal(err)
	}
	payload, err := db.exportStateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	boundary := db.commitLogApplied()
	if err := db.Put([]byte("key"), []byte("newer")); err != nil {
		t.Fatal(err)
	}
	if err := db.AddReplica("users", "node-b"); err != nil {
		t.Fatal(err)
	}
	applied := db.commitLogApplied()
	if err := (lsmStateSnapshotter{l: db}).RestoreStateSnapshot(boundary, payload); err != nil {
		t.Fatal(err)
	}
	if entry, ok := db.Get([]byte("key")); !ok || string(entry.Value) != "newer" {
		t.Fatal("restore overwrote newer WAL data")
	}
	if !stateSnapshotShardHasReplica(db.Shards(), "users", "node-b") || db.commitLogApplied() != applied {
		t.Fatal("restore rolled back newer control state or replay progress")
	}
}

func TestEtcdRaftMembershipSnapshotRestoresRealLSMState(t *testing.T) {
	dir := t.TempDir()
	db, err := New(Options{DataDir: dir, NodeID: "node-a", CommitLog: &CommitLogOptions{
		Provider: CommitLogProviderEtcdRaft, Transport: &recordingRaftTransport{},
		SnapshotPolicy: CommitLogSnapshotPolicy{AppliedEntries: 1},
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	applied := db.commitLogApplied()
	if err := db.AddRaftPeer("node-d"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "raft", fmt.Sprintf("commitlog-%016x", RaftPeerID("node-a")), "snapshot.json"))
	if err != nil {
		t.Fatal(err)
	}
	var disk struct {
		Snapshot raftpb.Snapshot `json:"snapshot"`
	}
	if err := json.Unmarshal(data, &disk); err != nil {
		t.Fatal(err)
	}
	boundary := disk.Snapshot.Metadata.Index
	if boundary <= applied || db.commitLogApplied() != applied {
		t.Fatalf("expected provider-only snapshot progress without live replay-floor change: snapshot=%d applied=%d current=%d", boundary, applied, db.commitLogApplied())
	}
	found := false
	for _, voter := range disk.Snapshot.Metadata.ConfState.Voters {
		found = found || voter == RaftPeerID("node-d")
	}
	if !found {
		t.Fatal("snapshot excludes newly committed member")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	restartedSource, err := New(Options{DataDir: dir, NodeID: "node-a", Raft: &RaftOptions{Peers: []string{"node-a", "node-d"}}, CommitLog: &CommitLogOptions{
		Provider:  CommitLogProviderEtcdRaft,
		Transport: &recordingRaftTransport{}, SnapshotPolicy: CommitLogSnapshotPolicy{AppliedEntries: 1},
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer restartedSource.Close()
	if restartedSource.commitLogApplied() != boundary {
		t.Fatalf("restart did not restore membership snapshot boundary: got=%d want=%d", restartedSource.commitLogApplied(), boundary)
	}
	if entry, ok := restartedSource.Get([]byte("key")); !ok || string(entry.Value) != "value" {
		t.Fatal("source restart lost snapshot value")
	}
	targetDir := t.TempDir()
	target, err := New(Options{DataDir: targetDir})
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	if err := target.applyRaftStateSnapshot(boundary, disk.Snapshot.Data); err != nil {
		t.Fatal(err)
	}
	if entry, ok := target.Get([]byte("key")); !ok || string(entry.Value) != "value" {
		t.Fatal("membership snapshot lost LSM value")
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(Options{DataDir: targetDir})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if entry, ok := restarted.Get([]byte("key")); !ok || string(entry.Value) != "value" {
		t.Fatal("restored membership snapshot value lost on restart")
	}
}
