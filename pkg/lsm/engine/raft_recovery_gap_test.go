package engine

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"lsmengine/internal/lsm/wal"
	"lsmengine/internal/lsm/wal/codec"
	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/types"
)

func TestRaftCorruptWALCannotAdvanceRecoveryCheckpoint(t *testing.T) {
	opts := Options{DataDir: t.TempDir(), NodeID: "node-a", CommitLog: &CommitLogOptions{Provider: CommitLogProviderEtcdRaft}}
	db, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	var entries []types.Entry
	for _, key := range []string{"first", "missing", "last"} {
		entry, err := db.commitLog.CommitData(context.Background(), dataMutation{Kind: "put", Key: []byte(key), Value: []byte("value")})
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, types.Entry{Key: entry.Mutation.Key, Value: entry.Mutation.Value, Seq: entry.Seq})
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err := codec.WriteSegmentHeader(&buf, 64*1024, 1); err != nil {
		t.Fatal(err)
	}
	for i, entry := range entries {
		var block bytes.Buffer
		if _, err := codec.WriteBlock(&block, []codec.RecordBuffer{codec.NewRecordBuffer(entry)}); err != nil {
			t.Fatal(err)
		}
		data := block.Bytes()
		if i == 1 {
			data[len(data)-1] ^= 0xff
		}
		buf.Write(data)
	}
	path := filepath.Join(opts.DataDir, "wal.log")
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	// Prove the fixture resynchronizes to a later valid record after the gap.
	var seen []uint64
	err = wal.OpenReplay(path, false).Replay(func(e types.Entry) error { seen = append(seen, e.Seq); return nil })
	if !errors.Is(err, errs.ErrWALCorruptSegment) || len(seen) != 2 || seen[1] != entries[2].Seq {
		t.Fatalf("fixture replay: %v, %v", seen, err)
	}
	opts.MemtableLimit = 1
	for attempt := 0; attempt < 2; attempt++ {
		db, err = New(opts)
		if db != nil {
			db.Close()
			t.Fatal("opened damaged WAL")
		}
		if !errors.Is(err, errs.ErrWALCorruptSegment) {
			t.Fatalf("open = %v", err)
		}
		after, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(after, buf.Bytes()) {
			t.Fatal("failed startup changed WAL")
		}
		files, err := filepath.Glob(filepath.Join(opts.DataDir, "*.sst"))
		if err != nil || len(files) != 0 {
			t.Fatalf("startup flushed past gap: %v, %v", files, err)
		}
	}
}

func TestRaftOversizedMutationRejectedBeforeCommit(t *testing.T) {
	opts := Options{DataDir: t.TempDir(), NodeID: "node-a", CommitLog: &CommitLogOptions{Provider: CommitLogProviderEtcdRaft}}
	db, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	before := db.commitLog.RuntimeStatus().Index
	large := bytes.Repeat([]byte("x"), 128*1024)
	for _, err := range []error{db.Put([]byte("key"), large), db.Delete(large)} {
		if !errors.Is(err, errs.ErrWALRecordTooLarge) {
			db.Close()
			t.Fatalf("expected size rejection: %v", err)
		}
	}
	if db.commitLog.RuntimeStatus().Index != before {
		db.Close()
		t.Fatal("oversized mutation entered raft history")
	}
	if err := db.Put([]byte("valid"), []byte("value")); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, ok := reopened.Get([]byte("valid")); !ok || string(got.Value) != "value" {
		t.Fatal("valid write lost")
	}
}

func TestRaftWALFailureCannotBeOvertaken(t *testing.T) {
	opts := Options{DataDir: t.TempDir(), NodeID: "node-a", CommitLog: &CommitLogOptions{Provider: CommitLogProviderEtcdRaft}}
	db, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	// Closing only the WAL injects failure after consensus has committed.
	if err := db.wal.Close(); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Put([]byte("first"), []byte("value")); err == nil {
		db.Close()
		t.Fatal("expected closed WAL error")
	}
	committed := db.commitLog.RuntimeStatus().Index
	if err := db.Put([]byte("second"), []byte("value")); err == nil {
		db.Close()
		t.Fatal("later write bypassed failed apply")
	}
	if db.commitLog.RuntimeStatus().Index != committed {
		db.Close()
		t.Fatal("later write proposed over failed WAL append")
	}
	_ = db.Close() // The WAL was deliberately closed above.
	reopened, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, ok := reopened.Get([]byte("first")); !ok || string(got.Value) != "value" {
		t.Fatal("committed write was lost after WAL failure")
	}
	if _, ok := reopened.Get([]byte("second")); ok {
		t.Fatal("blocked write unexpectedly committed")
	}
}

func TestRaftControlSaveFailureCannotBeOvertaken(t *testing.T) {
	fs := &failingControlStateFS{}
	opts := Options{DataDir: t.TempDir(), NodeID: "node-a", IOFS: fs,
		CommitLog: &CommitLogOptions{Provider: CommitLogProviderEtcdRaft},
		ShardMap:  []ShardConfig{{ID: "users", Leader: "node-a", Replicas: []string{"node-a", "node-b"}}},
	}
	db, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	fs.failWrite = true
	if err := db.TransferLeader("users", "node-b"); !errors.Is(err, errInjectedControlStateWrite) {
		db.Close()
		t.Fatalf("expected failed materialization: %v", err)
	}
	committed := db.commitLog.RuntimeStatus().Index
	fs.failWrite = false
	if err := db.TransferLeader("users", "node-c"); !errors.Is(err, errInjectedControlStateWrite) {
		db.Close()
		t.Fatalf("later mutation bypassed failed prefix: %v", err)
	}
	if db.commitLog.RuntimeStatus().Index != committed {
		db.Close()
		t.Fatal("later mutation was proposed over an unapplied commit")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if shards := reopened.Shards(); len(shards) != 1 || shards[0].Leader != "node-b" {
		t.Fatalf("missing recovered first mutation: %+v", shards)
	}
	if err := reopened.TransferLeader("users", "node-c"); err != nil {
		t.Fatalf("write did not resume after successful recovery: %v", err)
	}
}

func TestRaftRecoveryMakesProgressWithSmallFlushQueue(t *testing.T) {
	opts := Options{DataDir: t.TempDir(), NodeID: "node-a", MemtableLimit: 64, FlushQueueSize: 1,
		CommitLog: &CommitLogOptions{Provider: CommitLogProviderEtcdRaft}}
	db, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"first", "second", "third"} {
		if _, err := db.commitLog.CommitData(context.Background(), dataMutation{Kind: "put", Key: []byte(key), Value: bytes.Repeat([]byte("v"), 128)}); err != nil {
			db.Close()
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(opts)
	if err != nil {
		t.Fatalf("recovery must drain its own flush work: %v", err)
	}
	defer reopened.Close()
	for _, key := range []string{"first", "second", "third"} {
		if got, ok := reopened.Get([]byte(key)); !ok || len(got.Value) != 128 {
			t.Fatalf("missing recovered key %s", key)
		}
	}
}

func TestRaftRejectedControlDoesNotPreventRestart(t *testing.T) {
	opts := Options{DataDir: t.TempDir(), NodeID: "node-a", CommitLog: &CommitLogOptions{Provider: CommitLogProviderEtcdRaft}}
	db, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.TransferLeader("missing", "node-b"); err == nil {
		db.Close()
		t.Fatal("expected rejected missing shard")
	}
	if _, err := db.commitLog.CommitControl(context.Background(), controlMutation{Kind: "transfer-leader", ShardID: "also-missing", Target: "node-b"}); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(opts)
	if err != nil {
		t.Fatalf("rejected command poisoned startup: %v", err)
	}
	reopened.Close()
}

func TestRaftReplayPreservesCommittedSequence(t *testing.T) {
	opts := Options{DataDir: t.TempDir(), NodeID: "node-a", CommitLog: &CommitLogOptions{Provider: CommitLogProviderEtcdRaft}}
	db, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("key"), []byte("value")); err != nil {
		db.Close()
		t.Fatal(err)
	}
	before, ok := db.Get([]byte("key"))
	if !ok {
		db.Close()
		t.Fatal("missing initial value")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.seq != before.Seq {
		t.Fatalf("replay invented a sequence: got %d want %d", reopened.seq, before.Seq)
	}
}

func TestRaftRestartRecoversCommitBeforeEngineApply(t *testing.T) {
	opts := Options{DataDir: t.TempDir(), NodeID: "node-a", CommitLog: &CommitLogOptions{Provider: CommitLogProviderEtcdRaft}}
	db, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	// Stop at the durable consensus boundary, before the engine materializes it.
	entry, err := db.commitLog.CommitData(context.Background(), dataMutation{Kind: "put", Key: []byte("committed"), Value: []byte("recover-me")})
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if entry.Seq == 0 {
		db.Close()
		t.Fatal("consensus returned no committed sequence")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, ok := reopened.Get([]byte("committed"))
	if !ok || string(got.Value) != "recover-me" || got.Seq != entry.Seq {
		t.Fatalf("durable commit was not materialized on restart: got %+v found=%v want seq=%d", got, ok, entry.Seq)
	}
}

func TestRaftRestartRecoversControlOnce(t *testing.T) {
	opts := Options{DataDir: t.TempDir(), NodeID: "node-a", CommitLog: &CommitLogOptions{Provider: CommitLogProviderEtcdRaft},
		ShardMap: []ShardConfig{{ID: "users", Leader: "node-a", Replicas: []string{"node-a"}}},
	}
	db, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := db.commitLog.CommitControl(context.Background(), controlMutation{Kind: "split", ShardID: "users", Split: []byte("m"), OperationID: "split-request"})
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for restart := 0; restart < 2; restart++ {
		reopened, err := New(opts)
		if err != nil {
			t.Fatalf("restart %d: %v", restart, err)
		}
		state := reopened.control.snapshotStateLocked()
		if len(state.Shards) != 2 || state.Revision != 1 || state.CommitLogAppliedIndex != entry.Commit.Index {
			reopened.Close()
			t.Fatalf("restart %d did not apply split exactly once: %+v", restart, state)
		}
		before := reopened.commitLog.RuntimeStatus().Index
		if err := reopened.TriggerSplitWithOptions("users", []byte("m"), ControlWriteOptions{OperationID: "split-request"}); err != nil {
			reopened.Close()
			t.Fatalf("recovered operation lost retry identity: %v", err)
		}
		if after := reopened.commitLog.RuntimeStatus().Index; after != before {
			reopened.Close()
			t.Fatalf("duplicate retry proposed another entry: %d -> %d", before, after)
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
