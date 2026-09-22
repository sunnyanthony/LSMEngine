package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"lsmengine/pkg/lsm/errs"
)

func TestCommittedApplyConcurrentDrainsAreIdempotent(t *testing.T) {
	db, err := New(Options{DataDir: t.TempDir(), NodeID: "node-a", CommitLog: &CommitLogOptions{Provider: CommitLogProviderEtcdRaft}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var targets []uint64
	for i := 0; i < 20; i++ {
		entry, err := db.commitLog.CommitData(context.Background(), dataMutation{Kind: "put", Key: []byte(fmt.Sprint(i)), Value: []byte("value")})
		if err != nil {
			t.Fatal(err)
		}
		targets = append(targets, entry.Commit.Index)
	}
	var wg sync.WaitGroup
	for _, target := range targets {
		wg.Add(1)
		go func(index uint64) {
			defer wg.Done()
			if err := db.committedApply.drain(index); err != nil {
				t.Errorf("drain: %v", err)
			}
		}(target)
	}
	wg.Wait()
	events, err := db.ReadCDCEvents("default", 0, 100)
	if err != nil || len(events.Events) != len(targets) {
		t.Fatalf("events=%d err=%v", len(events.Events), err)
	}
	for i, event := range events.Events {
		if event.Offset != targets[i] {
			t.Fatal("concurrent drain reordered CDC")
		}
		entry, ok := db.Get([]byte(fmt.Sprint(i)))
		if !ok || entry.Seq != targets[i] {
			t.Fatal("concurrent drain lost committed data")
		}
	}
}

func TestCommittedApplyFailureBlocksBothKindsUntilRestart(t *testing.T) {
	for _, failedKind := range []string{"data", "control"} {
		t.Run(failedKind, func(t *testing.T) {
			fs := &failingControlStateFS{}
			opts := Options{DataDir: t.TempDir(), NodeID: "node-a", IOFS: fs, CommitLog: &CommitLogOptions{Provider: CommitLogProviderEtcdRaft},
				ShardMap: []ShardConfig{{ID: "shared", Leader: "node-a", Replicas: []string{"node-a", "node-b"}}}}
			db, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			commitData := func() {
				_, err := db.commitLog.CommitData(context.Background(), dataMutation{Kind: "put", Key: []byte("key"), Value: []byte("value")})
				if err != nil {
					t.Fatal(err)
				}
			}
			commitControl := func() {
				_, err := db.commitLog.CommitControl(context.Background(), controlMutation{Kind: "transfer-leader", ShardID: "shared", Target: "node-b", OperationID: "transfer"})
				if err != nil {
					t.Fatal(err)
				}
			}
			if failedKind == "data" {
				commitData()
				commitControl()
				if err := db.wal.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				commitControl()
				commitData()
				fs.failWrite = true
			}
			cursor := db.committedApply.cursor
			if err := db.committedApply.drain(0); err == nil {
				t.Fatal("injected apply failure not reported")
			}
			if db.committedApply.cursor != cursor || db.control.revision != 0 {
				t.Fatal("failure advanced progress")
			}
			if _, ok := db.Get([]byte("key")); ok {
				t.Fatal("failed stream applied data")
			}
			index := db.commitLog.RuntimeStatus().Index
			fs.failWrite = false
			if err := db.Put([]byte("later"), []byte("value")); err == nil {
				t.Fatal("data bypassed shared failure")
			}
			if err := db.TransferLeader("shared", "node-b"); err == nil {
				t.Fatal("control bypassed shared failure")
			}
			if db.commitLog.RuntimeStatus().Index != index {
				t.Fatal("failed stream accepted another proposal")
			}
			_ = db.Close()
			reopened, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if value, ok := reopened.Get([]byte("key")); !ok || string(value.Value) != "value" {
				t.Fatal("restart missed committed data")
			}
			if reopened.control.revision != 1 || reopened.Shards()[0].Leader != "node-b" {
				t.Fatal("restart missed committed control")
			}
		})
	}
}

func TestCommittedRevisionRejectionAdvancesWithoutBlockingData(t *testing.T) {
	db, err := New(Options{DataDir: t.TempDir(), NodeID: "node-a", CommitLog: &CommitLogOptions{Provider: CommitLogProviderEtcdRaft},
		ShardMap: []ShardConfig{{ID: "shared", Leader: "node-a", Replicas: []string{"node-a", "node-b"}}}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	zero := uint64(0)
	var rejected controlCommittedEntry
	for _, target := range []string{"node-b", "node-a"} {
		rejected, err = db.commitLog.CommitControl(context.Background(), controlMutation{Kind: "transfer-leader", ShardID: "shared", Target: target, OperationID: target, ExpectedRevision: &zero})
		if err != nil {
			t.Fatal(err)
		}
	}
	data, err := db.commitLog.CommitData(context.Background(), dataMutation{Kind: "put", Key: []byte("key"), Value: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.committedApply.drain(rejected.Commit.Index); !errors.Is(err, errs.ErrControlRevisionConflict) {
		t.Fatalf("rejection = %v", err)
	}
	if db.committedApply.cursor != data.Commit.Index || db.control.revision != 1 || db.Shards()[0].Leader != "node-b" {
		t.Fatal("rejection corrupted stream progress/state")
	}
	if err := db.committedApply.drain(data.Commit.Index); err != nil {
		t.Fatal(err)
	}
	events, err := db.ReadCDCEvents("shared", 0, 10)
	if err != nil || len(events.Events) != 1 {
		t.Fatalf("duplicate drain CDC: %+v %v", events, err)
	}
}
