package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	internalcommitlog "lsmengine/internal/lsm/commitlog"
	"lsmengine/pkg/lsm/errs"
)

type blockingCommittedSource struct {
	internalcommitlog.CommittedEntrySource
	entered chan struct{}
	release chan struct{}
}

type blockedProposalConsensus struct {
	commitLogConsensus
	entered chan struct{}
	release chan struct{}
}

func (c *blockedProposalConsensus) CommitData(ctx context.Context, m dataMutation) (dataCommittedEntry, error) {
	close(c.entered)
	<-c.release
	return c.commitLogConsensus.CommitData(ctx, m)
}

func TestCloseWaitsForAdmittedLocalProposal(t *testing.T) {
	opts := Options{DataDir: t.TempDir(), NodeID: "node-a", CommitLog: &CommitLogOptions{Provider: CommitLogProviderEtcdRaft}}
	db, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	blocked := &blockedProposalConsensus{commitLogConsensus: db.commitLog, entered: make(chan struct{}), release: make(chan struct{})}
	db.commitLog = blocked
	var once sync.Once
	release := func() { once.Do(func() { close(blocked.release) }) }
	defer release()
	writeDone := make(chan error, 1)
	go func() { writeDone <- db.Put([]byte("key"), []byte("value")) }()
	select {
	case <-blocked.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("proposal did not start")
	}
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	deadline := time.Now().Add(2 * time.Second)
	for !db.isClosing() {
		if time.Now().After(deadline) {
			t.Fatal("close did not start")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-closed:
		t.Fatalf("close returned while provider operation pending: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	release()
	if err := <-writeDone; !errors.Is(err, errs.ErrClosed) {
		t.Fatalf("write = %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	reopened, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if entry, ok := reopened.Get([]byte("key")); !ok || string(entry.Value) != "value" {
		t.Fatal("reopen lost admitted committed proposal")
	}
}

func (s *blockingCommittedSource) CommittedEntriesAfter(index uint64) []internalcommitlog.RecoveredEntry {
	close(s.entered)
	<-s.release
	return s.CommittedEntrySource.CommittedEntriesAfter(index)
}

func TestCloseQuiescesCommittedApplyBeforeClosingWAL(t *testing.T) {
	opts := Options{DataDir: t.TempDir(), NodeID: "node-a", CommitLog: &CommitLogOptions{Provider: CommitLogProviderEtcdRaft}}
	db, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	blocked := &blockingCommittedSource{CommittedEntrySource: db.committedApply.source, entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(blocked.release) }) }
	defer release()
	db.committedApply.source = blocked
	writeDone := make(chan error, 1)
	go func() { writeDone <- db.Put([]byte("key"), []byte("value")) }()
	select {
	case <-blocked.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("apply did not start")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- db.Close() }()
	deadline := time.Now().Add(2 * time.Second)
	for !db.isClosing() {
		if time.Now().After(deadline) {
			t.Fatal("close did not start")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-closeDone:
		t.Fatalf("close bypassed active apply: %v", err)
	default:
	}
	release()
	if err := <-writeDone; !errors.Is(err, errs.ErrClosed) {
		t.Fatalf("write after close admission = %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	if err := db.HandlePeerMessages(context.Background(), []CommitLogPeerMessage{{From: 1, To: 2}}); !errors.Is(err, errs.ErrClosed) {
		t.Fatalf("closed ingress = %v", err)
	}
	reopened, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if entry, ok := reopened.Get([]byte("key")); !ok || string(entry.Value) != "value" {
		t.Fatal("close lost committed but unapplied write")
	}
}

func TestCommittedBackpressureRetainsHeadForRestart(t *testing.T) {
	opts := Options{DataDir: t.TempDir(), NodeID: "node-a", MemtableLimit: 64, FlushQueueSize: 1, CommitLog: &CommitLogOptions{Provider: CommitLogProviderEtcdRaft}}
	db, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 3; i++ {
		_, err := db.commitLog.CommitData(context.Background(), dataMutation{Kind: "put", Key: []byte(fmt.Sprint(i)), Value: make([]byte, 128)})
		if err != nil {
			t.Fatal(err)
		}
	}
	cursor := db.committedApply.cursor
	// Inject the same admission flag used by failed flush enqueue.
	db.flushBlocked.Store(true)
	if err := db.committedApply.drain(0); !errors.Is(err, errs.ErrBackpressure) {
		t.Fatalf("drain = %v", err)
	}
	if db.committedApply.cursor != cursor {
		t.Fatal("backpressure consumed head")
	}
	db.flushBlocked.Store(false)
	if err := db.committedApply.drain(0); !errors.Is(err, errs.ErrBackpressure) {
		t.Fatal("stream did not stay fail-closed")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for i := 0; i < 3; i++ {
		if _, ok := reopened.Get([]byte(fmt.Sprint(i))); !ok {
			t.Fatalf("lost entry %d", i)
		}
	}
}

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
