package engine

import (
	"errors"
	"reflect"
	"testing"
	"time"

	compactionruntime "lsmengine/internal/lsm/compaction/runtime"
	"lsmengine/internal/lsm/metadata"
	"lsmengine/internal/lsm/tableset"
	"lsmengine/pkg/lsm/errs"
)

func TestStatsSnapshot(t *testing.T) {
	store, err := New(Options{
		DataDir:               t.TempDir(),
		CompactionL0Threshold: 1,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}()

	if err := store.Put([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("put: %v", err)
	}

	stats := store.Stats()
	if stats.MemtableBytes == 0 {
		t.Fatalf("expected memtable bytes > 0")
	}
	if stats.MemtableEntries == 0 {
		t.Fatalf("expected memtable entries > 0")
	}
	if stats.FlushQueueCapacity != 4 {
		t.Fatalf("expected default flush queue capacity 4, got %d", stats.FlushQueueCapacity)
	}
	if !stats.CompactionEnabled {
		t.Fatalf("expected compaction enabled")
	}
	if stats.Closing || stats.Closed {
		t.Fatalf("expected open state, got closing=%v closed=%v", stats.Closing, stats.Closed)
	}
}

func TestStatsSSTableLevelsAndCompactionPressure(t *testing.T) {
	store := &LSM{
		tables: tableset.NewSet([]tableset.Table{
			{Meta: metadata.TableMeta{Path: "l0-a.sst", Level: 0, SizeBytes: 10, SeqMax: 3}},
			{Meta: metadata.TableMeta{Path: "l0-b.sst", Level: 0, SizeBytes: 20, SeqMax: 2}},
			{Meta: metadata.TableMeta{Path: "l1-a.sst", Level: 1, SizeBytes: 30, SeqMax: 1}},
		}),
		flushQueueCapacity:    7,
		compactionL0Threshold: 2,
		compactionSvc:         &compactionruntime.Runtime{},
	}

	stats := store.Stats()
	if stats.TableCount != 3 || stats.SSTableCount != 3 {
		t.Fatalf("expected 3 tables, got table_count=%d sstable_count=%d", stats.TableCount, stats.SSTableCount)
	}
	if stats.SSTableBytes != 60 {
		t.Fatalf("expected 60 sstable bytes, got %d", stats.SSTableBytes)
	}
	if stats.L0TableCount != 2 || stats.L0SizeBytes != 30 {
		t.Fatalf("expected l0 count=2 bytes=30, got count=%d bytes=%d", stats.L0TableCount, stats.L0SizeBytes)
	}
	if stats.CompactionL0Threshold != 2 || !stats.CompactionPending {
		t.Fatalf("expected pending compaction at l0 threshold, got threshold=%d pending=%v", stats.CompactionL0Threshold, stats.CompactionPending)
	}
	if stats.FlushQueueCapacity != 7 {
		t.Fatalf("expected flush queue capacity 7, got %d", stats.FlushQueueCapacity)
	}
	if got := stats.SSTableLevels; len(got) != 2 ||
		got[0] != (SSTableLevelStats{Level: 0, TableCount: 2, SizeBytes: 30}) ||
		got[1] != (SSTableLevelStats{Level: 1, TableCount: 1, SizeBytes: 30}) {
		t.Fatalf("unexpected level stats: %+v", got)
	}
}

func TestStatsIncludesFlushedSSTables(t *testing.T) {
	opts := Options{
		DataDir:       t.TempDir(),
		MemtableLimit: 1,
	}
	store, err := New(opts)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}()

	if err := store.Put([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("put: %v", err)
	}

	var stats Stats
	waitForStats(t, func() bool {
		stats = store.Stats()
		return stats.SSTableCount >= 1
	})
	if stats.TableCount != stats.SSTableCount {
		t.Fatalf("expected legacy table count to match sstable count, got table_count=%d sstable_count=%d", stats.TableCount, stats.SSTableCount)
	}
	if stats.SSTableBytes == 0 {
		t.Fatalf("expected sstable bytes > 0")
	}
	if stats.L0TableCount == 0 {
		t.Fatalf("expected l0 table count > 0")
	}
	if len(stats.SSTableLevels) == 0 || stats.SSTableLevels[0].Level != 0 {
		t.Fatalf("expected l0 level stats, got %+v", stats.SSTableLevels)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close before reopen: %v", err)
	}
	reopened, err := New(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Fatalf("close reopened store: %v", err)
		}
	}()
	restored := reopened.Stats()
	if restored.SSTableCount != stats.SSTableCount || restored.SSTableBytes != stats.SSTableBytes ||
		!reflect.DeepEqual(restored.SSTableLevels, stats.SSTableLevels) {
		t.Fatalf("storage stats changed after recovery: before=%+v after=%+v", stats, restored)
	}
}

func TestStatsPointReadMetrics(t *testing.T) {
	store, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}()

	if err := store.Put([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, ok := store.Get([]byte("a")); !ok {
		t.Fatalf("expected memtable hit")
	}
	if _, ok := store.Get([]byte("missing")); ok {
		t.Fatalf("expected miss")
	}
	if err := store.Delete([]byte("a")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok := store.Get([]byte("a")); ok {
		t.Fatal("tombstone returned a value")
	}

	stats := store.Stats()
	if stats.PointReads != 3 {
		t.Fatalf("expected 3 point reads, got %d", stats.PointReads)
	}
	if stats.PointReadMemtableHits != 2 {
		t.Fatalf("expected value and tombstone memtable hits, got %d", stats.PointReadMemtableHits)
	}
	if stats.PointReadMisses != 1 {
		t.Fatalf("expected 1 miss, got %d", stats.PointReadMisses)
	}
	if stats.PointReadSSTableProbes != 0 || stats.PointReadMaxSSTableProbes != 0 {
		t.Fatalf("expected no sstable probes, got probes=%d max=%d", stats.PointReadSSTableProbes, stats.PointReadMaxSSTableProbes)
	}
}

func TestStatsSSTableReadAmplificationMetrics(t *testing.T) {
	store, err := New(Options{
		DataDir:       t.TempDir(),
		MemtableLimit: 1,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}()

	if err := store.Put([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Put([]byte("c"), []byte("d")); err != nil {
		t.Fatalf("second put: %v", err)
	}
	waitForStats(t, func() bool {
		stats := store.Stats()
		return stats.SSTableCount == 2 && stats.ImmutableCount == 0 && stats.FlushQueueDepth == 0
	})
	if _, ok := store.Get([]byte("a")); !ok {
		t.Fatalf("expected sstable hit")
	}
	if _, ok := store.Get([]byte("c")); !ok {
		t.Fatalf("expected newest sstable hit")
	}
	if _, ok := store.Get([]byte("z")); ok {
		t.Fatalf("expected sstable miss")
	}

	stats := store.Stats()
	if stats.PointReads != 3 {
		t.Fatalf("expected 3 point reads, got %d", stats.PointReads)
	}
	if stats.PointReadSSTableHits != 2 || stats.PointReadMisses != 1 {
		t.Fatalf("expected two sstable hits and one miss, got hits=%d misses=%d", stats.PointReadSSTableHits, stats.PointReadMisses)
	}
	if stats.PointReadSSTableProbes != 5 || stats.PointReadMaxSSTableProbes != 2 {
		t.Fatalf("expected 2+1+2 probes and max=2, got probes=%d max=%d", stats.PointReadSSTableProbes, stats.PointReadMaxSSTableProbes)
	}
	if stats.SSTableFlow.CacheHit+stats.SSTableFlow.CacheMiss+stats.SSTableFlow.FilterPass == 0 {
		t.Fatalf("expected sstable flow metrics, got %+v", stats.SSTableFlow)
	}
}

func TestStatsCompactionRuntimeMetrics(t *testing.T) {
	store, err := New(Options{
		DataDir:               t.TempDir(),
		MemtableLimit:         1,
		CompactionL0Threshold: 1,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}()

	if err := store.Put([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("put: %v", err)
	}

	var stats Stats
	waitForStats(t, func() bool {
		stats = store.Stats()
		return stats.CompactionRuntime.Triggers > 0 &&
			stats.CompactionRuntime.Runs > 0 &&
			stats.CompactionRuntime.SuccessfulSteps > 0
	})
	if stats.CompactionRuntime.Steps < stats.CompactionRuntime.SuccessfulSteps {
		t.Fatalf("expected steps to include successful steps, got %+v", stats.CompactionRuntime)
	}
	if stats.CompactionRuntime.Errors != 0 {
		t.Fatalf("expected no compaction errors, got %+v", stats.CompactionRuntime)
	}
}

func TestWriteBackpressureRejectsBeforeCompactionThresholdFlush(t *testing.T) {
	store, err := New(Options{
		DataDir:                           t.TempDir(),
		MemtableLimit:                     1,
		CompactionL0Threshold:             0,
		CompactionBackpressureL0Threshold: 1,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}()

	if err := store.Put([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("put: %v", err)
	}
	waitForStats(t, func() bool {
		return store.Stats().L0TableCount >= 1
	})

	err = store.Put([]byte("c"), []byte("d"))
	if !errors.Is(err, errs.ErrBackpressure) {
		t.Fatalf("expected backpressure, got %v", err)
	}
	stats := store.Stats()
	if !stats.WriteBackpressure.Active {
		t.Fatalf("expected write backpressure active, got %+v", stats.WriteBackpressure)
	}
	if stats.WriteBackpressure.Reason != writeBackpressureReasonL0 {
		t.Fatalf("expected l0 reason, got %+v", stats.WriteBackpressure)
	}
	if stats.WriteBackpressure.Rejects != 1 {
		t.Fatalf("expected one rejected write, got %+v", stats.WriteBackpressure)
	}
	if stats.Seq != 1 {
		t.Fatalf("expected rejected write not to commit, got seq=%d", stats.Seq)
	}
}

func TestWriteBackpressureDoesNotBlockCommittedApply(t *testing.T) {
	store, err := New(Options{
		DataDir:                           t.TempDir(),
		MemtableLimit:                     1,
		CompactionL0Threshold:             0,
		CompactionBackpressureL0Threshold: 1,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}()

	if err := store.Put([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("put: %v", err)
	}
	waitForStats(t, func() bool {
		return store.Stats().L0TableCount >= 1
	})

	seq, err := store.writer.applyCommittedData(dataCommittedEntry{
		Commit:   CommitLogCommit{Index: 2, Term: 1},
		Seq:      2,
		Mutation: dataMutation{Kind: "put", Key: []byte("c"), Value: []byte("d")},
	})
	if err != nil {
		t.Fatalf("apply committed data: %v", err)
	}
	if seq != 2 {
		t.Fatalf("expected seq 2, got %d", seq)
	}
	if _, ok := store.Get([]byte("c")); !ok {
		t.Fatalf("expected committed entry to apply despite local write backpressure")
	}
	stats := store.Stats()
	if stats.WriteBackpressure.Rejects != 0 {
		t.Fatalf("expected committed apply not to count as rejected write, got %+v", stats.WriteBackpressure)
	}
}

func TestWriteBackpressureRejectsAtFlushQueueThreshold(t *testing.T) {
	store, err := New(Options{
		DataDir:                         t.TempDir(),
		MemtableLimit:                   1,
		FlushBackpressureQueueThreshold: 1,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() {
		store.memMu.Lock()
		store.flushQueue = nil
		store.memMu.Unlock()
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}()

	store.memMu.Lock()
	store.flushQueue = append(store.flushQueue, store.mem)
	store.memMu.Unlock()

	err = store.Put([]byte("a"), []byte("b"))
	if !errors.Is(err, errs.ErrBackpressure) {
		t.Fatalf("expected backpressure, got %v", err)
	}
	stats := store.Stats()
	if stats.WriteBackpressure.Reason != writeBackpressureReasonFlushQueue {
		t.Fatalf("expected flush queue reason, got %+v", stats.WriteBackpressure)
	}
}

func TestFlushBackpressurePreservesCommittedMutations(t *testing.T) {
	for _, kind := range []string{"put", "delete"} {
		t.Run(kind, func(t *testing.T) {
			opts := Options{DataDir: t.TempDir(), MemtableLimit: 4096}
			store, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				store.flushBlocked.Store(false)
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
			}()
			if err := store.Put([]byte("key"), []byte("old")); err != nil {
				t.Fatal(err)
			}
			before := store.commitLog.RuntimeStatus().Index
			store.flushBlocked.Store(true)
			mutation := dataMutation{Kind: kind, Key: []byte("key")}
			if kind == "put" {
				mutation.Value = []byte("new")
				err = store.Put(mutation.Key, mutation.Value)
			} else {
				err = store.Delete(mutation.Key)
			}
			if !errors.Is(err, errs.ErrBackpressure) || store.commitLog.RuntimeStatus().Index != before {
				t.Fatalf("rejected mutation reached commit log: err=%v status=%+v", err, store.commitLog.RuntimeStatus())
			}
			seq := store.Stats().Seq + 1
			if _, err := store.writer.applyCommittedData(dataCommittedEntry{
				Commit: CommitLogCommit{Index: before + 1, Term: 1}, Seq: seq, Mutation: mutation,
			}); err != nil {
				t.Fatalf("committed mutation blocked: %v", err)
			}
			if got := store.Stats().WriteBackpressure.Rejects; got != 1 {
				t.Fatalf("apply changed admission rejects: %d", got)
			}
			store.flushBlocked.Store(false)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := reopened.Close(); err != nil {
					t.Fatal(err)
				}
			}()
			entry, found := reopened.Get(mutation.Key)
			if found != (kind == "put") || (found && string(entry.Value) != "new") {
				t.Fatalf("committed mutation lost after reopen: found=%v entry=%+v", found, entry)
			}
		})
	}
}

func TestHealthStates(t *testing.T) {
	store, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if health := store.Health(); !health.Ready || health.Reason != "ok" {
		t.Fatalf("expected ok health, got %+v", health)
	}

	store.closing.Store(true)
	if health := store.Health(); health.Reason != "closing" {
		t.Fatalf("expected closing health, got %+v", health)
	}
	store.closing.Store(false)

	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if health := store.Health(); health.Reason != "closed" {
		t.Fatalf("expected closed health, got %+v", health)
	}
}

func waitForStats(t *testing.T, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for stats condition")
}
