package engine

import (
	"testing"
	"time"

	compactionruntime "lsmengine/internal/lsm/compaction/runtime"
	"lsmengine/internal/lsm/metadata"
	"lsmengine/internal/lsm/tableset"
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

	stats := store.Stats()
	if stats.PointReads != 2 {
		t.Fatalf("expected 2 point reads, got %d", stats.PointReads)
	}
	if stats.PointReadMemtableHits != 1 {
		t.Fatalf("expected 1 memtable hit, got %d", stats.PointReadMemtableHits)
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
	waitForStats(t, func() bool {
		return store.Stats().SSTableCount >= 1
	})
	if _, ok := store.Get([]byte("a")); !ok {
		t.Fatalf("expected sstable hit")
	}
	if _, ok := store.Get([]byte("z")); ok {
		t.Fatalf("expected sstable miss")
	}

	stats := store.Stats()
	if stats.PointReads != 2 {
		t.Fatalf("expected 2 point reads, got %d", stats.PointReads)
	}
	if stats.PointReadSSTableHits != 1 || stats.PointReadMisses != 1 {
		t.Fatalf("expected one sstable hit and one miss, got hits=%d misses=%d", stats.PointReadSSTableHits, stats.PointReadMisses)
	}
	if stats.PointReadSSTableProbes < 2 || stats.PointReadMaxSSTableProbes == 0 {
		t.Fatalf("expected sstable probe metrics, got probes=%d max=%d", stats.PointReadSSTableProbes, stats.PointReadMaxSSTableProbes)
	}
	if stats.SSTableFlow.CacheHit+stats.SSTableFlow.CacheMiss+stats.SSTableFlow.FilterPass == 0 {
		t.Fatalf("expected sstable flow metrics, got %+v", stats.SSTableFlow)
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
