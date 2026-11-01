package engine

import (
	"reflect"
	"testing"

	"lsmengine/internal/lsm/metadata"
	"lsmengine/internal/lsm/tableset"
)

func TestStatsActiveTableLevels(t *testing.T) {
	store := &LSM{tables: tableset.NewSet([]tableset.Table{
		{Meta: metadata.TableMeta{Path: "l2", Level: 2, SizeBytes: 30}},
		{Meta: metadata.TableMeta{Path: "l0a", Level: 0, SizeBytes: 10}},
		{Meta: metadata.TableMeta{Path: "l0b", Level: 0, SizeBytes: 20}},
	})}
	want := []SSTableLevelStats{{Level: 0, TableCount: 2, SizeBytes: 30}, {Level: 2, TableCount: 1, SizeBytes: 30}}
	stats := store.Stats()
	if stats.TableCount != 3 || stats.SSTableBytes != 60 || !reflect.DeepEqual(stats.SSTableLevels, want) {
		t.Fatalf("unexpected storage stats: %+v", stats)
	}
	stats.SSTableLevels[0].SizeBytes = 999
	if got := store.Stats(); !reflect.DeepEqual(got.SSTableLevels, want) {
		t.Fatalf("caller mutated subsequent snapshot: %+v", got)
	}
	empty := (&LSM{tables: tableset.NewSet(nil)}).Stats()
	if empty.TableCount != 0 || empty.SSTableBytes != 0 || len(empty.SSTableLevels) != 0 {
		t.Fatalf("unexpected empty stats: %+v", empty)
	}
}

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
	if !stats.CompactionEnabled {
		t.Fatalf("expected compaction enabled")
	}
	if stats.Closing || stats.Closed {
		t.Fatalf("expected open state, got closing=%v closed=%v", stats.Closing, stats.Closed)
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
