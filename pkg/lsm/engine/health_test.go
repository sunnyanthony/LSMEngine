package engine

import "testing"

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
