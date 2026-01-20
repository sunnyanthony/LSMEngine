//go:build test

package integration_test

import (
	"path/filepath"
	"testing"

	"lsmengine/internal/lsm/sstable"
	sstableconfig "lsmengine/internal/lsm/sstable/config"
	"lsmengine/pkg/lsm"
)

func TestLSMCompactionDropsTombstone(t *testing.T) {
	dir := t.TempDir()
	store, err := lsm.New(lsm.Options{
		DataDir:                  dir,
		MemtableLimit:            4,
		WALSync:                  false,
		CompactionL0Threshold:    2,
		CompactionDropTombstones: true,
	})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	defer store.Close()

	waiter := startCompactionWait(t)
	if err := store.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("put k: %v", err)
	}
	if err := store.Put([]byte("x"), []byte("x1")); err != nil {
		t.Fatalf("put x: %v", err)
	}
	waitForSSTableFiles(t, dir, 1)

	if err := store.Delete([]byte("k")); err != nil {
		t.Fatalf("delete k: %v", err)
	}
	if err := store.Put([]byte("y"), []byte("y1")); err != nil {
		t.Fatalf("put y: %v", err)
	}
	_ = waiter.Wait(t)

	sstDir := filepath.Join(dir, "sstables")
	matches, err := filepath.Glob(filepath.Join(sstDir, "sstable-*.sst"))
	if err != nil {
		t.Fatalf("glob sstables: %v", err)
	}
	if len(matches) == 0 {
		t.Fatalf("expected sstable output after compaction")
	}

	opts := sstableconfig.DefaultOptions(sstDir)
	for _, path := range matches {
		table, err := sstable.LoadSSTable(path, opts)
		if err != nil {
			t.Fatalf("load sstable: %v", err)
		}
		if entry, ok := table.Get([]byte("k")); ok {
			_ = table.Close()
			t.Fatalf("expected tombstone dropped, found entry: %+v", entry)
		}
		if err := table.Close(); err != nil {
			t.Fatalf("close sstable: %v", err)
		}
	}
}
