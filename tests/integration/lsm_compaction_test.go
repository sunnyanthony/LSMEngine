package integration_test

import (
	"path/filepath"
	"testing"
	"time"

	"lsmengine/pkg/lsm"
)

func TestLSMCompactionMergesTables(t *testing.T) {
	dir := t.TempDir()
	opts := lsm.Options{
		DataDir:               dir,
		MemtableLimit:         6,
		WALSync:               false,
		CompactionL0Threshold: 2,
	}
	store, err := lsm.New(opts)
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	defer store.Close()

	// Enough writes to trigger multiple flushes.
	for _, kv := range [][2]string{
		{"a", "1"},
		{"b", "2"},
		{"c", "3"},
		{"a", "4"},
		{"d", "5"},
		{"e", "6"},
	} {
		if err := store.Put([]byte(kv[0]), []byte(kv[1])); err != nil {
			t.Fatalf("put %s: %v", kv[0], err)
		}
	}

	waitForSSTableCount(t, dir, 1)

	got, ok := store.Get([]byte("a"))
	if !ok || string(got.Value) != "4" {
		t.Fatalf("expected a=4 after compaction, ok=%v val=%q", ok, got.Value)
	}
}

func waitForSSTableCount(t *testing.T, dir string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		matches, err := filepath.Glob(filepath.Join(dir, "sstables", "sstable-*.sst"))
		if err != nil {
			t.Fatalf("glob sstables: %v", err)
		}
		if len(matches) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected %d sstable files within timeout", want)
}
