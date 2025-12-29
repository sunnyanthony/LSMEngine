package lsm

import (
	"os"
	"path/filepath"
	"testing"
)

// Ensures WAL replay restores state after restart.
func TestLSMWALReplay(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		DataDir:       dir,
		MemtableLimit: 64,
		WALSync:       true,
	}

	store, err := New(opts)
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	if err := store.Put("alpha", []byte("one")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Delete("beta"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	store.Close()

	// Sanity: wal file exists.
	if _, err := os.Stat(filepath.Join(dir, "wal.log")); err != nil {
		t.Fatalf("wal missing: %v", err)
	}

	// Reopen and ensure state is restored via WAL replay.
	store2, err := New(opts)
	if err != nil {
		t.Fatalf("new lsm reopen: %v", err)
	}
	defer store2.Close()

	if got, ok := store2.Get("alpha"); !ok || string(got.Value) != "one" {
		t.Fatalf("replayed alpha missing: %+v ok=%v", got, ok)
	}
	if got, ok := store2.Get("beta"); ok && !got.Tombstone {
		t.Fatalf("expected tombstone for beta, got %+v", got)
	}
}
