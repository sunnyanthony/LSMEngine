package lsm

import (
	"bytes"
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
	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Delete([]byte("beta")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close lsm: %v", err)
	}

	// Sanity: wal file exists.
	if _, err := os.Stat(filepath.Join(dir, "wal.log")); err != nil {
		t.Fatalf("wal missing: %v", err)
	}

	// Reopen and ensure state is restored via WAL replay.
	store2, err := New(opts)
	if err != nil {
		t.Fatalf("new lsm reopen: %v", err)
	}
	t.Cleanup(func() {
		if err := store2.Close(); err != nil {
			t.Errorf("close lsm reopen: %v", err)
		}
	})

	if got, ok := store2.Get([]byte("alpha")); !ok || !bytes.Equal(got.Value, []byte("one")) {
		t.Fatalf("replayed alpha missing: %+v ok=%v", got, ok)
	}
	if got, ok := store2.Get([]byte("beta")); ok {
		t.Fatalf("expected tombstone to return ok=false, got %+v", got)
	} else if !got.Tombstone {
		t.Fatalf("expected tombstone for beta, got %+v", got)
	}
}
