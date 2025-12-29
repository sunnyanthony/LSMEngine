package lsm

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLSMPutGetDelete(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		DataDir:       dir,
		MemtableLimit: 10,
		WALSync:       false,
	}
	store, err := New(opts)
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	defer store.Close()

	if err := store.Put("alpha", []byte("one")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Delete("beta"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if got, ok := store.Get("alpha"); !ok || string(got.Value) != "one" {
		t.Fatalf("get alpha failed: %+v ok=%v", got, ok)
	}
	if got, ok := store.Get("beta"); ok && !got.Tombstone {
		t.Fatalf("expected tombstone for beta, got %+v", got)
	}
	// ensure WAL file exists
	if _, err := os.Stat(filepath.Join(dir, "wal.log")); err != nil {
		t.Fatalf("wal not created: %v", err)
	}
}
