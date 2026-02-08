package engine

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"lsmengine/internal/lsm/iofs"
)

type closableFS struct {
	iofs.FS
	closed atomic.Bool
}

func (c *closableFS) Close() error {
	c.closed.Store(true)
	return nil
}

func TestLSMClosesIOFS(t *testing.T) {
	dir := t.TempDir()
	base := iofs.OSFS{}
	fs := &closableFS{FS: base}
	store, err := New(Options{
		DataDir: dir,
		IOFS:    fs,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := store.Put([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !fs.closed.Load() {
		t.Fatalf("expected IOFS to be closed")
	}
	// Ensure we did not delete data dir while closing.
	if _, err := os.Stat(filepath.Join(dir, "wal.log")); err != nil && !os.IsNotExist(err) {
		t.Fatalf("stat wal: %v", err)
	}
}
