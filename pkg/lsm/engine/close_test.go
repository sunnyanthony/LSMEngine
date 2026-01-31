package engine

import (
	"errors"
	"testing"

	"lsmengine/pkg/lsm/errs"
)

func TestCloseBlocksWrites(t *testing.T) {
	store, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := store.Put([]byte("a"), []byte("b")); !errors.Is(err, errs.ErrClosed) {
		t.Fatalf("expected ErrClosed from Put, got %v", err)
	}
	if err := store.Delete([]byte("a")); !errors.Is(err, errs.ErrClosed) {
		t.Fatalf("expected ErrClosed from Delete, got %v", err)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	store, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	err1 := store.Close()
	err2 := store.Close()
	if err1 != err2 {
		t.Fatalf("expected Close to be idempotent, got %v and %v", err1, err2)
	}
}

func TestSnapshotAfterCloseReturnsNil(t *testing.T) {
	store, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if snap := store.Snapshot(); snap != nil {
		t.Fatalf("expected nil snapshot after close")
	}
}
