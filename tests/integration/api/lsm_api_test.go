//go:build test

package integration_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"lsmengine/pkg/lsm"
	"lsmengine/pkg/lsm/errs"
)

func TestLSMPutGetDelete(t *testing.T) {
	dir := t.TempDir()
	opts := lsm.Options{
		DataDir:       dir,
		MemtableLimit: 10,
		WALSync:       false,
	}
	store, err := lsm.New(opts)
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	cleanupCloser(t, "store", store)

	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Delete([]byte("beta")); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if got, ok := store.Get([]byte("alpha")); !ok || !bytes.Equal(got.Value, []byte("one")) {
		t.Fatalf("get alpha failed: %+v ok=%v", got, ok)
	}
	if got, ok := store.Get([]byte("beta")); ok && !got.Tombstone {
		t.Fatalf("expected tombstone for beta, got %+v", got)
	}
	// ensure WAL file exists
	if _, err := os.Stat(filepath.Join(dir, "wal.log")); err != nil {
		t.Fatalf("wal not created: %v", err)
	}
}

func TestLSMGetTombstoneReturnsNotFound(t *testing.T) {
	dir := t.TempDir()
	store, err := lsm.New(lsm.Options{DataDir: dir})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	cleanupCloser(t, "store", store)

	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Delete([]byte("alpha")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	got, ok := store.Get([]byte("alpha"))
	if ok {
		t.Fatalf("expected tombstone to return ok=false")
	}
	if !got.Tombstone {
		t.Fatalf("expected tombstone entry, got %+v", got)
	}
}

func TestLSMMemtableKindMap(t *testing.T) {
	dir := t.TempDir()
	opts := lsm.Options{
		DataDir:       dir,
		MemtableLimit: 10,
		MemtableKind:  lsm.MemtableKindMap,
		WALSync:       false,
	}
	store, err := lsm.New(opts)
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	cleanupCloser(t, "store", store)

	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if got, ok := store.Get([]byte("alpha")); !ok || !bytes.Equal(got.Value, []byte("one")) {
		t.Fatalf("get alpha failed: %+v ok=%v", got, ok)
	}
}

func TestLSMMemtableKindInvalid(t *testing.T) {
	dir := t.TempDir()
	_, err := lsm.New(lsm.Options{
		DataDir:      dir,
		MemtableKind: "nope",
	})
	if err == nil {
		t.Fatalf("expected error for invalid memtable kind")
	}
}

func TestLSMPutEmptyKeyRejected(t *testing.T) {
	dir := t.TempDir()
	store, err := lsm.New(lsm.Options{DataDir: dir})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	cleanupCloser(t, "store", store)

	if err := store.Put(nil, []byte("v")); err == nil {
		t.Fatalf("expected error for empty key")
	} else if !errors.Is(err, errs.ErrWALEmptyKey) {
		t.Fatalf("expected empty key error, got %v", err)
	}
}

func TestLSMPutEmptyValueRejected(t *testing.T) {
	dir := t.TempDir()
	store, err := lsm.New(lsm.Options{DataDir: dir})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	cleanupCloser(t, "store", store)

	if err := store.Put([]byte("k"), nil); err == nil {
		t.Fatalf("expected error for empty value")
	} else if !errors.Is(err, errs.ErrWALEmptyValue) {
		t.Fatalf("expected empty value error, got %v", err)
	}
}

func TestLSMDeleteEmptyKeyRejected(t *testing.T) {
	dir := t.TempDir()
	store, err := lsm.New(lsm.Options{DataDir: dir})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	cleanupCloser(t, "store", store)

	if err := store.Delete(nil); err == nil {
		t.Fatalf("expected error for empty key")
	} else if !errors.Is(err, errs.ErrWALEmptyKey) {
		t.Fatalf("expected empty key error, got %v", err)
	}
}

func TestLSMPutCopiesInput(t *testing.T) {
	dir := t.TempDir()
	store, err := lsm.New(lsm.Options{DataDir: dir, WALSync: true})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	key := []byte("alpha")
	val := []byte("one")
	wantKey := append([]byte(nil), key...)
	wantVal := append([]byte(nil), val...)

	if err := store.Put(key, val); err != nil {
		t.Fatalf("put: %v", err)
	}
	key[0] = 'z'
	val[0] = 'x'

	got, ok := store.Get([]byte("alpha"))
	if !ok {
		t.Fatalf("expected key to exist")
	}
	if !bytes.Equal(got.Value, wantVal) {
		t.Fatalf("expected copied value, got %q", got.Value)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := lsm.New(lsm.Options{DataDir: dir})
	if err != nil {
		t.Fatalf("reopen lsm: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened: %v", err)
		}
	})
	replayed, ok := reopened.Get([]byte("alpha"))
	if !ok {
		t.Fatalf("expected replayed key to exist")
	}
	if !bytes.Equal(replayed.Key, wantKey) || !bytes.Equal(replayed.Value, wantVal) {
		t.Fatalf("expected copied replay, got key=%q value=%q", replayed.Key, replayed.Value)
	}
}

func cleanupCloser(t *testing.T, name string, c interface{ Close() error }) {
	t.Helper()
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("close %s: %v", name, err)
		}
	})
}
