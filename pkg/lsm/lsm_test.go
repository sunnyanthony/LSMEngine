package lsm

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/memtable"
	"lsmengine/pkg/lsm/types"
	"lsmengine/pkg/lsm/wal"
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
	store, err := New(Options{DataDir: dir})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	defer store.Close()

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
	opts := Options{
		DataDir:       dir,
		MemtableLimit: 10,
		MemtableKind:  memtable.KindMap,
		WALSync:       false,
	}
	store, err := New(opts)
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	defer store.Close()

	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if got, ok := store.Get([]byte("alpha")); !ok || !bytes.Equal(got.Value, []byte("one")) {
		t.Fatalf("get alpha failed: %+v ok=%v", got, ok)
	}
}

func TestLSMMemtableKindInvalid(t *testing.T) {
	dir := t.TempDir()
	_, err := New(Options{
		DataDir:      dir,
		MemtableKind: "nope",
	})
	if err == nil {
		t.Fatalf("expected error for invalid memtable kind")
	}
}

func TestLSMPutEmptyKeyRejected(t *testing.T) {
	dir := t.TempDir()
	store, err := New(Options{DataDir: dir})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	defer store.Close()

	if err := store.Put(nil, []byte("v")); err == nil {
		t.Fatalf("expected error for empty key")
	} else if !errors.Is(err, errs.ErrWALEmptyKey) {
		t.Fatalf("expected empty key error, got %v", err)
	}
}

func TestLSMPutEmptyValueRejected(t *testing.T) {
	dir := t.TempDir()
	store, err := New(Options{DataDir: dir})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	defer store.Close()

	if err := store.Put([]byte("k"), nil); err == nil {
		t.Fatalf("expected error for empty value")
	} else if !errors.Is(err, errs.ErrWALEmptyValue) {
		t.Fatalf("expected empty value error, got %v", err)
	}
}

func TestLSMDeleteEmptyKeyRejected(t *testing.T) {
	dir := t.TempDir()
	store, err := New(Options{DataDir: dir})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	defer store.Close()

	if err := store.Delete(nil); err == nil {
		t.Fatalf("expected error for empty key")
	} else if !errors.Is(err, errs.ErrWALEmptyKey) {
		t.Fatalf("expected empty key error, got %v", err)
	}
}

func TestLSMPutCopiesInput(t *testing.T) {
	dir := t.TempDir()
	store, err := New(Options{DataDir: dir, WALSync: true})
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

	w, err := wal.NewWAL(wal.Options{Path: filepath.Join(dir, "wal.log"), Sync: false})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	defer w.Close()
	var replayed []types.Entry
	if err := w.Replay(func(e types.Entry) error {
		replayed = append(replayed, e)
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(replayed) != 1 {
		t.Fatalf("expected 1 entry replayed, got %d", len(replayed))
	}
	if !bytes.Equal(replayed[0].Key, wantKey) || !bytes.Equal(replayed[0].Value, wantVal) {
		t.Fatalf("expected copied replay, got key=%q value=%q", replayed[0].Key, replayed[0].Value)
	}
}
