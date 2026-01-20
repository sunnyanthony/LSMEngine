package integration_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"lsmengine/pkg/lsm"
	"lsmengine/pkg/lsm/errs"
)

func TestLSMWALMissingSegmentPolicyError(t *testing.T) {
	dir := t.TempDir()
	createMissingWALSegments(t, dir)

	policy := lsm.MissingSegmentError
	if _, err := lsm.New(lsm.Options{
		DataDir:                 dir,
		WALMissingSegmentPolicy: &policy,
	}); err == nil || !errors.Is(err, errs.ErrWALMissingSegment) {
		t.Fatalf("expected missing segment error, got %v", err)
	}
}

func TestLSMWALMissingSegmentPolicyIgnore(t *testing.T) {
	dir := t.TempDir()
	createMissingWALSegments(t, dir)

	policy := lsm.MissingSegmentIgnore
	store, err := lsm.New(lsm.Options{
		DataDir:                 dir,
		WALMissingSegmentPolicy: &policy,
	})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	defer store.Close()

	if got, ok := store.Get([]byte("alpha")); !ok || string(got.Value) != "one" {
		t.Fatalf("expected replayed alpha=one, ok=%v val=%q", ok, got.Value)
	}
}

func TestLSMWALCorruptSegmentPolicyError(t *testing.T) {
	dir := t.TempDir()
	createCorruptWAL(t, dir)

	autoRepair := false
	if _, err := lsm.New(lsm.Options{
		DataDir:       dir,
		WALAutoRepair: &autoRepair,
	}); err == nil || !errors.Is(err, errs.ErrWALCorruptSegment) {
		t.Fatalf("expected corrupt segment error, got %v", err)
	}
}

func TestLSMWALCorruptSegmentPolicyAutoRepair(t *testing.T) {
	dir := t.TempDir()
	createCorruptWAL(t, dir)

	autoRepair := true
	store, err := lsm.New(lsm.Options{
		DataDir:       dir,
		WALAutoRepair: &autoRepair,
	})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	defer store.Close()

	if err := store.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put after repair: %v", err)
	}
}

func createMissingWALSegments(t *testing.T, dir string) {
	t.Helper()

	store, err := lsm.New(lsm.Options{DataDir: dir, WALSync: false})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	walPath := filepath.Join(dir, "wal.log")
	if err := os.Rename(walPath, walPath+".1"); err != nil {
		t.Fatalf("rename wal: %v", err)
	}

	store, err = lsm.New(lsm.Options{DataDir: dir, WALSync: false})
	if err != nil {
		t.Fatalf("new lsm 2: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close 2: %v", err)
	}
	if err := os.Rename(walPath, walPath+".3"); err != nil {
		t.Fatalf("rename wal 2: %v", err)
	}
}

func createCorruptWAL(t *testing.T, dir string) {
	t.Helper()

	store, err := lsm.New(lsm.Options{DataDir: dir, WALSync: false})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	path := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	if _, err := f.WriteAt([]byte{0x00, 0x00, 0x00, 0x00}, 0); err != nil {
		_ = f.Close()
		t.Fatalf("corrupt wal: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}
}
