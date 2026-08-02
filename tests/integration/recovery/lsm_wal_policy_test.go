//go:build test

package integration_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"lsmengine/internal/lsm/wal/codec"
	"lsmengine/pkg/lsm"
	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/types"
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
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

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
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	if err := store.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put after repair: %v", err)
	}
}

func TestLSMWALReplayResyncsAfterCorruptMiddleBlock(t *testing.T) {
	dir := t.TempDir()
	createWALWithCorruptMiddleBlock(t, dir)

	autoRepair := true
	store, err := lsm.New(lsm.Options{
		DataDir:       dir,
		WALAutoRepair: &autoRepair,
	})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	if got, ok := store.Get([]byte("alpha")); !ok || string(got.Value) != "one" {
		t.Fatalf("expected replayed alpha=one, ok=%v val=%q", ok, got.Value)
	}
	if _, ok := store.Get([]byte("beta")); ok {
		t.Fatalf("expected corrupt beta block to be skipped")
	}
	if got, ok := store.Get([]byte("gamma")); !ok || string(got.Value) != "three" {
		t.Fatalf("expected resynced gamma=three, ok=%v val=%q", ok, got.Value)
	}
}

func TestLSMWALAutoRepairPersistsTailTruncation(t *testing.T) {
	dir := t.TempDir()
	createWALWithTruncatedTail(t, dir)

	autoRepair := true
	store, err := lsm.New(lsm.Options{
		DataDir:       dir,
		WALAutoRepair: &autoRepair,
	})
	if err != nil {
		t.Fatalf("new lsm with auto repair: %v", err)
	}
	if got, ok := store.Get([]byte("stable")); !ok || string(got.Value) != "one" {
		t.Fatalf("expected stable=one after repair, ok=%v val=%q", ok, got.Value)
	}
	if _, ok := store.Get([]byte("tail")); ok {
		t.Fatalf("expected truncated tail record to be absent")
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close repaired store: %v", err)
	}

	autoRepair = false
	restarted, err := lsm.New(lsm.Options{
		DataDir:       dir,
		WALAutoRepair: &autoRepair,
	})
	if err != nil {
		t.Fatalf("expected repaired wal to reopen without auto repair: %v", err)
	}
	defer restarted.Close()
	if got, ok := restarted.Get([]byte("stable")); !ok || string(got.Value) != "one" {
		t.Fatalf("expected restarted stable=one, ok=%v val=%q", ok, got.Value)
	}
	if _, ok := restarted.Get([]byte("tail")); ok {
		t.Fatalf("expected restarted tail record to remain absent")
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

func createWALWithCorruptMiddleBlock(t *testing.T, dir string) {
	t.Helper()

	first := walBlock(t, types.Entry{Key: []byte("alpha"), Value: []byte("one"), Seq: 1})
	corrupt := walBlock(t, types.Entry{Key: []byte("beta"), Value: []byte("two"), Seq: 2})
	corrupt[8] ^= 0xff
	third := walBlock(t, types.Entry{Key: []byte("gamma"), Value: []byte("three"), Seq: 3})

	writeRawWAL(t, dir, first, corrupt, third)
}

func createWALWithTruncatedTail(t *testing.T, dir string) {
	t.Helper()

	first := walBlock(t, types.Entry{Key: []byte("stable"), Value: []byte("one"), Seq: 1})
	tail := walBlock(t, types.Entry{Key: []byte("tail"), Value: []byte("two"), Seq: 2})
	tail = tail[:len(tail)-4]

	writeRawWAL(t, dir, first, tail)
}

func writeRawWAL(t *testing.T, dir string, blocks ...[]byte) {
	t.Helper()

	buf := bytes.NewBuffer(nil)
	if _, err := codec.WriteSegmentHeader(buf, 64*1024, 1); err != nil {
		t.Fatalf("write segment header: %v", err)
	}
	for _, block := range blocks {
		buf.Write(block)
	}
	if err := os.WriteFile(filepath.Join(dir, "wal.log"), buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write wal: %v", err)
	}
}

func walBlock(t *testing.T, entry types.Entry) []byte {
	t.Helper()

	buf := bytes.NewBuffer(nil)
	if _, err := codec.WriteBlock(buf, []codec.RecordBuffer{codec.NewRecordBuffer(entry)}); err != nil {
		t.Fatalf("write block: %v", err)
	}
	return append([]byte(nil), buf.Bytes()...)
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
		if cerr := f.Close(); cerr != nil {
			t.Errorf("close wal: %v", cerr)
		}
		t.Fatalf("corrupt wal: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}
}
