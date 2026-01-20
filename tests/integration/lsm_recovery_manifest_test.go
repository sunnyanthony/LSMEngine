//go:build test

package integration_test

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"lsmengine/internal/lsm/bootstrap"
	"lsmengine/pkg/lsm"
)

func TestLSMRecoveryFallbackScanWhenManifestMissing(t *testing.T) {
	dir := t.TempDir()
	store, err := lsm.New(lsm.Options{
		DataDir:       dir,
		MemtableLimit: 4,
		WALSync:       false,
	})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}

	if err := store.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if err := store.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("put b: %v", err)
	}
	waitForSSTableFiles(t, dir, 1)
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	removeManifestFiles(t, dir)
	waiter := startManifestFallbackWait(t)

	reopened, err := lsm.New(lsm.Options{DataDir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	waiter.Wait(t)

	if got, ok := reopened.Get([]byte("a")); !ok || string(got.Value) != "1" {
		t.Fatalf("expected a=1 after fallback, ok=%v val=%q", ok, got.Value)
	}
	if got, ok := reopened.Get([]byte("b")); !ok || string(got.Value) != "2" {
		t.Fatalf("expected b=2 after fallback, ok=%v val=%q", ok, got.Value)
	}
}

func TestLSMRecoveryFallbackScanWhenManifestCorrupt(t *testing.T) {
	dir := t.TempDir()
	store, err := lsm.New(lsm.Options{
		DataDir:       dir,
		MemtableLimit: 4,
		WALSync:       false,
	})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}

	if err := store.Put([]byte("x"), []byte("9")); err != nil {
		t.Fatalf("put x: %v", err)
	}
	if err := store.Put([]byte("y"), []byte("8")); err != nil {
		t.Fatalf("put y: %v", err)
	}
	waitForSSTableFiles(t, dir, 1)
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	manifestPath := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(manifestPath, []byte("{corrupt"), 0o644); err != nil {
		t.Fatalf("corrupt manifest: %v", err)
	}

	waiter := startManifestFallbackWait(t)
	reopened, err := lsm.New(lsm.Options{DataDir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	waiter.Wait(t)

	if got, ok := reopened.Get([]byte("x")); !ok || string(got.Value) != "9" {
		t.Fatalf("expected x=9 after fallback, ok=%v val=%q", ok, got.Value)
	}
	if got, ok := reopened.Get([]byte("y")); !ok || string(got.Value) != "8" {
		t.Fatalf("expected y=8 after fallback, ok=%v val=%q", ok, got.Value)
	}
}

func TestLSMRecoveryWALOnlyWhenManifestUnreadable(t *testing.T) {
	dir := t.TempDir()
	store, err := lsm.New(lsm.Options{
		DataDir:       dir,
		MemtableLimit: 1 << 20,
		WALSync:       false,
	})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	if err := store.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("put k1: %v", err)
	}
	if err := store.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("put k2: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	matches, err := filepath.Glob(filepath.Join(dir, "sstables", "sstable-*.sst"))
	if err != nil {
		t.Fatalf("glob sstables: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("expected no sstables, found %d", len(matches))
	}

	manifestPath := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(manifestPath, []byte("locked"), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := os.Chmod(manifestPath, 0); err != nil {
		t.Fatalf("chmod manifest: %v", err)
	}

	reopened, err := lsm.New(lsm.Options{
		DataDir:       dir,
		MemtableLimit: 1 << 20,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	if got, ok := reopened.Get([]byte("k1")); !ok || string(got.Value) != "v1" {
		t.Fatalf("expected k1=v1 after WAL replay, ok=%v val=%q", ok, got.Value)
	}
	if got, ok := reopened.Get([]byte("k2")); !ok || string(got.Value) != "v2" {
		t.Fatalf("expected k2=v2 after WAL replay, ok=%v val=%q", ok, got.Value)
	}
}

func TestLSMRecoveryFallbackScanWhenManifestMissingTable(t *testing.T) {
	dir := t.TempDir()
	store, err := lsm.New(lsm.Options{
		DataDir:               dir,
		MemtableLimit:         4,
		WALSync:               false,
		CompactionL0Threshold: 0,
	})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}

	if err := store.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if err := store.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("put b: %v", err)
	}
	waitForSSTableFiles(t, dir, 1)
	if err := store.Put([]byte("c"), []byte("3")); err != nil {
		t.Fatalf("put c: %v", err)
	}
	if err := store.Put([]byte("d"), []byte("4")); err != nil {
		t.Fatalf("put d: %v", err)
	}
	waitForSSTableFiles(t, dir, 2)
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	paths, err := filepath.Glob(filepath.Join(dir, "sstables", "sstable-*.sst"))
	if err != nil {
		t.Fatalf("glob sstables: %v", err)
	}
	if len(paths) < 2 {
		t.Fatalf("expected >=2 sstables, got %d", len(paths))
	}
	sort.Slice(paths, func(i, j int) bool {
		return parseSeqMax(paths[i]) < parseSeqMax(paths[j])
	})
	if err := os.Remove(paths[0]); err != nil {
		t.Fatalf("remove sstable: %v", err)
	}

	waiter := startManifestFallbackWait(t)
	reopened, err := lsm.New(lsm.Options{DataDir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	waiter.Wait(t)

	if got, ok := reopened.Get([]byte("a")); ok || got.Tombstone {
		t.Fatalf("expected a missing after fallback, ok=%v entry=%+v", ok, got)
	}
	if got, ok := reopened.Get([]byte("b")); ok || got.Tombstone {
		t.Fatalf("expected b missing after fallback, ok=%v entry=%+v", ok, got)
	}
	if got, ok := reopened.Get([]byte("c")); !ok || string(got.Value) != "3" {
		t.Fatalf("expected c=3 after fallback, ok=%v val=%q", ok, got.Value)
	}
	if got, ok := reopened.Get([]byte("d")); !ok || string(got.Value) != "4" {
		t.Fatalf("expected d=4 after fallback, ok=%v val=%q", ok, got.Value)
	}
}

type manifestFallbackWaiter struct {
	ch chan struct{}
}

func startManifestFallbackWait(t *testing.T) *manifestFallbackWaiter {
	t.Helper()
	waiter := &manifestFallbackWaiter{ch: make(chan struct{}, 1)}
	bootstrap.SetTestHooks(&bootstrap.TestHooks{
		BeforeFallbackScan: func() {
			select {
			case waiter.ch <- struct{}{}:
			default:
			}
		},
	})
	t.Cleanup(func() {
		bootstrap.SetTestHooks(nil)
	})
	return waiter
}

func (w *manifestFallbackWaiter) Wait(t *testing.T) {
	t.Helper()
	select {
	case <-w.ch:
		return
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for manifest fallback scan")
	}
}

func removeManifestFiles(t *testing.T, dir string) {
	t.Helper()
	paths := []string{
		filepath.Join(dir, "manifest.log"),
		filepath.Join(dir, "manifest.json"),
	}
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			t.Fatalf("remove manifest %s: %v", path, err)
		}
	}
}

func parseSeqMax(path string) uint64 {
	base := filepath.Base(path)
	base = strings.TrimPrefix(base, "sstable-")
	parts := strings.SplitN(base, "-", 2)
	if len(parts) == 0 {
		return 0
	}
	seq, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0
	}
	return seq
}
