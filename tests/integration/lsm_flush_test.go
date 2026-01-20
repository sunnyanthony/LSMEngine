package integration_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"lsmengine/pkg/lsm"
)

func TestLSMFlushToSSTableAndReload(t *testing.T) {
	dir := t.TempDir()
	opts := lsm.Options{
		DataDir:       dir,
		MemtableLimit: 4,
		WALSync:       false,
	}
	store, err := lsm.New(opts)
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}

	if err := store.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if err := store.Put([]byte("b"), []byte("22")); err != nil {
		t.Fatalf("put b: %v", err)
	}
	waitForSSTableFiles(t, dir, 1)

	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	store, err = lsm.New(lsm.Options{DataDir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	if got, ok := store.Get([]byte("a")); !ok || string(got.Value) != "1" {
		t.Fatalf("reopen get a: ok=%v val=%q", ok, got.Value)
	}
	if got, ok := store.Get([]byte("b")); !ok || string(got.Value) != "22" {
		t.Fatalf("reopen get b: ok=%v val=%q", ok, got.Value)
	}
}

func TestLSMMultiFlushReload(t *testing.T) {
	dir := t.TempDir()
	opts := lsm.Options{
		DataDir:       dir,
		MemtableLimit: 4,
		WALSync:       false,
	}
	store, err := lsm.New(opts)
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}

	for _, kv := range [][2]string{
		{"a", "1"},
		{"b", "2"},
		{"c", "3"},
		{"d", "4"},
	} {
		if err := store.Put([]byte(kv[0]), []byte(kv[1])); err != nil {
			t.Fatalf("put %s: %v", kv[0], err)
		}
	}
	waitForSSTableFiles(t, dir, 2)

	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	store, err = lsm.New(lsm.Options{DataDir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	for _, kv := range [][2]string{
		{"a", "1"},
		{"b", "2"},
		{"c", "3"},
		{"d", "4"},
	} {
		if got, ok := store.Get([]byte(kv[0])); !ok || string(got.Value) != kv[1] {
			t.Fatalf("reopen get %s: ok=%v val=%q", kv[0], ok, got.Value)
		}
	}
}

func TestLSMReadFromSSTableWithoutWAL(t *testing.T) {
	dir := t.TempDir()
	opts := lsm.Options{
		DataDir:       dir,
		MemtableLimit: 4,
		WALSync:       false,
	}
	store, err := lsm.New(opts)
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}

	if err := store.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if err := store.Put([]byte("b"), []byte("22")); err != nil {
		t.Fatalf("put b: %v", err)
	}
	waitForSSTableFiles(t, dir, 1)

	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	removeWALFiles(t, dir)

	store, err = lsm.New(lsm.Options{DataDir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	if got, ok := store.Get([]byte("a")); !ok || string(got.Value) != "1" {
		t.Fatalf("get a: ok=%v val=%q", ok, got.Value)
	}
	if got, ok := store.Get([]byte("b")); !ok || string(got.Value) != "22" {
		t.Fatalf("get b: ok=%v val=%q", ok, got.Value)
	}
	metrics := store.FlowMetrics()
	if metrics.CacheHit+metrics.CacheMiss == 0 {
		t.Fatalf("expected sstable read metrics, got %+v", metrics)
	}
}

func waitForSSTableFiles(t *testing.T, dir string, want int) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		matches, err := filepath.Glob(filepath.Join(dir, "sstables", "sstable-*.sst"))
		if err != nil {
			t.Fatalf("glob sstables: %v", err)
		}
		if len(matches) >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected %d sstable files within timeout", want)
}

func removeWALFiles(t *testing.T, dir string) {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(dir, "wal.log*"))
	if err != nil {
		t.Fatalf("glob wal: %v", err)
	}
	for _, path := range matches {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			t.Fatalf("remove wal %s: %v", path, err)
		}
	}
}
