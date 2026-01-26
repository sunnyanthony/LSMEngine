//go:build test

package integration_test

import (
	"testing"

	"lsmengine/pkg/lsm"
	"lsmengine/tests/integration/helpers"
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
	helpers.WaitForSSTableFiles(t, dir, 1)

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
	helpers.WaitForSSTableFiles(t, dir, 2)

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
	helpers.WaitForSSTableFiles(t, dir, 1)

	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	helpers.RemoveWALFiles(t, dir)

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
