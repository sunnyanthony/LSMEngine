//go:build test

package integration_test

import (
	"testing"

	"lsmengine/pkg/lsm"
	"lsmengine/tests/integration/helpers"
)

func TestLSMReplayWithoutFlush(t *testing.T) {
	dir := t.TempDir()
	opts := lsm.Options{
		DataDir:       dir,
		MemtableLimit: 100,
		WALSync:       true,
	}
	store, err := lsm.New(opts)
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Put([]byte("beta"), []byte("two")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := lsm.New(lsm.Options{DataDir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened: %v", err)
		}
	})
	if got, ok := reopened.Get([]byte("alpha")); !ok || string(got.Value) != "one" {
		t.Fatalf("replay get alpha: ok=%v val=%q", ok, got.Value)
	}
	if got, ok := reopened.Get([]byte("beta")); !ok || string(got.Value) != "two" {
		t.Fatalf("replay get beta: ok=%v val=%q", ok, got.Value)
	}
}

func TestLSMFlowMetricsNonZero(t *testing.T) {
	dir := t.TempDir()
	opts := lsm.Options{
		DataDir:       dir,
		MemtableLimit: 2,
		WALSync:       false,
	}
	store, err := lsm.New(opts)
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	if err := store.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("put: %v", err)
	}
	helpers.WaitForSSTableFiles(t, dir, 1)
	helpers.WaitForManifest(t, dir, 1, 1)
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := lsm.New(lsm.Options{DataDir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened: %v", err)
		}
	})
	if _, ok := reopened.Get([]byte("a")); !ok {
		t.Fatalf("expected key in sstable")
	}
	snap := reopened.FlowMetrics()
	if snap.CacheHit+snap.CacheMiss+snap.FilterPass+snap.Errors == 0 {
		t.Fatalf("expected metrics to record events, got %+v", snap)
	}
}
