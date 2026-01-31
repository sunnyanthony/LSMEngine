//go:build test

package integration_test

import (
	"testing"

	"lsmengine/pkg/lsm"
	"lsmengine/tests/integration/helpers"
)

func TestLSMCloseFlushesMemtable(t *testing.T) {
	dir := t.TempDir()
	store, err := lsm.New(lsm.Options{
		DataDir:       dir,
		MemtableLimit: 1 << 20,
		WALSync:       false,
	})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	if err := store.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("put alpha: %v", err)
	}
	if err := store.Put([]byte("beta"), []byte("two")); err != nil {
		t.Fatalf("put beta: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	helpers.RemoveWALFiles(t, dir)

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
		t.Fatalf("expected alpha=one, ok=%v val=%q", ok, got.Value)
	}
	if got, ok := reopened.Get([]byte("beta")); !ok || string(got.Value) != "two" {
		t.Fatalf("expected beta=two, ok=%v val=%q", ok, got.Value)
	}
}
