package integration_test

import (
	"testing"

	"lsmengine/pkg/lsm"
)

func TestLSMManifestCheckpointWritten(t *testing.T) {
	dir := t.TempDir()
	store, err := lsm.New(lsm.Options{
		DataDir:                 dir,
		MemtableLimit:           4,
		WALSync:                 false,
		ManifestCheckpointEvery: 1,
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
	waitForManifest(t, dir, 1, 1)

	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := lsm.New(lsm.Options{DataDir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	if got, ok := reopened.Get([]byte("a")); !ok || string(got.Value) != "1" {
		t.Fatalf("expected a=1 after reopen, ok=%v val=%q", ok, got.Value)
	}
}
