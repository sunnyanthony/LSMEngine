//go:build test

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	"lsmengine/pkg/lsm"
	"lsmengine/tests/integration/helpers"
)

func TestLSMRecoveryHandlesTruncatedManifestCheckpoint(t *testing.T) {
	dir := t.TempDir()
	store, err := lsm.New(lsm.Options{
		DataDir:                 dir,
		MemtableLimit:           4,
		WALSync:                 false,
		ManifestCheckpointEvery: 1000,
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
	helpers.WaitForSSTableFiles(t, dir, 1)

	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	helpers.RemoveWALFiles(t, dir)

	manifestPath := filepath.Join(dir, "manifest.json")
	info, err := os.Stat(manifestPath)
	if err != nil {
		t.Fatalf("stat manifest: %v", err)
	}
	if info.Size() < 2 {
		t.Fatalf("expected manifest to contain data, size=%d", info.Size())
	}
	if err := os.Truncate(manifestPath, info.Size()-1); err != nil {
		t.Fatalf("truncate manifest: %v", err)
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

	if got, ok := reopened.Get([]byte("a")); !ok || string(got.Value) != "1" {
		t.Fatalf("expected a=1 after recovery, ok=%v val=%q", ok, got.Value)
	}
	if got, ok := reopened.Get([]byte("b")); !ok || string(got.Value) != "2" {
		t.Fatalf("expected b=2 after recovery, ok=%v val=%q", ok, got.Value)
	}
}
