//go:build test

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	sstableconfig "lsmengine/internal/lsm/sstable/config"
	"lsmengine/pkg/lsm"
	"lsmengine/tests/integration/helpers"
)

func TestLSMRecoveryDropsCorruptSSTableWhenConfigured(t *testing.T) {
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
	helpers.WaitForSSTableFiles(t, dir, 1)
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	matches, err := filepath.Glob(filepath.Join(dir, "sstables", "sstable-*.sst"))
	if err != nil {
		t.Fatalf("glob sstables: %v", err)
	}
	if len(matches) == 0 {
		t.Fatalf("expected at least one sstable")
	}
	path := matches[0]
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat sstable: %v", err)
	}
	if info.Size() < 2 {
		t.Fatalf("sstable too small to corrupt, size=%d", info.Size())
	}
	if err := os.Truncate(path, info.Size()-1); err != nil {
		t.Fatalf("truncate sstable: %v", err)
	}

	removeManifestFiles(t, dir)
	helpers.RemoveWALFiles(t, dir)

	policy := lsm.SSTableCorruptionDropTable
	override := sstableconfig.PolicySnapshot{CorruptionPolicy: sstableconfig.CorruptionDropTable}
	reopened, err := lsm.New(lsm.Options{
		DataDir: dir,
		SSTable: &lsm.SSTableOptions{
			CorruptionPolicy: &policy,
		},
		SSTablePolicyOverride: &override,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened: %v", err)
		}
	})

	if got, ok := reopened.Get([]byte("a")); ok || got.Tombstone {
		t.Fatalf("expected a missing after dropping corrupt sstable, ok=%v entry=%+v", ok, got)
	}
	if got, ok := reopened.Get([]byte("b")); ok || got.Tombstone {
		t.Fatalf("expected b missing after dropping corrupt sstable, ok=%v entry=%+v", ok, got)
	}
}
