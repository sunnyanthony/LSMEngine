//go:build test

package integration_test

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"lsmengine/pkg/lsm"
	"lsmengine/tests/integration/helpers"
)

func TestLSMReplayFlushesWhenMemtableLimitReached(t *testing.T) {
	dir := t.TempDir()
	store, err := lsm.New(lsm.Options{
		DataDir:               dir,
		MemtableLimit:         1 << 20,
		WALSync:               false,
		CompactionL0Threshold: 0,
	})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}

	val := bytes.Repeat([]byte("v"), 32)
	for i := 0; i < 120; i++ {
		key := []byte(fmt.Sprintf("k%03d", i))
		if err := store.Put(key, val); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	matches, err := filepath.Glob(filepath.Join(dir, "sstables", "sstable-*.sst"))
	if err != nil {
		t.Fatalf("glob sstables: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("expected no sstables before replay, found %d", len(matches))
	}

	reopened, err := lsm.New(lsm.Options{
		DataDir:               dir,
		MemtableLimit:         128,
		WALSync:               false,
		CompactionL0Threshold: 0,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened: %v", err)
		}
	})

	helpers.WaitForSSTableFiles(t, dir, 1)

	if got, ok := reopened.Get([]byte("k000")); !ok || !bytes.Equal(got.Value, val) {
		t.Fatalf("expected k000 replayed, ok=%v val=%q", ok, got.Value)
	}
	if got, ok := reopened.Get([]byte("k119")); !ok || !bytes.Equal(got.Value, val) {
		t.Fatalf("expected k119 replayed, ok=%v val=%q", ok, got.Value)
	}
}
