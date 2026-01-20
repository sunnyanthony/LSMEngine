//go:build test

package integration_test

import (
	"testing"

	"lsmengine/pkg/lsm"
)

func TestLSMCompactionMergesTables(t *testing.T) {
	dir := t.TempDir()
	opts := lsm.Options{
		DataDir:               dir,
		MemtableLimit:         6,
		WALSync:               false,
		CompactionL0Threshold: 2,
	}
	store, err := lsm.New(opts)
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	defer store.Close()

	// Enough writes to trigger multiple flushes.
	waiter := startCompactionWait(t)
	for _, kv := range [][2]string{
		{"a", "1"},
		{"b", "2"},
		{"c", "3"},
		{"a", "4"},
		{"d", "5"},
		{"e", "6"},
	} {
		if err := store.Put([]byte(kv[0]), []byte(kv[1])); err != nil {
			t.Fatalf("put %s: %v", kv[0], err)
		}
	}

	result := waiter.Wait(t)
	if len(result.Obsolete) < 2 {
		t.Fatalf("expected at least 2 obsolete tables, got %d", len(result.Obsolete))
	}

	got, ok := store.Get([]byte("a"))
	if !ok || string(got.Value) != "4" {
		t.Fatalf("expected a=4 after compaction, ok=%v val=%q", ok, got.Value)
	}
}
