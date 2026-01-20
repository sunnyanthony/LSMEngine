package integration_test

import (
	"bytes"
	"testing"

	"lsmengine/pkg/lsm"
)

func TestLSMRangeAfterRestartSeesMemtableAndSSTable(t *testing.T) {
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

	reopened, err := lsm.New(lsm.Options{
		DataDir:       dir,
		MemtableLimit: 1024,
		WALSync:       false,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	if err := reopened.Put([]byte("c"), []byte("3")); err != nil {
		t.Fatalf("put c: %v", err)
	}

	snap := reopened.Snapshot()
	defer snap.Close()

	it := snap.Range(nil, nil)
	var keys [][]byte
	var vals [][]byte
	for it.Next() {
		entry := it.Entry()
		keys = append(keys, entry.Key)
		vals = append(vals, entry.Value)
	}
	if err := it.Err(); err != nil {
		t.Fatalf("range err: %v", err)
	}
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}
	if !bytes.Equal(keys[0], []byte("a")) || !bytes.Equal(vals[0], []byte("1")) {
		t.Fatalf("expected a=1, got %q=%q", keys[0], vals[0])
	}
	if !bytes.Equal(keys[1], []byte("b")) || !bytes.Equal(vals[1], []byte("2")) {
		t.Fatalf("expected b=2, got %q=%q", keys[1], vals[1])
	}
	if !bytes.Equal(keys[2], []byte("c")) || !bytes.Equal(vals[2], []byte("3")) {
		t.Fatalf("expected c=3, got %q=%q", keys[2], vals[2])
	}
}
