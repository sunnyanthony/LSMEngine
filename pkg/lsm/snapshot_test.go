package lsm_test

import (
	"bytes"
	"testing"

	"lsmengine/pkg/lsm"
)

func TestSnapshotGetSeesFrozenValue(t *testing.T) {
	dir := t.TempDir()
	store, err := lsm.New(lsm.Options{DataDir: dir, MemtableLimit: 1024})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}

	if err := store.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	snap := store.Snapshot()

	if err := store.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, ok := snap.Get([]byte("k"))
	if !ok || !bytes.Equal(got.Value, []byte("v1")) {
		t.Fatalf("expected snapshot value v1, got %+v (ok=%v)", got, ok)
	}
	current, ok := store.Get([]byte("k"))
	if !ok || !bytes.Equal(current.Value, []byte("v2")) {
		t.Fatalf("expected current value v2, got %+v (ok=%v)", current, ok)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_ = snap.Close()
}

func TestSnapshotRangeStable(t *testing.T) {
	dir := t.TempDir()
	store, err := lsm.New(lsm.Options{DataDir: dir, MemtableLimit: 1024})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}

	if err := store.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("put: %v", err)
	}
	snap := store.Snapshot()

	if err := store.Put([]byte("c"), []byte("3")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Delete([]byte("b")); err != nil {
		t.Fatalf("delete: %v", err)
	}

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
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}
	if !bytes.Equal(keys[0], []byte("a")) || !bytes.Equal(keys[1], []byte("b")) {
		t.Fatalf("unexpected key order: %q %q", keys[0], keys[1])
	}
	if !bytes.Equal(vals[0], []byte("1")) || !bytes.Equal(vals[1], []byte("2")) {
		t.Fatalf("unexpected values: %q %q", vals[0], vals[1])
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_ = snap.Close()
}

func TestSnapshotRangeDedupPrefersNewest(t *testing.T) {
	dir := t.TempDir()
	store, err := lsm.New(lsm.Options{DataDir: dir, MemtableLimit: 1024})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}

	if err := store.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	snap1 := store.Snapshot()

	if err := store.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Put([]byte("x"), []byte("x1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	snap2 := store.Snapshot()

	it := snap2.Range(nil, nil)
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
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}
	if !bytes.Equal(keys[0], []byte("k")) || !bytes.Equal(keys[1], []byte("x")) {
		t.Fatalf("unexpected key order: %q %q", keys[0], keys[1])
	}
	if !bytes.Equal(vals[0], []byte("v2")) || !bytes.Equal(vals[1], []byte("x1")) {
		t.Fatalf("unexpected values: %q %q", vals[0], vals[1])
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_ = snap2.Close()
	_ = snap1.Close()
}

func TestSnapshotRangeSkipsTombstone(t *testing.T) {
	dir := t.TempDir()
	store, err := lsm.New(lsm.Options{DataDir: dir, MemtableLimit: 1024})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}

	if err := store.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	snap1 := store.Snapshot()

	if err := store.Delete([]byte("k")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	snap2 := store.Snapshot()

	it := snap2.Range(nil, nil)
	for it.Next() {
		entry := it.Entry()
		if bytes.Equal(entry.Key, []byte("k")) {
			t.Fatalf("expected tombstone to hide key")
		}
	}
	if err := it.Err(); err != nil {
		t.Fatalf("range err: %v", err)
	}

	got, ok := snap1.Get([]byte("k"))
	if !ok || !bytes.Equal(got.Value, []byte("v1")) {
		t.Fatalf("expected snapshot1 to retain v1, got %+v (ok=%v)", got, ok)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_ = snap2.Close()
	_ = snap1.Close()
}
