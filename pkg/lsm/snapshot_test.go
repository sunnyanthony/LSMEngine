package lsm_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

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
	if err := store.Put([]byte("y"), []byte("y1")); err != nil {
		t.Fatalf("put y: %v", err)
	}

	got, ok := snap.Get([]byte("k"))
	if !ok || !bytes.Equal(got.Value, []byte("v1")) {
		t.Fatalf("expected snapshot value v1, got %+v (ok=%v)", got, ok)
	}
	if got, ok := snap.Get([]byte("y")); ok || got.Tombstone {
		t.Fatalf("expected snapshot to miss y, ok=%v entry=%+v", ok, got)
	}
	current, ok := store.Get([]byte("k"))
	if !ok || !bytes.Equal(current.Value, []byte("v2")) {
		t.Fatalf("expected current value v2, got %+v (ok=%v)", current, ok)
	}
	if got, ok := store.Get([]byte("y")); !ok || !bytes.Equal(got.Value, []byte("y1")) {
		t.Fatalf("expected current y1, got %+v (ok=%v)", got, ok)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	closeSnap(t, snap)
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
	closeSnap(t, snap)
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
	closeSnap(t, snap2)
	closeSnap(t, snap1)
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
	if got, ok := snap2.Get([]byte("k")); ok || !got.Tombstone {
		t.Fatalf("expected snapshot2 tombstone, ok=%v entry=%+v", ok, got)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	closeSnap(t, snap2)
	closeSnap(t, snap1)
}

func TestSnapshotRangeIncludesSSTable(t *testing.T) {
	dir := t.TempDir()
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	opts := lsm.Options{
		DataDir:       dir,
		MemtableLimit: 4,
		WALSync:       false,
	}
	store, err := lsm.New(opts)
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	if err := store.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Put([]byte("b"), []byte("22")); err != nil {
		t.Fatalf("put: %v", err)
	}
	waitForSSTables(t, dir, 1)

	if err := store.Put([]byte("c"), []byte("3")); err != nil {
		t.Fatalf("put: %v", err)
	}
	snap := store.Snapshot()
	t.Cleanup(func() {
		if err := snap.Close(); err != nil {
			t.Errorf("close snapshot: %v", err)
		}
	})

	it := snap.Range(nil, nil)
	var keys [][]byte
	for it.Next() {
		keys = append(keys, it.Entry().Key)
	}
	if err := it.Err(); err != nil {
		t.Fatalf("range err: %v", err)
	}
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}
	if !bytes.Equal(keys[0], []byte("a")) || !bytes.Equal(keys[1], []byte("b")) || !bytes.Equal(keys[2], []byte("c")) {
		t.Fatalf("unexpected order: %q %q %q", keys[0], keys[1], keys[2])
	}
	if got, ok := snap.Get([]byte("a")); !ok || !bytes.Equal(got.Value, []byte("1")) {
		t.Fatalf("expected snapshot a=1, ok=%v val=%q", ok, got.Value)
	}
	if got, ok := snap.Get([]byte("b")); !ok || !bytes.Equal(got.Value, []byte("22")) {
		t.Fatalf("expected snapshot b=22, ok=%v val=%q", ok, got.Value)
	}
	if got, ok := snap.Get([]byte("c")); !ok || !bytes.Equal(got.Value, []byte("3")) {
		t.Fatalf("expected snapshot c=3, ok=%v val=%q", ok, got.Value)
	}
}

func TestSnapshotRangeMemtableOverridesSSTable(t *testing.T) {
	dir := t.TempDir()
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	opts := lsm.Options{
		DataDir:       dir,
		MemtableLimit: 4,
		WALSync:       false,
	}
	store, err := lsm.New(opts)
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	if err := store.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Put([]byte("b"), []byte("22")); err != nil {
		t.Fatalf("put: %v", err)
	}
	waitForSSTables(t, dir, 1)

	if err := store.Put([]byte("a"), []byte("9")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Delete([]byte("b")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	snap := store.Snapshot()
	t.Cleanup(func() {
		if err := snap.Close(); err != nil {
			t.Errorf("close snapshot: %v", err)
		}
	})

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
	if len(keys) != 1 {
		t.Fatalf("expected 1 key, got %d", len(keys))
	}
	if !bytes.Equal(keys[0], []byte("a")) || !bytes.Equal(vals[0], []byte("9")) {
		t.Fatalf("expected updated value, got %q=%q", keys[0], vals[0])
	}
	if got, ok := snap.Get([]byte("b")); ok || !got.Tombstone {
		t.Fatalf("expected snapshot b tombstone, ok=%v entry=%+v", ok, got)
	}
}

func waitForSSTables(t *testing.T, dir string, want int) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		matches, err := filepath.Glob(filepath.Join(dir, "sstables", "sstable-*.sst"))
		if err != nil {
			t.Fatalf("glob sstables: %v", err)
		}
		if len(matches) >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected %d sstable files within timeout", want)
}

func closeSnap(t *testing.T, snap interface{ Close() error }) {
	t.Helper()
	if err := snap.Close(); err != nil {
		t.Errorf("close snapshot: %v", err)
	}
}
