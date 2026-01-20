//go:build test

package integration_test

import (
	"bytes"
	"testing"

	"lsmengine/pkg/lsm"
)

func TestLSMSnapshotSurvivesCompaction(t *testing.T) {
	dir := t.TempDir()
	store, err := lsm.New(lsm.Options{
		DataDir:               dir,
		MemtableLimit:         4,
		WALSync:               false,
		CompactionL0Threshold: 2,
	})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	defer store.Close()

	if err := store.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("put k: %v", err)
	}
	if err := store.Put([]byte("x"), []byte("x1")); err != nil {
		t.Fatalf("put x: %v", err)
	}
	waitForSSTableFiles(t, dir, 1)

	snap := store.Snapshot()
	defer snap.Close()

	waiter := startCompactionWait(t)
	if err := store.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("put k2: %v", err)
	}
	if err := store.Delete([]byte("x")); err != nil {
		t.Fatalf("delete x: %v", err)
	}
	if err := store.Put([]byte("y"), []byte("y1")); err != nil {
		t.Fatalf("put y: %v", err)
	}
	_ = waiter.Wait(t)

	gotSnapK, ok := snap.Get([]byte("k"))
	if !ok || !bytes.Equal(gotSnapK.Value, []byte("v1")) {
		t.Fatalf("snapshot expected k=v1, ok=%v val=%q", ok, gotSnapK.Value)
	}
	gotSnapX, ok := snap.Get([]byte("x"))
	if !ok || !bytes.Equal(gotSnapX.Value, []byte("x1")) {
		t.Fatalf("snapshot expected x=x1, ok=%v val=%q", ok, gotSnapX.Value)
	}
	if got, ok := snap.Get([]byte("y")); ok || !got.Tombstone {
		t.Fatalf("snapshot expected y missing, ok=%v entry=%+v", ok, got)
	}

	curK, ok := store.Get([]byte("k"))
	if !ok || !bytes.Equal(curK.Value, []byte("v2")) {
		t.Fatalf("current expected k=v2, ok=%v val=%q", ok, curK.Value)
	}
	if curX, ok := store.Get([]byte("x")); ok || !curX.Tombstone {
		t.Fatalf("current expected x deleted, ok=%v entry=%+v", ok, curX)
	}
	if curY, ok := store.Get([]byte("y")); !ok || !bytes.Equal(curY.Value, []byte("y1")) {
		t.Fatalf("current expected y=y1, ok=%v val=%q", ok, curY.Value)
	}
}
