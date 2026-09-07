package engine

import (
	"testing"

	memtable "lsmengine/internal/lsm/memtable"
	"lsmengine/pkg/lsm/types"
)

func TestFlushCompletionPreservesUnflushedPrefix(t *testing.T) {
	store, err := New(Options{DataDir: t.TempDir(), MemtableLimit: 1 << 20, WALBlockSize: 64, WALMaxSegmentBytes: 64, WALRetainArchivedSegments: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var frozen []memtable.Table
	for _, key := range []string{"a", "b", "c"} {
		if err := store.Put([]byte(key), []byte("value")); err != nil {
			t.Fatal(err)
		}
		frozen = append(frozen, store.freezeMemtableIfCurrent(store.activeMem()))
	}
	store.memMu.Lock()
	store.flushQueue = append(store.flushQueue, frozen...)
	store.memMu.Unlock()
	snap := store.Snapshot()
	defer snap.Close()
	for step, index := range []int{2, 0, 1} {
		table, err := store.flusher.Flush(entriesFromTable(frozen[index]))
		if err != nil {
			t.Fatal(err)
		}
		store.flushSvc.onFlushTable(table, frozen[index])
		m, err := store.manifest.Load()
		if err != nil {
			t.Fatal(err)
		}
		want := []uint64{0, 1, 3}[step]
		if m.WALSeq != want {
			t.Fatalf("completion %d advanced checkpoint to %d, want %d", index, m.WALSeq, want)
		}
		for _, key := range []string{"a", "b", "c"} {
			if entry, ok := snap.Get([]byte(key)); !ok || string(entry.Value) != "value" {
				t.Fatalf("snapshot lost %s after flush %d: %+v", key, index, entry)
			}
		}
		if step == 0 {
			if !store.flushQueued(frozen[0]) || store.flushQueued(frozen[2]) {
				t.Fatal("completion removed the wrong memtable")
			}
			seen := map[uint64]bool{}
			if err := store.wal.Replay(func(e types.Entry) error { seen[e.Seq] = true; return nil }); err != nil {
				t.Fatal(err)
			}
			if !seen[1] || !seen[2] {
				t.Fatalf("WAL needed by unfinished tables was removed: %v", seen)
			}
		}
	}
}

func TestFlushCompletionUsesIdentityForEqualSequences(t *testing.T) {
	store, err := New(Options{DataDir: t.TempDir(), MemtableLimit: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var frozen []memtable.Table
	for _, key := range []string{"a", "b"} {
		mem := store.activeMem()
		mem.ApplyOwned(types.Entry{Key: []byte(key), Seq: 5, Tombstone: true})
		frozen = append(frozen, store.freezeMemtableIfCurrent(mem))
	}
	store.memMu.Lock()
	store.flushQueue = append(store.flushQueue, frozen...)
	store.memMu.Unlock()
	for _, index := range []int{1, 0} {
		table, err := store.flusher.Flush(entriesFromTable(frozen[index]))
		if err != nil {
			t.Fatal(err)
		}
		store.flushSvc.onFlushTable(table, frozen[index])
		if store.flushQueued(frozen[index]) {
			t.Fatal("completed table was not removed by identity")
		}
		if index == 1 && !store.flushQueued(frozen[0]) {
			t.Fatal("equal sequence removed an unfinished table")
		}
	}
}
