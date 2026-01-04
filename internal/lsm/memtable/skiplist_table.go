package memtable

import (
	"sync"

	"lsmengine/internal/lsm/memtable/skiplist"
	"lsmengine/pkg/lsm/types"
)

// SkipListTable stores entries in a skiplist for ordered iteration.
type SkipListTable struct {
	mu        sync.RWMutex
	list      *skiplist.SkipList
	seq       uint64
	cmp       Compare
	sizeBytes int
	arena     *Arena
}

func NewSkipListTable() Table {
	return NewSkipListTableWithArena(DefaultArenaBlockSize)
}

func NewSkipListTableWithArena(blockSize int) Table {
	return newSkipListTable(blockSize)
}

func newSkipListTable(blockSize int) *SkipListTable {
	return &SkipListTable{
		list:  skiplist.New(),
		cmp:   DefaultCompare,
		arena: NewArena(blockSize),
	}
}

// ApplyOwned inserts an entry without copying key/value.
func (t *SkipListTable) ApplyOwned(entry types.Entry) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if entry.Seq > t.seq {
		t.seq = entry.Seq
	}
	inserted, prev, replaced := t.list.Upsert(entry)
	if inserted {
		t.sizeBytes += entrySize(entry)
	} else if replaced {
		t.sizeBytes += entrySize(entry) - entrySize(prev)
	}
}

// CopyEntry copies key/value into the table-owned arena without inserting.
func (t *SkipListTable) CopyEntry(entry types.Entry) types.Entry {
	return t.copyEntry(entry)
}

// ApplyBatchOwned applies entries without copying key/value.
func (t *SkipListTable) ApplyBatchOwned(entries []types.Entry) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, entry := range entries {
		if entry.Seq > t.seq {
			t.seq = entry.Seq
		}
		inserted, prev, replaced := t.list.Upsert(entry)
		if inserted {
			t.sizeBytes += entrySize(entry)
		} else if replaced {
			t.sizeBytes += entrySize(entry) - entrySize(prev)
		}
	}
}

func (t *SkipListTable) Get(key []byte) (types.Entry, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.list.Get(key)
}

func (t *SkipListTable) Size() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.sizeBytes
}

func (t *SkipListTable) Stats() TableStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return TableStats{
		Entries: t.list.Len(),
		Bytes:   t.sizeBytes,
	}
}

// Drain returns entries in sorted key order and clears the table.
func (t *SkipListTable) Drain() []types.Entry {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.list.Len() == 0 {
		return nil
	}
	out := make([]types.Entry, 0, t.list.Len())
	it := t.list.Iter()
	for it.Next() {
		out = append(out, it.Entry())
	}
	t.list = skiplist.New()
	t.sizeBytes = 0
	return out
}

func (t *SkipListTable) Iter() Iterator {
	return t.Range(nil, nil)
}

func (t *SkipListTable) Range(start, end []byte) Iterator {
	t.mu.RLock()
	if len(start) > 0 && len(end) > 0 && t.cmp(start, end) >= 0 {
		t.mu.RUnlock()
		return newSliceIterator(nil)
	}
	iter := t.list.IterFrom(start)
	var entries []types.Entry
	for iter.Next() {
		entry := iter.Entry()
		if len(end) > 0 && t.cmp(entry.Key, end) >= 0 {
			break
		}
		entries = append(entries, entry)
	}
	t.mu.RUnlock()
	return newSliceIterator(entries)
}

// Freeze marks the table immutable; no-op for skiplist-backed memtables.
func (t *SkipListTable) Freeze() {
}

func (t *SkipListTable) copyEntry(entry types.Entry) types.Entry {
	entry.Key = t.copyBytes(entry.Key)
	entry.Value = t.copyBytes(entry.Value)
	return entry
}

func (t *SkipListTable) copyBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	if t.arena == nil {
		return append([]byte(nil), src...)
	}
	return t.arena.AllocCopy(src)
}
