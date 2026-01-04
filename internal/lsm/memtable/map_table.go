package memtable

import (
	"bytes"
	"sort"
	"sync"

	"lsmengine/pkg/lsm/types"
)

// MapTable stores recent writes and tracks size. It keeps a map for fast lookups
// and a keys slice for deterministic ordering when flushing.
type MapTable struct {
	mu        sync.RWMutex
	entries   map[uint64][]types.Entry
	keys      [][]byte
	seq       uint64
	cmp       Compare
	sizeBytes int
	arena     *Arena
}

func NewMapTable() Table {
	return NewMapTableWithArena(DefaultArenaBlockSize)
}

func NewMapTableWithArena(blockSize int) Table {
	return newMapTable(blockSize)
}

func newMapTable(blockSize int) *MapTable {
	return &MapTable{
		entries: make(map[uint64][]types.Entry),
		cmp:     DefaultCompare,
		arena:   NewArena(blockSize),
	}
}

// ApplyOwned inserts an entry without copying key/value.
func (m *MapTable) ApplyOwned(entry types.Entry) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if entry.Seq > m.seq {
		m.seq = entry.Seq
	}
	m.updateEntry(entry)
}

// CopyEntry copies key/value into the table-owned arena without inserting.
func (m *MapTable) CopyEntry(entry types.Entry) types.Entry {
	return m.copyEntry(entry)
}

// ApplyBatchOwned applies entries without copying key/value.
func (m *MapTable) ApplyBatchOwned(entries []types.Entry) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, entry := range entries {
		if entry.Seq > m.seq {
			m.seq = entry.Seq
		}
		m.updateEntry(entry)
	}
}

func (m *MapTable) Get(key []byte) (types.Entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.getByKey(key)
}

func (m *MapTable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sizeBytes
}

func (m *MapTable) Stats() TableStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return TableStats{
		Entries: len(m.keys),
		Bytes:   m.sizeBytes,
	}
}

// Drain returns entries sorted by key and clears the memtable.
func (m *MapTable) Drain() []types.Entry {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.entries) == 0 {
		return nil
	}

	sort.Slice(m.keys, func(i, j int) bool {
		return m.cmp(m.keys[i], m.keys[j]) < 0
	})
	out := make([]types.Entry, 0, len(m.keys))
	for _, k := range m.keys {
		if e, ok := m.getByKey(k); ok {
			out = append(out, e)
		}
	}

	m.entries = make(map[uint64][]types.Entry)
	m.keys = m.keys[:0]
	m.sizeBytes = 0
	return out
}

func (m *MapTable) Iter() Iterator {
	return m.Range(nil, nil)
}

func (m *MapTable) Range(start, end []byte) Iterator {
	m.mu.RLock()
	if len(start) > 0 && len(end) > 0 && m.cmp(start, end) >= 0 {
		m.mu.RUnlock()
		return newSliceIterator(nil)
	}
	if len(m.keys) == 0 {
		m.mu.RUnlock()
		return newSliceIterator(nil)
	}
	keys := append([][]byte(nil), m.keys...)
	if len(keys) > 1 {
		sort.Slice(keys, func(i, j int) bool {
			return m.cmp(keys[i], keys[j]) < 0
		})
	}
	entries := make([]types.Entry, 0, len(keys))
	for _, key := range keys {
		if len(start) > 0 && m.cmp(key, start) < 0 {
			continue
		}
		if len(end) > 0 && m.cmp(key, end) >= 0 {
			break
		}
		entry, ok := m.getByKey(key)
		if ok {
			entries = append(entries, entry)
		}
	}
	m.mu.RUnlock()
	return newSliceIterator(entries)
}

// Freeze marks the table immutable; no-op for map-backed memtables.
func (m *MapTable) Freeze() {
	// No-op; keep consistent behavior across memtable types.
}

func (m *MapTable) updateEntry(entry types.Entry) {
	prev, existed := m.setEntry(entry)
	if existed {
		m.sizeBytes += entrySize(entry) - entrySize(prev)
		return
	}
	m.sizeBytes += entrySize(entry)
}

func (m *MapTable) setEntry(entry types.Entry) (types.Entry, bool) {
	hash := hashKey(entry.Key)
	bucket := m.entries[hash]
	for i := range bucket {
		if bytes.Equal(bucket[i].Key, entry.Key) {
			prev := bucket[i]
			bucket[i] = entry
			m.entries[hash] = bucket
			return prev, true
		}
	}
	m.entries[hash] = append(bucket, entry)
	m.keys = append(m.keys, entry.Key)
	return types.Entry{}, false
}

func (m *MapTable) getByKey(key []byte) (types.Entry, bool) {
	hash := hashKey(key)
	bucket := m.entries[hash]
	for i := range bucket {
		if bytes.Equal(bucket[i].Key, key) {
			return bucket[i], true
		}
	}
	return types.Entry{}, false
}

func (m *MapTable) copyEntry(entry types.Entry) types.Entry {
	entry.Key = m.copyBytes(entry.Key)
	entry.Value = m.copyBytes(entry.Value)
	return entry
}

func (m *MapTable) copyBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	if m.arena == nil {
		return append([]byte(nil), src...)
	}
	return m.arena.AllocCopy(src)
}
