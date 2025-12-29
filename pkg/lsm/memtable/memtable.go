package memtable

import (
	"lsmengine/pkg/lsm/types"
	"sort"
	"sync"
)

// MemTable stores recent writes and tracks size. It keeps a map for fast lookups
// and a keys slice for deterministic ordering when flushing.
type MemTable struct {
	mu      sync.RWMutex
	entries map[string]types.Entry
	keys    []string
	seq     uint64
}

func NewMemTable() *MemTable {
	return &MemTable{
		entries: make(map[string]types.Entry),
	}
}

func (m *MemTable) Put(key string, value []byte) types.Entry {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.seq++
	if _, exists := m.entries[key]; !exists {
		m.keys = append(m.keys, key)
	}
	entry := types.Entry{
		Key:   key,
		Value: append([]byte(nil), value...),
		Seq:   m.seq,
	}
	m.entries[key] = entry
	return entry
}

func (m *MemTable) Delete(key string) types.Entry {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.seq++
	if _, exists := m.entries[key]; !exists {
		m.keys = append(m.keys, key)
	}
	entry := types.Entry{
		Key:       key,
		Tombstone: true,
		Seq:       m.seq,
	}
	m.entries[key] = entry
	return entry
}

func (m *MemTable) Get(key string) (types.Entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.entries[key]
	return entry, ok
}

func (m *MemTable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.entries)
}

// Drain returns entries sorted by key and clears the memtable.
func (m *MemTable) Drain() []types.Entry {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.entries) == 0 {
		return nil
	}

	sort.Strings(m.keys)
	out := make([]types.Entry, 0, len(m.entries))
	for _, k := range m.keys {
		if e, ok := m.entries[k]; ok {
			out = append(out, e)
		}
	}

	m.entries = make(map[string]types.Entry)
	m.keys = m.keys[:0]
	return out
}
