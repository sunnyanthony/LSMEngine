// In-memory table registry and resolve logic.

package tableset

import (
	"fmt"
	"sort"
	"sync"

	"lsmengine/internal/lsm/metadata"
	"lsmengine/internal/lsm/sstable"
)

// Table ties table metadata to a loaded SSTable handle.
type Table struct {
	Meta   metadata.TableMeta
	Handle sstable.SSTable
}

// Edit describes a batch update to the table set.
type Edit struct {
	Add        []Table
	RemovePath []string
}

// Set holds the in-memory table list with metadata for control-plane planning.
type Set struct {
	mu      sync.RWMutex
	tables  []Table
	index   map[string]int
	pinned  map[string]int
	pending map[string]Table
}

// NewSet builds a new table set from an initial slice.
func NewSet(tables []Table) *Set {
	s := &Set{
		tables: nil,
		index:  make(map[string]int),
	}
	if len(tables) > 0 {
		s.tables = append([]Table(nil), tables...)
		s.rebuild()
	}
	return s
}

// Snapshot returns a copy of the metadata in read-order (newest first).
func (s *Set) Snapshot() []metadata.TableMeta {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]metadata.TableMeta, 0, len(s.tables))
	for _, t := range s.tables {
		out = append(out, t.Meta.Copy())
	}
	return out
}

// Tables returns a copy of the table handles in read-order (newest first).
func (s *Set) Tables() []sstable.SSTable {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]sstable.SSTable, 0, len(s.tables))
	for _, t := range s.tables {
		out = append(out, t.Handle)
	}
	return out
}

// TablesWithMeta returns a copy of the table handles with metadata.
func (s *Set) TablesWithMeta() []Table {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Table, 0, len(s.tables))
	for _, t := range s.tables {
		t.Meta = t.Meta.Copy()
		out = append(out, t)
	}
	return out
}

// SnapshotAndPin returns the current tables and pins them for snapshot use.
func (s *Set) SnapshotAndPin() []Table {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pinned == nil {
		s.pinned = make(map[string]int)
	}
	out := make([]Table, 0, len(s.tables))
	for _, t := range s.tables {
		t.Meta = t.Meta.Copy()
		out = append(out, t)
		s.pinned[t.Meta.Path]++
	}
	return out
}

// Unpin releases snapshot pins and returns tables pending cleanup.
func (s *Set) Unpin(paths []string) []Table {
	if s == nil || len(paths) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pinned == nil {
		return nil
	}
	var ready []Table
	for _, path := range paths {
		count := s.pinned[path]
		if count <= 1 {
			delete(s.pinned, path)
			if s.pending != nil {
				if table, ok := s.pending[path]; ok {
					table.Meta = table.Meta.Copy()
					ready = append(ready, table)
					delete(s.pending, path)
				}
			}
			continue
		}
		s.pinned[path] = count - 1
	}
	return ready
}

// Pending returns tables that are removed but still pinned.
func (s *Set) Pending() []Table {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.pending) == 0 {
		return nil
	}
	out := make([]Table, 0, len(s.pending))
	for _, t := range s.pending {
		t.Meta = t.Meta.Copy()
		out = append(out, t)
	}
	return out
}

// Resolve converts metadata references into loaded table handles.
func (s *Set) Resolve(metas []metadata.TableMeta) ([]sstable.SSTable, error) {
	if s == nil {
		return nil, fmt.Errorf("tableset: nil set")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]sstable.SSTable, 0, len(metas))
	for _, meta := range metas {
		idx, ok := s.index[meta.Path]
		if !ok {
			return nil, fmt.Errorf("tableset: missing table %s", meta.Path)
		}
		out = append(out, s.tables[idx].Handle)
	}
	return out, nil
}

// Apply applies a batch edit to the table set.
func (s *Set) Apply(edit Edit) []Table {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var removed []Table
	if len(edit.RemovePath) > 0 {
		remove := make(map[string]struct{}, len(edit.RemovePath))
		for _, path := range edit.RemovePath {
			remove[path] = struct{}{}
		}
		dst := s.tables[:0]
		for _, t := range s.tables {
			if _, ok := remove[t.Meta.Path]; ok {
				if s.pinned != nil && s.pinned[t.Meta.Path] > 0 {
					if s.pending == nil {
						s.pending = make(map[string]Table)
					}
					s.pending[t.Meta.Path] = t
					continue
				}
				removed = append(removed, t)
				continue
			}
			dst = append(dst, t)
		}
		s.tables = dst
	}

	if len(edit.Add) > 0 {
		for _, t := range edit.Add {
			t.Meta = t.Meta.Copy()
			s.tables = append(s.tables, t)
		}
	}

	s.rebuild()
	return removed
}

func (s *Set) rebuild() {
	s.index = make(map[string]int, len(s.tables))
	sort.Slice(s.tables, func(i, j int) bool {
		if s.tables[i].Meta.SeqMax != s.tables[j].Meta.SeqMax {
			return s.tables[i].Meta.SeqMax > s.tables[j].Meta.SeqMax
		}
		return s.tables[i].Meta.Path < s.tables[j].Meta.Path
	})
	for i := range s.tables {
		s.index[s.tables[i].Meta.Path] = i
	}
}
