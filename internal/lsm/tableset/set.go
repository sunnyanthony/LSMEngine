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
	mu     sync.RWMutex
	tables []Table
	index  map[string]int
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
func (s *Set) Apply(edit Edit) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(edit.RemovePath) > 0 {
		remove := make(map[string]struct{}, len(edit.RemovePath))
		for _, path := range edit.RemovePath {
			remove[path] = struct{}{}
		}
		dst := s.tables[:0]
		for _, t := range s.tables {
			if _, ok := remove[t.Meta.Path]; ok {
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
