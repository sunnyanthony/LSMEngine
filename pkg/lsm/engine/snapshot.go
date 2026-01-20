// Snapshot creation and range scan entry points.

package engine

import (
	"sync/atomic"

	memtable "lsmengine/internal/lsm/memtable"
	"lsmengine/internal/lsm/tableset"
	"lsmengine/pkg/lsm/types"
)

// Snapshot provides a stable view of memtables at a point in time.
type Snapshot struct {
	lsm    *LSM
	mems   []memtable.Table
	tables []tableset.Table
	pinned memtable.Table
	closed uint32
}

// Snapshot freezes the current memtable and returns a point-in-time view.
func (l *LSM) Snapshot() *Snapshot {
	l.memMu.Lock()
	frozen := l.freezeMemtableLocked(true)
	immutables := append([]memtable.Table(nil), l.immutables...)
	l.memMu.Unlock()

	mems := make([]memtable.Table, len(immutables))
	for i := range immutables {
		mems[i] = immutables[len(immutables)-1-i]
	}

	var tables []tableset.Table
	if l.tables != nil {
		tables = l.tables.SnapshotAndPin()
	}

	return &Snapshot{
		lsm:    l,
		mems:   mems,
		tables: tables,
		pinned: frozen,
	}
}

// Close releases the pinned memtable so it can be flushed.
func (s *Snapshot) Close() error {
	if s == nil || s.lsm == nil {
		return nil
	}
	if !atomic.CompareAndSwapUint32(&s.closed, 0, 1) {
		return nil
	}
	if s.pinned != nil {
		s.lsm.releasePinned(s.pinned)
	}
	if s.lsm.tables != nil && len(s.tables) > 0 {
		paths := make([]string, 0, len(s.tables))
		for _, t := range s.tables {
			paths = append(paths, t.Meta.Path)
		}
		pending := s.lsm.tables.Unpin(paths)
		s.lsm.cleanupTables(pending)
	}
	return nil
}

// Get returns the newest entry visible to the snapshot.
func (s *Snapshot) Get(key []byte) (types.Entry, bool) {
	for _, table := range s.mems {
		if e, ok := table.Get(key); ok {
			return copyEntry(e), !e.Tombstone
		}
	}
	for _, table := range s.tables {
		if view, ok := table.Handle.GetView(key); ok {
			entry := types.Entry{
				Key:       view.Key,
				Value:     view.Value,
				Tombstone: view.Tombstone,
				Seq:       view.Seq,
			}
			return copyEntry(entry), !view.Tombstone
		}
	}
	return types.Entry{}, false
}

// Range scans keys in [start, end) order across memtables and SSTables.
func (s *Snapshot) Range(start, end []byte) Iterator {
	if len(s.mems) == 0 && len(s.tables) == 0 {
		return newEmptyIterator()
	}
	iters := make([]viewIterator, 0, len(s.mems)+len(s.tables))
	for _, table := range s.mems {
		iters = append(iters, &memtableViewIter{iter: table.Range(start, end)})
	}
	for _, table := range s.tables {
		iters = append(iters, &sstableViewIter{iter: table.Handle.Range(start, end)})
	}
	return newViewToEntryIterator(newViewMergeIterator(iters))
}
