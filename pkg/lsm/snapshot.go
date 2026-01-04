package lsm

import (
	"sync/atomic"

	"lsmengine/internal/lsm/memtable"
	"lsmengine/internal/lsm/sstable"
	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/types"
)

// Snapshot provides a stable view of memtables at a point in time.
type Snapshot struct {
	lsm    *LSM
	mems   []memtable.Table
	tables []sstable.SSTable
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

	l.tablesMu.RLock()
	tables := append([]sstable.SSTable(nil), l.tables...)
	l.tablesMu.RUnlock()

	return &Snapshot{
		lsm:    l,
		mems:   mems,
		tables: tables,
		pinned: frozen,
	}
}

// Close releases the pinned memtable so it can be flushed.
func (s *Snapshot) Close() error {
	if s == nil || s.lsm == nil || s.pinned == nil {
		return nil
	}
	if !atomic.CompareAndSwapUint32(&s.closed, 0, 1) {
		return nil
	}
	s.lsm.releasePinned(s.pinned)
	return nil
}

// Get returns the newest entry visible to the snapshot.
func (s *Snapshot) Get(key []byte) (types.Entry, bool) {
	for _, table := range s.mems {
		if e, ok := table.Get(key); ok {
			return e, !e.Tombstone
		}
	}
	for _, table := range s.tables {
		if e, ok := table.Get(key); ok {
			return e, !e.Tombstone
		}
	}
	return types.Entry{}, false
}

// Range scans keys in [start, end) order. SSTables are not yet supported.
func (s *Snapshot) Range(start, end []byte) Iterator {
	if len(s.tables) > 0 {
		return newErrorIterator(errs.ErrRangeUnsupported)
	}
	if len(s.mems) == 0 {
		return newEmptyIterator()
	}
	iters := make([]memtable.Iterator, 0, len(s.mems))
	for _, table := range s.mems {
		iters = append(iters, table.Range(start, end))
	}
	return newMergeIterator(iters)
}
