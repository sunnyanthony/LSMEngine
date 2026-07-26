// Flush path service implementation.

package engine

import (
	"context"
	"fmt"
	"sync/atomic"

	memtable "lsmengine/internal/lsm/memtable"
	"lsmengine/internal/lsm/sstable"
	"lsmengine/internal/lsm/tableedit"
	"lsmengine/internal/lsm/tableset"
	"lsmengine/pkg/lsm/types"
)

type flushService struct {
	l *LSM
}

func newFlushService(l *LSM) *flushService {
	return &flushService{l: l}
}

func (s *flushService) enqueue(table memtable.Table) {
	if table == nil {
		return
	}
	ctx := s.l.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := table.WaitWriters(ctx); err != nil {
		return
	}
	s.l.memRetireMu.Lock()
	s.l.memMu.RLock()
	immutable, queued := false, false
	for _, pending := range s.l.immutables {
		immutable = immutable || pending == table
	}
	for _, pending := range s.l.flushQueue {
		queued = queued || pending == table
	}
	s.l.memMu.RUnlock()
	if !immutable || queued {
		s.l.memRetireMu.Unlock()
		return
	}
	entries := entriesFromTable(table)
	if len(entries) == 0 {
		s.l.removeImmutable(table)
		s.l.recycleMemtable(table)
		s.l.memRetireMu.Unlock()
		return
	}
	s.l.memMu.Lock()
	s.l.flushQueue = append(s.l.flushQueue, table)
	s.l.memMu.Unlock()
	s.l.memRetireMu.Unlock()
	if s.l.dispatch == nil {
		return
	}
	complete := func(t sstable.SSTable) { s.onFlushTable(t, table) }
	if s.l.dispatch.EnqueueWithCallback(entries, complete) {
		return
	}
	s.l.flushBlocked.Store(true)
	go s.enqueueBlocking(entries, complete)
}

func (s *flushService) enqueueBlocking(entries []types.Entry, complete func(sstable.SSTable)) {
	if s.l.dispatch == nil || s.l.ctx == nil {
		return
	}
	if s.l.dispatch.EnqueueBlockingWithCallback(s.l.ctx, entries, complete) {
		s.l.flushBlocked.Store(false)
		return
	}
	s.l.flushBlocked.Store(false)
}

// onFlush applies a newly flushed table to the table set and manifest.
func (s *flushService) onFlushTable(t sstable.SSTable, flushed memtable.Table) {
	s.l.commitApplyMu.Lock()
	defer s.l.commitApplyMu.Unlock()
	checkpoint, err := s.checkpointForFlush(t.Seq, flushed)
	if err != nil {
		_ = t.Close()
		if s.l.logger != nil {
			s.l.logger.Printf("flush checkpoint: %v", err)
		}
		return
	}
	meta := tableedit.TableMetaFromSSTable(t, 0)
	add := []tableset.Table{{Meta: meta, Handle: t}}
	if err := s.editService().Apply(add, nil, checkpoint); err != nil {
		if s.l.logger != nil {
			s.l.logger.Printf("flush apply: %v", err)
		}
		return
	}
	s.l.updateLastFlush(checkpoint)
	s.l.retireMemtable(flushed)
	s.l.pruneArchivedWALSegments(checkpoint)
	if s.l.compactionSvc != nil {
		s.l.compactionSvc.Trigger()
	}
}

// Called with commitApplyMu held so new mutations cannot cross the sampled prefix.
func (s *flushService) checkpointForFlush(seq uint64, flushed memtable.Table) (uint64, error) {
	m, err := s.l.manifest.Load()
	if err != nil {
		return 0, err
	}
	checkpoint := seq
	for _, table := range m.Tables {
		if table.SeqMax > checkpoint {
			checkpoint = table.SeqMax
		}
	}
	s.l.memMu.RLock()
	defer s.l.memMu.RUnlock()
	found := false
	for _, table := range s.l.flushQueue {
		if table == flushed {
			found = true
			break
		}
	}
	if !found {
		return 0, fmt.Errorf("unknown flushed memtable at sequence %d", seq)
	}
	pending := append([]memtable.Table{s.l.mem}, s.l.immutables...)
	for _, table := range pending {
		if table == nil || table == flushed {
			continue
		}
		min, _ := memtableSequenceBounds(table)
		if min > 0 && min <= checkpoint {
			checkpoint = min - 1
		}
	}
	if checkpoint < m.WALSeq {
		return 0, fmt.Errorf("unflushed sequence below durable checkpoint %d", m.WALSeq)
	}
	return checkpoint, nil
}

func memtableSequenceBounds(table memtable.Table) (min, max uint64) {
	it := table.Iter()
	for it.Next() {
		seq := it.Entry().Seq
		if min == 0 || seq < min {
			min = seq
		}
		if seq > max {
			max = seq
		}
	}
	return min, max
}

func (s *flushService) editService() tableedit.Editor {
	if s.l == nil {
		return nil
	}
	return s.l.tableEditor()
}

func (l *LSM) updateLastFlush(seq uint64) {
	if l == nil || seq == 0 {
		return
	}
	for {
		last := atomic.LoadUint64(&l.lastFlush)
		if seq <= last {
			return
		}
		if atomic.CompareAndSwapUint64(&l.lastFlush, last, seq) {
			return
		}
	}
}

func (l *LSM) pruneArchivedWALSegments(checkpoint uint64) {
	if l == nil || l.wal == nil || l.walRetainArchivedSegments <= 0 || checkpoint == 0 {
		return
	}
	stats, err := l.wal.PruneArchivedSegments(checkpoint, l.walRetainArchivedSegments)
	if err != nil {
		if l.logger != nil {
			l.logger.Printf("wal retention: %v", err)
		}
		return
	}
	if stats.RemovedSegments > 0 && l.logger != nil {
		l.logger.Printf(
			"wal retention: removed=%d retained=%d pruned_through=%d",
			stats.RemovedSegments,
			stats.RetainedSegments,
			stats.PrunedThrough,
		)
	}
}
