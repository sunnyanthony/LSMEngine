// Flush path service implementation.

package engine

import (
	"context"

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
	entries := entriesFromTable(table)
	if len(entries) == 0 {
		s.l.removeImmutable(table)
		s.l.recycleMemtable(table)
		return
	}
	s.l.memMu.Lock()
	s.l.flushQueue = append(s.l.flushQueue, table)
	s.l.memMu.Unlock()
	if s.l.dispatch == nil {
		return
	}
	if s.l.dispatch.Enqueue(entries) {
		return
	}
	s.l.flushBlocked.Store(true)
	go s.enqueueBlocking(entries)
}

func (s *flushService) enqueueBlocking(entries []types.Entry) {
	if s.l.dispatch == nil || s.l.ctx == nil {
		return
	}
	if s.l.dispatch.EnqueueBlocking(s.l.ctx, entries) {
		s.l.flushBlocked.Store(false)
		return
	}
	s.l.flushBlocked.Store(false)
}

// onFlush applies a newly flushed table to the table set and manifest.
func (s *flushService) onFlush(t sstable.SSTable) {
	meta := tableedit.TableMetaFromSSTable(t, 0)
	add := []tableset.Table{{Meta: meta, Handle: t}}
	if err := s.editService().Apply(add, nil, t.Seq); err != nil && s.l.logger != nil {
		s.l.logger.Printf("flush apply: %v", err)
	}
	if s.l.compactionSvc != nil {
		s.l.compactionSvc.Trigger()
	}
	flushed := s.l.popFlushedTable()
	s.l.recycleMemtable(flushed)
}

func (s *flushService) editService() tableedit.Editor {
	if s.l == nil {
		return nil
	}
	return s.l.tableEditor()
}
