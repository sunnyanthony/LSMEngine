// Write path service implementation.

package engine

import (
	memtable "lsmengine/internal/lsm/memtable"
	"lsmengine/pkg/lsm/bus"
	"lsmengine/pkg/lsm/errs"
)

type writeService struct {
	l *LSM
}

func newWriteService(l *LSM) *writeService {
	return &writeService{l: l}
}

func (s *writeService) Put(key []byte, value []byte) error {
	if s.l.isClosing() {
		return errs.ErrClosed
	}
	if len(key) == 0 {
		err := errs.ErrWALEmptyKey
		s.l.notifyWriteEvent("put", key, 0, "failed", err)
		return err
	}
	if len(value) == 0 {
		err := errs.ErrWALEmptyValue
		s.l.notifyWriteEvent("put", key, 0, "failed", err)
		return err
	}
	mem, err := s.acquireMemForWrite(len(key) + len(value))
	if err != nil {
		s.l.notifyWriteEvent("put", key, 0, "failed", err)
		return err
	}

	builder := s.l.entryBuilder(mem)
	entry := builder.Build(key, value, false, s.l.nextSeq())
	if err := s.l.wal.AppendOwned(entry); err != nil {
		mem.DecWriter()
		s.l.notifyWriteEvent("put", key, entry.Seq, "failed", err)
		return err
	}
	s.l.applyEntryOwned(mem, entry)
	shouldFlush := mem.Size() >= s.l.mtLimit
	mem.DecWriter()
	if shouldFlush {
		s.triggerFlush(mem)
	}
	if s.l.bus != nil {
		s.l.bus.Publish(bus.Event{Type: bus.EventWalAppended, Sequence: entry.Seq})
	}
	s.l.notifyWriteEvent("put", key, entry.Seq, "committed", nil)
	return nil
}

func (s *writeService) Delete(key []byte) error {
	if s.l.isClosing() {
		return errs.ErrClosed
	}
	if len(key) == 0 {
		err := errs.ErrWALEmptyKey
		s.l.notifyWriteEvent("delete", key, 0, "failed", err)
		return err
	}
	mem, err := s.acquireMemForWrite(len(key))
	if err != nil {
		s.l.notifyWriteEvent("delete", key, 0, "failed", err)
		return err
	}

	builder := s.l.entryBuilder(mem)
	entry := builder.Build(key, nil, true, s.l.nextSeq())
	if err := s.l.wal.AppendOwned(entry); err != nil {
		mem.DecWriter()
		s.l.notifyWriteEvent("delete", key, entry.Seq, "failed", err)
		return err
	}
	s.l.applyEntryOwned(mem, entry)
	shouldFlush := mem.Size() >= s.l.mtLimit
	mem.DecWriter()
	if shouldFlush {
		s.triggerFlush(mem)
	}
	if s.l.bus != nil {
		s.l.bus.Publish(bus.Event{Type: bus.EventWalAppended, Sequence: entry.Seq})
	}
	s.l.notifyWriteEvent("delete", key, entry.Seq, "committed", nil)
	return nil
}

func (s *writeService) triggerFlush(current memtable.Table) {
	frozen := s.l.freezeMemtableIfCurrent(current)
	if frozen == nil {
		return
	}
	s.flushService().enqueue(frozen)
}

func (s *writeService) flushService() *flushService {
	if s.l.flushSvc == nil {
		s.l.flushSvc = newFlushService(s.l)
	}
	return s.l.flushSvc
}

func (s *writeService) shouldThrottleWriteForMem(mem memtable.Table, delta int) bool {
	if s.l == nil || s.l.dispatch == nil || s.l.mtLimit <= 0 {
		return false
	}
	if s.l.flushBlocked.Load() {
		return true
	}
	if mem == nil {
		return false
	}
	if mem.Size()+delta < s.l.mtLimit {
		return false
	}
	return !s.l.dispatch.CanEnqueue()
}

func (s *writeService) acquireMemForWrite(delta int) (memtable.Table, error) {
	if s.l.isClosing() {
		return nil, errs.ErrClosed
	}
	s.l.memMu.RLock()
	mem := s.l.mem
	if s.shouldThrottleWriteForMem(mem, delta) {
		s.l.memMu.RUnlock()
		return nil, errs.ErrBackpressure
	}
	if mem == nil {
		s.l.memMu.RUnlock()
		return nil, errs.ErrBackpressure
	}
	if err := mem.IncWriter(); err != nil {
		s.l.memMu.RUnlock()
		return nil, err
	}
	s.l.memMu.RUnlock()
	return mem, nil
}
