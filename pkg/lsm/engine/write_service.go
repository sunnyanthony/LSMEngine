// Write path service implementation.

package engine

import (
	"context"

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
	if s.l.control != nil {
		if err := s.l.control.allowWrite(key); err != nil {
			s.l.notifyWriteEvent("put", key, 0, "failed", err)
			return err
		}
	}

	seq, err := s.commitPut(key, value)
	if err != nil {
		s.l.notifyWriteEvent("put", key, seq, "failed", err)
		return err
	}
	s.l.recordCDCEvent("put", key, value, seq, false)
	s.l.notifyWriteEvent("put", key, seq, "committed", nil)
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
	if s.l.control != nil {
		if err := s.l.control.allowWrite(key); err != nil {
			s.l.notifyWriteEvent("delete", key, 0, "failed", err)
			return err
		}
	}

	seq, err := s.commitDelete(key)
	if err != nil {
		s.l.notifyWriteEvent("delete", key, seq, "failed", err)
		return err
	}
	s.l.recordCDCEvent("delete", key, nil, seq, true)
	s.l.notifyWriteEvent("delete", key, seq, "committed", nil)
	return nil
}

func (s *writeService) commitPut(key []byte, value []byte) (uint64, error) {
	if s.l == nil || s.l.commitLog == nil {
		return 0, errs.ErrBackpressure
	}
	entry, err := s.l.commitLog.CommitData(context.Background(), dataMutation{
		Kind:  "put",
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), value...),
	})
	if err != nil {
		return 0, err
	}
	return s.applyCommittedData(entry)
}

func (s *writeService) commitDelete(key []byte) (uint64, error) {
	if s.l == nil || s.l.commitLog == nil {
		return 0, errs.ErrBackpressure
	}
	entry, err := s.l.commitLog.CommitData(context.Background(), dataMutation{
		Kind: "delete",
		Key:  append([]byte(nil), key...),
	})
	if err != nil {
		return 0, err
	}
	return s.applyCommittedData(entry)
}

func (s *writeService) applyCommittedData(entry dataCommittedEntry) (uint64, error) {
	s.l.commitApplyMu.Lock()
	defer s.l.commitApplyMu.Unlock()
	return s.applyCommittedDataLocked(entry)
}

func (s *writeService) applyCommittedDataLocked(entry dataCommittedEntry) (uint64, error) {
	if entry.Commit.Index == 0 || entry.Commit.Term == 0 || entry.Seq == 0 {
		return 0, errs.ErrBackpressure
	}
	if s.l.shouldSkipCommittedEntryLocked(entry.Commit.Index) {
		return entry.Seq, nil
	}
	var (
		seq uint64
		err error
	)
	switch entry.Mutation.Kind {
	case "put":
		seq, err = s.appendPutToLocalStore(entry.Mutation.Key, entry.Mutation.Value, entry.Seq)
	case "delete":
		seq, err = s.appendDeleteToLocalStore(entry.Mutation.Key, entry.Seq)
	default:
		return entry.Seq, errs.ErrBackpressure
	}
	if err != nil {
		return seq, err
	}
	s.l.markCommitLogAppliedLocked(entry.Commit.Index)
	return seq, nil
}

func (s *writeService) appendPutToLocalStore(key []byte, value []byte, seq uint64) (uint64, error) {
	mem, err := s.acquireMemForWrite(len(key) + len(value))
	if err != nil {
		return 0, err
	}

	builder := s.l.entryBuilder(mem)
	entry := builder.Build(key, value, false, seq)
	if err := s.l.wal.AppendOwned(entry); err != nil {
		mem.DecWriter()
		return entry.Seq, err
	}
	s.l.observeCommittedSeq(entry.Seq)
	s.l.applyEntryOwned(mem, entry)
	shouldFlush := mem.Size() >= s.l.mtLimit
	mem.DecWriter()
	if shouldFlush {
		s.triggerFlush(mem)
	}
	if s.l.bus != nil {
		s.l.bus.Publish(bus.Event{Type: bus.EventWalAppended, Sequence: entry.Seq})
	}
	return entry.Seq, nil
}

func (s *writeService) appendDeleteToLocalStore(key []byte, seq uint64) (uint64, error) {
	mem, err := s.acquireMemForWrite(len(key))
	if err != nil {
		return 0, err
	}

	builder := s.l.entryBuilder(mem)
	entry := builder.Build(key, nil, true, seq)
	if err := s.l.wal.AppendOwned(entry); err != nil {
		mem.DecWriter()
		return entry.Seq, err
	}
	s.l.observeCommittedSeq(entry.Seq)
	s.l.applyEntryOwned(mem, entry)
	shouldFlush := mem.Size() >= s.l.mtLimit
	mem.DecWriter()
	if shouldFlush {
		s.triggerFlush(mem)
	}
	if s.l.bus != nil {
		s.l.bus.Publish(bus.Event{Type: bus.EventWalAppended, Sequence: entry.Seq})
	}
	return entry.Seq, nil
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
