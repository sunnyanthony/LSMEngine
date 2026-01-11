package engine

import (
	"lsmengine/internal/lsm/memtable"
	"lsmengine/pkg/lsm/bus"
	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/types"
)

func (l *LSM) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return errs.ErrWALEmptyKey
	}
	if len(value) == 0 {
		return errs.ErrWALEmptyValue
	}
	term, err := l.termForWrite()
	if err != nil {
		return err
	}
	if l.shouldThrottleWrite(len(key) + len(value)) {
		return errs.ErrBackpressure
	}
	mem := l.activeMem()
	entry := types.Entry{
		Key:   key,
		Value: value,
		Seq:   l.nextSeq(),
	}
	entry = l.prepareEntry(mem, entry)
	if err := l.wal.AppendOwned(entry); err != nil {
		return err
	}
	l.applyEntryOwned(mem, entry)
	if mem.Size() >= l.mtLimit {
		l.triggerFlush()
	}
	if l.bus != nil {
		l.bus.Publish(bus.Event{Type: bus.EventWalAppended, Sequence: entry.Seq})
	}
	l.enqueueReplication(entry, term)
	return nil
}

func (l *LSM) Delete(key []byte) error {
	if len(key) == 0 {
		return errs.ErrWALEmptyKey
	}
	term, err := l.termForWrite()
	if err != nil {
		return err
	}
	if l.shouldThrottleWrite(len(key)) {
		return errs.ErrBackpressure
	}
	mem := l.activeMem()
	entry := types.Entry{
		Key:       key,
		Tombstone: true,
		Seq:       l.nextSeq(),
	}
	entry = l.prepareEntry(mem, entry)
	if err := l.wal.AppendOwned(entry); err != nil {
		return err
	}
	l.applyEntryOwned(mem, entry)
	if mem.Size() >= l.mtLimit {
		l.triggerFlush()
	}
	if l.bus != nil {
		l.bus.Publish(bus.Event{Type: bus.EventWalAppended, Sequence: entry.Seq})
	}
	l.enqueueReplication(entry, term)
	return nil
}

func (l *LSM) triggerFlush() {
	frozen := l.swapMemtable()
	if frozen == nil {
		return
	}
	l.enqueueFlush(frozen)
}

func (l *LSM) enqueueFlush(table memtable.Table) {
	entries := entriesFromTable(table)
	if len(entries) == 0 {
		l.removeImmutable(table)
		l.recycleMemtable(table)
		return
	}
	l.memMu.Lock()
	l.flushQueue = append(l.flushQueue, table)
	l.memMu.Unlock()
	if l.dispatch == nil {
		return
	}
	if l.dispatch.Enqueue(entries) {
		return
	}
	l.flushBlocked.Store(true)
	go l.enqueueFlushBlocking(entries)
}

func (l *LSM) enqueueFlushBlocking(entries []types.Entry) {
	if l.dispatch == nil || l.ctx == nil {
		return
	}
	if l.dispatch.EnqueueBlocking(l.ctx, entries) {
		l.flushBlocked.Store(false)
		return
	}
	l.flushBlocked.Store(false)
}

func (l *LSM) shouldThrottleWrite(delta int) bool {
	if l == nil || l.dispatch == nil || l.mtLimit <= 0 {
		return false
	}
	if l.flushBlocked.Load() {
		return true
	}
	mem := l.activeMem()
	if mem == nil {
		return false
	}
	if mem.Size()+delta < l.mtLimit {
		return false
	}
	return !l.dispatch.CanEnqueue()
}
