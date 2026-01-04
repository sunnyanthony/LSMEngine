package lsm

import (
	"sync/atomic"

	"lsmengine/internal/lsm/manifest"
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
	mem := l.activeMem()
	entry := types.Entry{
		Key:   key,
		Value: value,
		Seq:   atomic.AddUint64(&l.seq, 1),
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
	return nil
}

func (l *LSM) Delete(key []byte) error {
	if len(key) == 0 {
		return errs.ErrWALEmptyKey
	}
	mem := l.activeMem()
	entry := types.Entry{
		Key:       key,
		Tombstone: true,
		Seq:       atomic.AddUint64(&l.seq, 1),
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
	enqueued := l.dispatch.Enqueue(entries)
	l.memMu.Unlock()
	if enqueued {
		return
	}
	// backpressure: flush synchronously
	flushed, err := l.flusher.Flush(entries)
	if err == nil {
		l.appendTable(flushed)
		_ = l.manifest.Save(manifest.Manifest{
			WALSeq: flushed.Seq,
			Tables: append([]manifest.Entry(nil), manifest.Entry{Path: flushed.Path, Seq: flushed.Seq}),
		})
		if l.logger != nil {
			l.logger.Printf("flush (sync) completed seq=%d", flushed.Seq)
		}
	} else if l.logger != nil {
		l.logger.Printf("flush (sync) failed: %v", err)
	}
}
