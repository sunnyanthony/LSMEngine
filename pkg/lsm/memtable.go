package lsm

import (
	"sync/atomic"

	"lsmengine/pkg/lsm/memtable"
)

func (l *LSM) activeMem() memtable.Table {
	l.memMu.RLock()
	mem := l.mem
	l.memMu.RUnlock()
	return mem
}

func (l *LSM) immutableMems() []memtable.Table {
	l.memMu.RLock()
	defer l.memMu.RUnlock()
	if len(l.immutables) == 0 {
		return nil
	}
	return append([]memtable.Table(nil), l.immutables...)
}

func (l *LSM) swapMemtable() memtable.Table {
	l.memMu.Lock()
	defer l.memMu.Unlock()
	if l.mem == nil || l.mtFactory == nil {
		return nil
	}
	frozen := l.mem
	l.mem = l.mtFactory()
	if freezer, ok := frozen.(memtable.Freezer); ok {
		freezer.Freeze()
	}
	l.immutables = append(l.immutables, frozen)
	l.flushQueue = append(l.flushQueue, frozen)
	return frozen
}

func (l *LSM) removeImmutable(table memtable.Table) {
	l.memMu.Lock()
	defer l.memMu.Unlock()
	l.removeImmutableLocked(table)
	l.removeFlushQueueLocked(table)
}

func (l *LSM) removeImmutableLocked(table memtable.Table) {
	for i, t := range l.immutables {
		if t == table {
			l.immutables = append(l.immutables[:i], l.immutables[i+1:]...)
			return
		}
	}
}

func (l *LSM) removeFlushQueueLocked(table memtable.Table) {
	for i, t := range l.flushQueue {
		if t == table {
			l.flushQueue = append(l.flushQueue[:i], l.flushQueue[i+1:]...)
			return
		}
	}
}

func (l *LSM) popFlushed() {
	l.memMu.Lock()
	defer l.memMu.Unlock()
	if len(l.flushQueue) == 0 {
		return
	}
	flushed := l.flushQueue[0]
	l.flushQueue = l.flushQueue[1:]
	l.removeImmutableLocked(flushed)
}

func (l *LSM) bumpSeq(seq uint64) {
	for {
		cur := atomic.LoadUint64(&l.seq)
		if seq <= cur {
			return
		}
		if atomic.CompareAndSwapUint64(&l.seq, cur, seq) {
			return
		}
	}
}
