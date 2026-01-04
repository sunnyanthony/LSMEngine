package lsm

import (
	"sync/atomic"

	"lsmengine/internal/lsm/memtable"
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
	out := make([]memtable.Table, len(l.immutables))
	for i := range l.immutables {
		out[i] = l.immutables[len(l.immutables)-1-i]
	}
	return out
}

func (l *LSM) swapMemtable() memtable.Table {
	l.memMu.Lock()
	defer l.memMu.Unlock()
	return l.freezeMemtableLocked(false)
}

func (l *LSM) freezeMemtableLocked(pinned bool) memtable.Table {
	if l.mem == nil || l.mtFactory == nil {
		return nil
	}
	frozen := l.mem
	l.mem = l.mtFactory()
	if freezer, ok := frozen.(memtable.Freezer); ok {
		freezer.Freeze()
	}
	l.immutables = append(l.immutables, frozen)
	if pinned {
		l.pinMemtableLocked(frozen)
	} else {
		l.flushQueue = append(l.flushQueue, frozen)
	}
	return frozen
}

func (l *LSM) pinMemtableLocked(table memtable.Table) {
	if l.pinned == nil {
		l.pinned = make(map[memtable.Table]int)
	}
	l.pinned[table]++
}

func (l *LSM) unpinMemtableLocked(table memtable.Table) bool {
	if l.pinned == nil {
		return false
	}
	count := l.pinned[table]
	if count <= 1 {
		delete(l.pinned, table)
		return true
	}
	l.pinned[table] = count - 1
	return false
}

func (l *LSM) releasePinned(table memtable.Table) {
	l.memMu.Lock()
	shouldFlush := l.unpinMemtableLocked(table)
	l.memMu.Unlock()
	if shouldFlush {
		if l.ctx != nil {
			select {
			case <-l.ctx.Done():
				return
			default:
			}
		}
		l.enqueueFlush(table)
	}
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
