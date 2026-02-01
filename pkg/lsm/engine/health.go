// Stats and health snapshots for monitoring.

package engine

import (
	"sync/atomic"

	memtable "lsmengine/internal/lsm/memtable"
)

// Stats describes a point-in-time view of engine activity.
type Stats struct {
	MemtableBytes     int
	MemtableEntries   int
	ImmutableCount    int
	ImmutableBytes    int
	FlushQueueDepth   int
	PinnedCount       int
	TableCount        int
	Seq               uint64
	Closing           bool
	Closed            bool
	FlushBlocked      bool
	CompactionEnabled bool
}

// Health summarizes whether the engine is ready to serve traffic.
type Health struct {
	Ready  bool
	Reason string
}

// Stats returns a snapshot of current engine state.
func (l *LSM) Stats() Stats {
	if l == nil {
		return Stats{}
	}
	out := Stats{
		Seq:               atomic.LoadUint64(&l.seq),
		Closing:           l.closing.Load(),
		Closed:            l.closed.Load(),
		FlushBlocked:      l.flushBlocked.Load(),
		CompactionEnabled: l.compactionSvc != nil,
	}

	l.memMu.RLock()
	mem := l.mem
	immutables := append([]memtable.Table(nil), l.immutables...)
	out.ImmutableCount = len(immutables)
	out.FlushQueueDepth = len(l.flushQueue)
	out.PinnedCount = len(l.pinned)
	l.memMu.RUnlock()

	if mem != nil {
		out.MemtableBytes, out.MemtableEntries = tableStats(mem)
	}
	for _, table := range immutables {
		bytes, _ := tableStats(table)
		out.ImmutableBytes += bytes
	}

	if l.tables != nil {
		out.TableCount = len(l.tables.Tables())
	}
	return out
}

// Health returns a coarse readiness signal based on engine state.
func (l *LSM) Health() Health {
	if l == nil {
		return Health{Ready: false, Reason: "nil"}
	}
	if l.closed.Load() {
		return Health{Ready: false, Reason: "closed"}
	}
	if l.closing.Load() {
		return Health{Ready: false, Reason: "closing"}
	}
	if l.flushBlocked.Load() {
		return Health{Ready: false, Reason: "backpressure"}
	}
	return Health{Ready: true, Reason: "ok"}
}

func tableStats(table memtable.Table) (bytes int, entries int) {
	if table == nil {
		return 0, 0
	}
	if provider, ok := table.(memtable.StatsProvider); ok {
		stats := provider.Stats()
		return stats.Bytes, stats.Entries
	}
	return table.Size(), 0
}
