// Stats and health snapshots for monitoring.

package engine

import (
	"sort"
	"sync/atomic"

	memtable "lsmengine/internal/lsm/memtable"
)

// SSTableLevelStats describes active SSTables at one level, excluding obsolete files.
type SSTableLevelStats struct {
	Level      int
	TableCount int
	SizeBytes  uint64
}

// Stats describes a point-in-time view of engine activity.
type Stats struct {
	MemtableBytes   int
	MemtableEntries int
	ImmutableCount  int
	ImmutableBytes  int
	FlushQueueDepth int
	PinnedCount     int
	TableCount      int
	// SSTableBytes excludes WAL, Raft state, trash, and obsolete pinned tables.
	SSTableBytes      uint64
	SSTableLevels     []SSTableLevelStats
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
		metas := l.tables.Snapshot()
		out.TableCount = len(metas)
		levels := make(map[int]SSTableLevelStats)
		for _, meta := range metas {
			level := levels[meta.Level]
			level.Level = meta.Level
			level.TableCount++
			level.SizeBytes += meta.SizeBytes
			levels[meta.Level] = level
			out.SSTableBytes += meta.SizeBytes
		}
		out.SSTableLevels = make([]SSTableLevelStats, 0, len(levels))
		for _, level := range levels {
			out.SSTableLevels = append(out.SSTableLevels, level)
		}
		sort.Slice(out.SSTableLevels, func(i, j int) bool {
			return out.SSTableLevels[i].Level < out.SSTableLevels[j].Level
		})
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
