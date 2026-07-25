// Stats and health snapshots for monitoring.

package engine

import (
	"sort"
	"sync/atomic"

	memtable "lsmengine/internal/lsm/memtable"
	"lsmengine/internal/lsm/metadata"
)

// SSTableLevelStats describes immutable table pressure for one LSM level.
type SSTableLevelStats struct {
	Level      int
	TableCount int
	SizeBytes  uint64
}

// Stats describes a point-in-time view of engine activity.
type Stats struct {
	MemtableBytes         int
	MemtableEntries       int
	ImmutableCount        int
	ImmutableBytes        int
	FlushQueueDepth       int
	FlushQueueCapacity    int
	PinnedCount           int
	TableCount            int
	SSTableCount          int
	SSTableBytes          uint64
	SSTableLevels         []SSTableLevelStats
	L0TableCount          int
	L0SizeBytes           uint64
	CompactionL0Threshold int
	CompactionPending     bool
	Seq                   uint64
	Closing               bool
	Closed                bool
	FlushBlocked          bool
	CompactionEnabled     bool
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
	out.FlushQueueCapacity = l.flushQueueCapacity
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
		out.applySSTableStats(l.tables.Snapshot())
	}
	out.CompactionL0Threshold = l.compactionL0Threshold
	out.CompactionPending = out.CompactionEnabled &&
		out.CompactionL0Threshold > 0 &&
		out.L0TableCount >= out.CompactionL0Threshold
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

func (s *Stats) applySSTableStats(metas []metadata.TableMeta) {
	if s == nil {
		return
	}
	s.TableCount = len(metas)
	s.SSTableCount = len(metas)
	if len(metas) == 0 {
		return
	}
	byLevel := make(map[int]*SSTableLevelStats)
	for _, meta := range metas {
		s.SSTableBytes += meta.SizeBytes
		if meta.Level == 0 {
			s.L0TableCount++
			s.L0SizeBytes += meta.SizeBytes
		}
		level := byLevel[meta.Level]
		if level == nil {
			level = &SSTableLevelStats{Level: meta.Level}
			byLevel[meta.Level] = level
		}
		level.TableCount++
		level.SizeBytes += meta.SizeBytes
	}
	levels := make([]int, 0, len(byLevel))
	for level := range byLevel {
		levels = append(levels, level)
	}
	sort.Ints(levels)
	s.SSTableLevels = make([]SSTableLevelStats, 0, len(levels))
	for _, level := range levels {
		s.SSTableLevels = append(s.SSTableLevels, *byLevel[level])
	}
}
