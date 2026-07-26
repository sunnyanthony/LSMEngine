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

// SSTableFlowStats describes lower-level SSTable read-pipeline counters.
type SSTableFlowStats struct {
	CacheHit   uint64
	CacheMiss  uint64
	FilterPass uint64
	FilterSkip uint64
	Errors     uint64
}

// CompactionRuntimeStats describes cumulative background compaction activity.
type CompactionRuntimeStats struct {
	Triggers          uint64
	CoalescedTriggers uint64
	Runs              uint64
	Steps             uint64
	SuccessfulSteps   uint64
	Errors            uint64
	Running           bool
}

// WriteBackpressureStats describes current write-admission pressure state.
type WriteBackpressureStats struct {
	Active                 bool
	Reason                 string
	Rejects                uint64
	FlushQueueThreshold    int
	CompactionL0Threshold  int
	CompactionL0TableCount int
}

// WALStats describes WAL segment pressure and write-path configuration state.
type WALStats struct {
	SegmentID              uint64
	SegmentCount           int
	ArchivedSegmentCount   int
	ActiveSegmentBytes     uint64
	ArchivedSegmentBytes   uint64
	TotalBytes             uint64
	CheckpointSeq          uint64
	CheckpointLag          uint64
	MaxSegmentBytes        uint64
	RetainArchivedSegments int
	BlockSize              uint32
	PendingBlockBytes      int
	PendingBlockRecords    int
	Sync                   bool
	Async                  bool
	Closed                 bool
	SegmentScanError       string
}

// Stats describes a point-in-time view of engine activity.
type Stats struct {
	MemtableBytes             int
	MemtableEntries           int
	ImmutableCount            int
	ImmutableBytes            int
	FlushQueueDepth           int
	FlushQueueCapacity        int
	PinnedCount               int
	TableCount                int
	SSTableCount              int
	SSTableBytes              uint64
	SSTableLevels             []SSTableLevelStats
	L0TableCount              int
	L0SizeBytes               uint64
	CompactionL0Threshold     int
	CompactionCheckIntervalMS int64
	CompactionPending         bool
	PointReads                uint64
	PointReadMemtableHits     uint64
	PointReadImmutableHits    uint64
	PointReadSSTableHits      uint64
	PointReadMisses           uint64
	PointReadSSTableProbes    uint64
	PointReadMaxSSTableProbes uint64
	SSTableFlow               SSTableFlowStats
	CompactionRuntime         CompactionRuntimeStats
	WriteBackpressure         WriteBackpressureStats
	WAL                       WALStats
	Seq                       uint64
	Closing                   bool
	Closed                    bool
	FlushBlocked              bool
	CompactionEnabled         bool
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
	out.CompactionCheckIntervalMS = l.compactionCheckInterval.Milliseconds()
	out.CompactionPending = out.CompactionEnabled &&
		out.CompactionL0Threshold > 0 &&
		out.L0TableCount >= out.CompactionL0Threshold
	out.applyReadStats(l)
	out.applyCompactionRuntimeStats(l)
	out.applyWriteBackpressureStats(l)
	out.applyWALStats(l)
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
	if l.writeBackpressureSnapshot().active {
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

func (s *Stats) applyReadStats(l *LSM) {
	if s == nil || l == nil {
		return
	}
	reads := l.pointReads.snapshot()
	s.PointReads = reads.count
	s.PointReadMemtableHits = reads.memtableHits
	s.PointReadImmutableHits = reads.immutableHits
	s.PointReadSSTableHits = reads.sstableHits
	s.PointReadMisses = reads.misses
	s.PointReadSSTableProbes = reads.sstableProbes
	s.PointReadMaxSSTableProbes = reads.maxSSTableProbes
	flow := l.FlowMetrics()
	s.SSTableFlow = SSTableFlowStats{
		CacheHit:   flow.CacheHit,
		CacheMiss:  flow.CacheMiss,
		FilterPass: flow.FilterPass,
		FilterSkip: flow.FilterSkip,
		Errors:     flow.Errors,
	}
}

func (s *Stats) applyWALStats(l *LSM) {
	if s == nil || l == nil || l.wal == nil {
		return
	}
	stats := l.wal.Stats()
	s.WAL = WALStats{
		SegmentID:              stats.SegmentID,
		SegmentCount:           stats.SegmentCount,
		ArchivedSegmentCount:   stats.ArchivedSegmentCount,
		ActiveSegmentBytes:     stats.ActiveSegmentBytes,
		ArchivedSegmentBytes:   stats.ArchivedSegmentBytes,
		TotalBytes:             stats.TotalBytes,
		CheckpointSeq:          atomic.LoadUint64(&l.lastFlush),
		MaxSegmentBytes:        stats.MaxSegmentBytes,
		RetainArchivedSegments: l.walRetainArchivedSegments,
		BlockSize:              stats.BlockSize,
		PendingBlockBytes:      stats.PendingBlockBytes,
		PendingBlockRecords:    stats.PendingBlockRecords,
		Sync:                   stats.Sync,
		Async:                  stats.Async,
		Closed:                 stats.Closed,
		SegmentScanError:       stats.SegmentScanError,
	}
	if s.Seq > s.WAL.CheckpointSeq {
		s.WAL.CheckpointLag = s.Seq - s.WAL.CheckpointSeq
	}
}

func (s *Stats) applyCompactionRuntimeStats(l *LSM) {
	if s == nil || l == nil || l.compactionSvc == nil {
		return
	}
	stats := l.compactionSvc.Stats()
	s.CompactionRuntime = CompactionRuntimeStats{
		Triggers:          stats.Triggers,
		CoalescedTriggers: stats.CoalescedTriggers,
		Runs:              stats.Runs,
		Steps:             stats.Steps,
		SuccessfulSteps:   stats.SuccessfulSteps,
		Errors:            stats.Errors,
		Running:           stats.Running,
	}
}

func (s *Stats) applyWriteBackpressureStats(l *LSM) {
	if s == nil || l == nil {
		return
	}
	stats := l.writeBackpressureSnapshot()
	s.WriteBackpressure = WriteBackpressureStats{
		Active:                 stats.active,
		Reason:                 stats.reason,
		Rejects:                stats.rejects,
		FlushQueueThreshold:    stats.queueLimit,
		CompactionL0Threshold:  stats.l0Limit,
		CompactionL0TableCount: stats.l0TableCount,
	}
}
