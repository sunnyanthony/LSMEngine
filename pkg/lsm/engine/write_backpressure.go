package engine

import (
	memtable "lsmengine/internal/lsm/memtable"
	"lsmengine/pkg/lsm/errs"
)

const (
	writeBackpressureReasonFlushBlocked = "flush_blocked"
	writeBackpressureReasonFlushQueue   = "flush_queue"
	writeBackpressureReasonL0           = "l0_compaction_pressure"
	writeBackpressureReasonWAL          = "wal_checkpoint_lag"
)

type writeBackpressureSnapshot struct {
	active       bool
	reason       string
	rejects      uint64
	queueLimit   int
	l0Limit      int
	l0TableCount int
	walLagLimit  uint64
	walLag       uint64
}

func (s *writeService) admitWrite(delta int) error {
	if s == nil || s.l == nil {
		return errs.ErrBackpressure
	}
	if reason := s.l.writeAdmissionBackpressureReason(delta); reason != "" {
		s.l.writeBackpressureRejects.Add(1)
		return errs.ErrBackpressure
	}
	return nil
}

func (l *LSM) writeBackpressureSnapshot() writeBackpressureSnapshot {
	if l == nil {
		return writeBackpressureSnapshot{}
	}
	reason := l.writeBackpressureReason(false, 0)
	return writeBackpressureSnapshot{
		active:       reason != "",
		reason:       reason,
		rejects:      l.writeBackpressureRejects.Load(),
		queueLimit:   l.flushBackpressureQueueThreshold,
		l0Limit:      l.compactionBackpressureL0Threshold,
		l0TableCount: l.l0TableCount(),
		walLagLimit:  l.walBackpressureMaxCheckpointLag,
		walLag:       l.walCheckpointLag(),
	}
}

func (l *LSM) writeAdmissionBackpressureReason(delta int) string {
	return l.writeBackpressureReason(true, delta)
}

func (l *LSM) writeBackpressureReason(admission bool, delta int) string {
	if l == nil {
		return ""
	}
	if l.flushBlocked.Load() {
		return writeBackpressureReasonFlushBlocked
	}

	var (
		mem        memtable.Table
		queueDepth int
		wouldFlush bool
	)
	l.memMu.RLock()
	mem = l.mem
	queueDepth = len(l.flushQueue)
	if admission && mem != nil && l.mtLimit > 0 {
		wouldFlush = mem.Size()+delta >= l.mtLimit
	}
	l.memMu.RUnlock()

	queueLimit := l.flushBackpressureQueueThreshold
	if queueLimit > 0 && queueDepth >= queueLimit {
		return writeBackpressureReasonFlushQueue
	}
	l0Limit := l.compactionBackpressureL0Threshold
	if l0Limit > 0 && l.l0TableCount() >= l0Limit {
		if !admission || wouldFlush {
			return writeBackpressureReasonL0
		}
	}
	walLimit := l.walBackpressureMaxCheckpointLag
	if walLimit > 0 && l.walCheckpointLag() > walLimit {
		return writeBackpressureReasonWAL
	}
	if !admission || !wouldFlush {
		return ""
	}
	if l.dispatch != nil && !l.dispatch.CanEnqueue() {
		return writeBackpressureReasonFlushQueue
	}
	return ""
}

func (l *LSM) l0TableCount() int {
	if l == nil || l.tables == nil {
		return 0
	}
	count := 0
	for _, meta := range l.tables.Snapshot() {
		if meta.Level == 0 {
			count++
		}
	}
	return count
}
