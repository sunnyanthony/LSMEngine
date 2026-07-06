package engine

import "lsmengine/pkg/lsm/errs"

type lsmCommittedEntryObserver struct {
	l *LSM
}

func (o lsmCommittedEntryObserver) ObserveCommittedControl(entry controlCommittedEntry) error {
	if o.l == nil {
		return nil
	}
	return o.l.applyCommittedControlFromLog(entry)
}

func (o lsmCommittedEntryObserver) ObserveCommittedData(entry dataCommittedEntry) error {
	if o.l == nil {
		return nil
	}
	return o.l.applyCommittedDataFromLog(entry)
}

func (l *LSM) initialCommitLogAppliedIndex() uint64 {
	if l == nil {
		return 0
	}
	if l.control == nil {
		return l.seq
	}
	controlApplied := l.control.commitLogApplied()
	if l.seq == 0 || controlApplied == 0 {
		return 0
	}
	if l.seq < controlApplied {
		return l.seq
	}
	return controlApplied
}

func (l *LSM) applyCommittedDataFromLog(entry dataCommittedEntry) error {
	if l == nil || l.writer == nil {
		return errs.ErrBackpressure
	}
	l.commitApplyMu.Lock()
	defer l.commitApplyMu.Unlock()
	if l.shouldSkipCommittedDataLocked(entry.Commit.Index) {
		l.markCommitLogAppliedLocked(entry.Commit.Index)
		return nil
	}
	seq, err := l.writer.applyCommittedDataLocked(entry)
	if err != nil {
		return err
	}
	switch entry.Mutation.Kind {
	case "put":
		l.recordCDCEvent("put", entry.Mutation.Key, entry.Mutation.Value, seq, false)
	case "delete":
		l.recordCDCEvent("delete", entry.Mutation.Key, nil, seq, true)
	}
	return nil
}

func (l *LSM) applyCommittedControlFromLog(entry controlCommittedEntry) error {
	if l == nil || l.control == nil {
		return errs.ErrShardNotFound
	}
	l.commitApplyMu.Lock()
	defer l.commitApplyMu.Unlock()
	if l.shouldSkipCommittedControlLocked(entry.Commit.Index) {
		l.markCommitLogAppliedLocked(entry.Commit.Index)
		return nil
	}
	if err := l.control.applyReplicatedControlEntry(entry); err != nil {
		return err
	}
	l.markCommitLogAppliedLocked(entry.Commit.Index)
	return nil
}

func (l *LSM) shouldSkipCommittedDataLocked(index uint64) bool {
	return index == 0 || index <= l.seq
}

func (l *LSM) shouldSkipCommittedControlLocked(index uint64) bool {
	return index == 0 || (l.control != nil && index <= l.control.commitLogApplied())
}

func (l *LSM) markCommitLogApplied(index uint64) {
	if l == nil || index == 0 {
		return
	}
	l.commitApplyMu.Lock()
	defer l.commitApplyMu.Unlock()
	l.markCommitLogAppliedLocked(index)
}

func (l *LSM) markCommitLogAppliedLocked(index uint64) {
	if index > l.commitLogAppliedIndex {
		l.commitLogAppliedIndex = index
	}
}
