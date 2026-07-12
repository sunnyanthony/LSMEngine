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

func (l *LSM) initialCommitLogProviderIndex() uint64 {
	if l == nil || l.commitLog == nil {
		return 0
	}
	if l.commitLog.Provider() != CommitLogProviderLocal {
		return l.commitLogAppliedIndex
	}
	applied := l.seq
	if l.control != nil {
		if controlApplied := l.control.commitLogApplied(); controlApplied > applied {
			applied = controlApplied
		}
	}
	return applied
}

func (l *LSM) applyCommittedDataFromLog(entry dataCommittedEntry) error {
	if l == nil || l.writer == nil {
		return errs.ErrBackpressure
	}
	l.commitApplyMu.Lock()
	if l.shouldSkipCommittedDataLocked(entry.Commit.Index) {
		l.markCommitLogAppliedLocked(entry.Commit.Index)
		l.commitApplyMu.Unlock()
		return nil
	}
	seq, err := l.writer.applyCommittedDataLocked(entry)
	if err != nil {
		l.commitApplyMu.Unlock()
		return err
	}
	l.commitApplyMu.Unlock()
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
	if l.shouldSkipCommittedControlLocked(entry.Commit.Index) {
		l.markCommitLogAppliedLocked(entry.Commit.Index)
		l.commitApplyMu.Unlock()
		return nil
	}
	if err := l.control.applyReplicatedControlEntry(entry); err != nil {
		l.commitApplyMu.Unlock()
		return err
	}
	l.markCommitLogAppliedLocked(entry.Commit.Index)
	l.commitApplyMu.Unlock()
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

func (l *LSM) observeCommitLogAppliedIndex(index uint64) {
	if l == nil || index == 0 {
		return
	}
	observer, ok := l.commitLog.(commitLogIndexObserver)
	if !ok {
		return
	}
	observer.ObserveCommittedIndex(index)
}
