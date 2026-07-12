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
	if l.shouldSkipCommittedEntryLocked(entry.Commit.Index) {
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
	if l.shouldSkipCommittedEntryLocked(entry.Commit.Index) {
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

func (l *LSM) shouldSkipCommittedEntryLocked(index uint64) bool {
	return index == 0 || index <= l.commitLogAppliedIndex
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
