package engine

import (
	"errors"
	"fmt"
	"sync"

	internalcommitlog "lsmengine/internal/lsm/commitlog"
	"lsmengine/pkg/lsm/errs"
)

// committedApply serializes data and control effects, never proposal/network IO.
type committedApply struct {
	mu      sync.Mutex
	l       *LSM
	source  internalcommitlog.CommittedEntrySource
	cursor  uint64
	failure error
	results map[uint64]error
}

func (l *LSM) initCommittedApply(builtin *builtinCommitLogConsensus) {
	source, ok := builtin.inner.(internalcommitlog.CommittedEntrySource)
	if !ok {
		return
	}
	a := &committedApply{l: l, source: source, results: make(map[uint64]error)}
	// Startup recovery has already applied this entire retained history.
	for _, entry := range source.CommittedEntriesAfter(0) {
		if entry.Data != nil {
			a.cursor = entry.Data.Commit.Index
		}
		if entry.Control != nil {
			a.cursor = entry.Control.Commit.Index
		}
	}
	l.committedApply = a
	l.control.applyFromLog = func(entry controlCommittedEntry) error { return a.drain(entry.Commit.Index) }
	l.control.beforeProposal = a.status
}

func (a *committedApply) status() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.l.isClosing() {
		return errs.ErrClosed
	}
	return a.failure
}

func (a *committedApply) drain(target uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.l.isClosing() {
		return errs.ErrClosed
	}
	if a.failure != nil {
		return a.failure
	}
	for _, item := range a.source.CommittedEntriesAfter(a.cursor) {
		var index uint64
		var err error
		switch {
		case item.Data != nil && item.Control == nil:
			entry := fromInternalDataCommittedEntry(*item.Data)
			index = entry.Commit.Index
			_, err = a.l.writer.applyCommittedData(entry)
			if err == nil {
				m := entry.Mutation
				a.l.recordCDCEvent(m.Kind, m.Key, m.Value, entry.Seq, m.Kind == "delete")
			}
		case item.Control != nil && item.Data == nil:
			entry := fromInternalControlCommittedEntry(*item.Control)
			index = entry.Commit.Index
			err = a.l.control.applyCommittedControlFromLog(entry)
		default:
			err = fmt.Errorf("invalid committed entry")
		}
		if err != nil {
			var rejection *persistedControlRejection
			if !errors.As(err, &rejection) {
				a.failure = err
				return err
			}
			a.results[index] = err
		}
		a.cursor = index
	}
	if target > a.cursor {
		return fmt.Errorf("committed entry %d not available through %d", target, a.cursor)
	}
	return a.results[target]
}

func (a *committedApply) quiesce() {
	// closing is set before this barrier; future drain calls reject application.
	a.mu.Lock()
	a.mu.Unlock()
}
