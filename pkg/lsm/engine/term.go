package engine

import (
	"sync/atomic"

	"lsmengine/pkg/lsm/errs"
)

func (l *LSM) termForWrite() (uint64, error) {
	term := l.currentTerm()
	if l.termProvider != nil {
		if leader, ok := l.termProvider.(LeaderProvider); ok && !leader.IsLeader() {
			return term, errs.ErrNotLeader
		}
	}
	return term, nil
}

func (l *LSM) currentTerm() uint64 {
	if l.termProvider == nil {
		return l.ensureNodeTerm(atomic.LoadUint64(&l.nodeTerm))
	}
	term := l.termProvider.Term()
	if term == 0 {
		term = atomic.LoadUint64(&l.nodeTerm)
	}
	term = l.ensureNodeTerm(term)
	atomic.StoreUint64(&l.nodeTerm, term)
	return term
}

func (l *LSM) ensureNodeTerm(term uint64) uint64 {
	if term == 0 {
		term = 1
		atomic.StoreUint64(&l.nodeTerm, term)
	}
	return term
}
