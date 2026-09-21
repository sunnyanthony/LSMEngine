// Committed sequence tracking.

package engine

import "sync/atomic"

func (l *LSM) observeCommittedSeq(seq uint64) {
	if seq == 0 {
		return
	}
	for {
		last := atomic.LoadUint64(&l.seq)
		if seq <= last {
			return
		}
		if atomic.CompareAndSwapUint64(&l.seq, last, seq) {
			return
		}
	}
}
