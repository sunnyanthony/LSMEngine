package engine

import (
	"sync/atomic"
	"time"
)

const (
	hlcCounterBits = 16
	hlcCounterMask = uint64(1<<hlcCounterBits) - 1
)

// nextSeq returns a hybrid logical clock value (time, counter) that is
// monotonically increasing across local writes.
func (l *LSM) nextSeq() uint64 {
	for {
		now := uint64(time.Now().UnixMicro())
		last := atomic.LoadUint64(&l.seq)
		lastTime := last >> hlcCounterBits
		lastCounter := uint16(last & hlcCounterMask)

		var next uint64
		if now > lastTime {
			next = now << hlcCounterBits
		} else {
			counter := lastCounter + 1
			if counter == 0 {
				next = (lastTime + 1) << hlcCounterBits
			} else {
				next = (lastTime << hlcCounterBits) | uint64(counter)
			}
		}
		if atomic.CompareAndSwapUint64(&l.seq, last, next) {
			return next
		}
	}
}

// bumpSeq observes a remote or replayed sequence and advances the local HLC.
func (l *LSM) bumpSeq(seq uint64) {
	if seq == 0 {
		return
	}
	for {
		now := uint64(time.Now().UnixMicro())
		last := atomic.LoadUint64(&l.seq)
		lastTime := last >> hlcCounterBits
		lastCounter := uint16(last & hlcCounterMask)
		remoteTime := seq >> hlcCounterBits
		remoteCounter := uint16(seq & hlcCounterMask)

		maxTime := lastTime
		if now > maxTime {
			maxTime = now
		}
		if remoteTime > maxTime {
			maxTime = remoteTime
		}

		var counter uint16
		switch {
		case maxTime == lastTime && maxTime == remoteTime:
			counter = maxUint16(lastCounter, remoteCounter) + 1
		case maxTime == lastTime:
			counter = lastCounter + 1
		case maxTime == remoteTime:
			counter = remoteCounter + 1
		default:
			counter = 0
		}
		if counter == 0 {
			maxTime++
		}
		next := (maxTime << hlcCounterBits) | uint64(counter)
		if next <= last {
			return
		}
		if atomic.CompareAndSwapUint64(&l.seq, last, next) {
			return
		}
	}
}

func maxUint16(a, b uint16) uint16 {
	if a >= b {
		return a
	}
	return b
}
