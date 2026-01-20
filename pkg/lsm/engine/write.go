// Write path API entry points.

package engine

import "lsmengine/pkg/lsm/errs"

func (l *LSM) Put(key []byte, value []byte) error {
	if l == nil {
		return errs.ErrBackpressure
	}
	if l.writer == nil {
		l.writer = newWriteService(l)
	}
	return l.writer.Put(key, value)
}

func (l *LSM) Delete(key []byte) error {
	if l == nil {
		return errs.ErrBackpressure
	}
	if l.writer == nil {
		l.writer = newWriteService(l)
	}
	return l.writer.Delete(key)
}
