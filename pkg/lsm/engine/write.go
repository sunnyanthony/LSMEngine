// Write path API entry points.

package engine

import "lsmengine/pkg/lsm/errs"

func (l *LSM) Put(key []byte, value []byte) error {
	_, err := l.PutWithSeq(key, value)
	return err
}

// PutWithSeq writes a key/value pair and returns the committed sequence.
func (l *LSM) PutWithSeq(key []byte, value []byte) (uint64, error) {
	if l == nil {
		return 0, errs.ErrBackpressure
	}
	if l.isClosing() {
		return 0, errs.ErrClosed
	}
	if l.writer == nil {
		l.writer = newWriteService(l)
	}
	return l.writer.PutWithSeq(key, value)
}

func (l *LSM) Delete(key []byte) error {
	_, err := l.DeleteWithSeq(key)
	return err
}

// DeleteWithSeq deletes a key and returns the committed sequence.
func (l *LSM) DeleteWithSeq(key []byte) (uint64, error) {
	if l == nil {
		return 0, errs.ErrBackpressure
	}
	if l.isClosing() {
		return 0, errs.ErrClosed
	}
	if l.writer == nil {
		l.writer = newWriteService(l)
	}
	return l.writer.DeleteWithSeq(key)
}
