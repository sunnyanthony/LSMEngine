// Point read API entry point.

package engine

import "lsmengine/pkg/lsm/types"

func (l *LSM) Get(key []byte) (types.Entry, bool) {
	if l == nil {
		return types.Entry{}, false
	}
	if l.reader == nil {
		l.reader = newReadService(l)
	}
	return l.reader.Get(key)
}
