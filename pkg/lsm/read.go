package lsm

import "lsmengine/pkg/lsm/types"

func (l *LSM) Get(key []byte) (types.Entry, bool) {
	mem := l.activeMem()
	if e, ok := mem.Get(key); ok {
		return e, !e.Tombstone
	}
	for _, table := range l.immutableMems() {
		if e, ok := table.Get(key); ok {
			return e, !e.Tombstone
		}
	}
	l.tablesMu.RLock()
	defer l.tablesMu.RUnlock()
	for _, table := range l.tables {
		if e, ok := table.Get(key); ok {
			return e, !e.Tombstone
		}
	}
	return types.Entry{}, false
}
