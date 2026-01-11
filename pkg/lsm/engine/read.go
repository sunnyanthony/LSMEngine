package engine

import "lsmengine/pkg/lsm/types"

func (l *LSM) Get(key []byte) (types.Entry, bool) {
	mem := l.activeMem()
	if e, ok := mem.Get(key); ok {
		return copyEntry(e), !e.Tombstone
	}
	for _, table := range l.immutableMems() {
		if e, ok := table.Get(key); ok {
			return copyEntry(e), !e.Tombstone
		}
	}
	for _, table := range l.tables.Tables() {
		if e, ok := table.Get(key); ok {
			return copyEntry(e), !e.Tombstone
		}
	}
	return types.Entry{}, false
}
