package lsm

import (
	"lsmengine/internal/lsm/memtable"
	"lsmengine/pkg/lsm/types"
)

func (l *LSM) applyEntryOwned(table memtable.Table, entry types.Entry) {
	table.ApplyOwned(entry)
}

func (l *LSM) applyEntriesOwned(table memtable.Table, entries []types.Entry) {
	table.ApplyBatchOwned(entries)
}

func (l *LSM) prepareEntry(table memtable.Table, entry types.Entry) types.Entry {
	return table.CopyEntry(entry)
}

func entriesFromTable(table memtable.Table) []types.Entry {
	return entriesFromIter(table.Iter())
}

func entriesFromIter(it memtable.Iterator) []types.Entry {
	var entries []types.Entry
	for it.Next() {
		entries = append(entries, it.Entry())
	}
	return entries
}
