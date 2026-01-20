// Helpers to build and apply entries to memtables.

package engine

import (
	"lsmengine/internal/lsm/memory"
	memtable "lsmengine/internal/lsm/memtable"
	"lsmengine/pkg/lsm/types"
)

func (l *LSM) applyEntryOwned(table memtable.Table, entry types.Entry) {
	table.ApplyOwned(entry)
}

func (l *LSM) applyEntriesOwned(table memtable.Table, entries []types.Entry) {
	table.ApplyBatchOwned(entries)
}

func (l *LSM) prepareEntry(table memtable.Table, entry types.Entry) types.Entry {
	return l.entryBuilder(table).FromEntry(entry)
}

func (l *LSM) entryBuilder(table memtable.Table) memory.EntryBuilder {
	return memory.NewEntryBuilder(table.CopyEntry)
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
