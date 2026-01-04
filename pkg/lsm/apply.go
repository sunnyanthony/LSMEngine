package lsm

import (
	"lsmengine/pkg/lsm/memtable"
	"lsmengine/pkg/lsm/types"
)

func (l *LSM) applyEntryOwned(table memtable.Table, entry types.Entry) {
	if applier, ok := table.(interface{ ApplyOwned(types.Entry) }); ok {
		applier.ApplyOwned(entry)
		return
	}
	table.Apply(entry)
}

func (l *LSM) applyEntriesOwned(table memtable.Table, entries []types.Entry) {
	if applier, ok := table.(memtable.BatchOwnedApplier); ok {
		applier.ApplyBatchOwned(entries)
		return
	}
	for i := range entries {
		l.applyEntryOwned(table, entries[i])
	}
}

func (l *LSM) prepareEntry(table memtable.Table, entry types.Entry) types.Entry {
	if copier, ok := table.(memtable.EntryCopier); ok {
		return copier.CopyEntry(entry)
	}
	return copyEntry(entry)
}

func copyEntry(entry types.Entry) types.Entry {
	if len(entry.Key) > 0 {
		entry.Key = append([]byte(nil), entry.Key...)
	}
	if len(entry.Value) > 0 {
		entry.Value = append([]byte(nil), entry.Value...)
	}
	return entry
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
