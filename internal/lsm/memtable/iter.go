package memtable

import "lsmengine/pkg/lsm/types"

type sliceIterator struct {
	entries []types.Entry
	idx     int
	curr    types.Entry
}

func newSliceIterator(entries []types.Entry) Iterator {
	return &sliceIterator{entries: entries}
}

func (it *sliceIterator) Next() bool {
	if it.idx >= len(it.entries) {
		return false
	}
	it.curr = it.entries[it.idx]
	it.idx++
	return true
}

func (it *sliceIterator) Entry() types.Entry {
	return it.curr
}
