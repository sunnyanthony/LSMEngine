package engine

import "lsmengine/pkg/lsm/types"

// Iterator walks range scan results in key order.
type Iterator interface {
	Next() bool
	Entry() types.Entry
	Err() error
}

type errorIterator struct {
	err error
}

func (it *errorIterator) Next() bool {
	return false
}

func (it *errorIterator) Entry() types.Entry {
	return types.Entry{}
}

func (it *errorIterator) Err() error {
	return it.err
}

func newErrorIterator(err error) Iterator {
	return &errorIterator{err: err}
}

func newEmptyIterator() Iterator {
	return &errorIterator{}
}

type copyIterator struct {
	iter Iterator
	cur  types.Entry
}

func newCopyIterator(iter Iterator) Iterator {
	if iter == nil {
		return newEmptyIterator()
	}
	return &copyIterator{iter: iter}
}

func (it *copyIterator) Next() bool {
	if it.iter == nil {
		return false
	}
	if !it.iter.Next() {
		return false
	}
	it.cur = copyEntry(it.iter.Entry())
	return true
}

func (it *copyIterator) Entry() types.Entry {
	return it.cur
}

func (it *copyIterator) Err() error {
	if it.iter == nil {
		return nil
	}
	return it.iter.Err()
}
