package lsm

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
