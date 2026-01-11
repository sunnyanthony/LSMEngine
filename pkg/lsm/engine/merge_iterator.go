package engine

import (
	"bytes"
	"container/heap"

	"lsmengine/pkg/lsm/types"
)

type mergeIterator struct {
	h       mergeHeap
	cur     types.Entry
	err     error
	lastKey []byte
}

func newMergeIterator(iters []Iterator) Iterator {
	if len(iters) == 0 {
		return newEmptyIterator()
	}
	h := make(mergeHeap, 0, len(iters))
	for idx, it := range iters {
		if it.Next() {
			h = append(h, mergeItem{
				idx:   idx,
				iter:  it,
				entry: it.Entry(),
			})
		} else if err := it.Err(); err != nil {
			return newErrorIterator(err)
		}
	}
	if len(h) == 0 {
		return newEmptyIterator()
	}
	heap.Init(&h)
	return &mergeIterator{h: h}
}

func (it *mergeIterator) Next() bool {
	if it.err != nil {
		return false
	}
	for it.h.Len() > 0 {
		item := heap.Pop(&it.h).(mergeItem)
		key := item.entry.Key
		best := item
		items := []mergeItem{item}

		for it.h.Len() > 0 && bytes.Equal(it.h[0].entry.Key, key) {
			next := heap.Pop(&it.h).(mergeItem)
			items = append(items, next)
			if next.idx < best.idx {
				best = next
			}
		}

		for _, cur := range items {
			if cur.iter.Next() {
				cur.entry = cur.iter.Entry()
				heap.Push(&it.h, cur)
			} else if err := cur.iter.Err(); err != nil {
				it.err = err
				return false
			}
		}

		if it.isDuplicate(key) {
			continue
		}

		if best.entry.Tombstone {
			it.setLastKey(key)
			continue
		}
		it.setLastKey(key)
		it.cur = best.entry
		return true
	}
	return false
}

func (it *mergeIterator) Entry() types.Entry {
	return it.cur
}

func (it *mergeIterator) Err() error {
	return it.err
}

func (it *mergeIterator) isDuplicate(key []byte) bool {
	return len(it.lastKey) > 0 && bytes.Equal(it.lastKey, key)
}

func (it *mergeIterator) setLastKey(key []byte) {
	it.lastKey = append(it.lastKey[:0], key...)
}

// Simple heap sort to help the merge sort
type mergeItem struct {
	idx   int
	iter  Iterator
	entry types.Entry
}

type mergeHeap []mergeItem

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].entry.Key, h[j].entry.Key)
	if cmp != 0 {
		return cmp < 0
	}
	return h[i].idx < h[j].idx
}

func (h mergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *mergeHeap) Push(x interface{}) {
	*h = append(*h, x.(mergeItem))
}

func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
