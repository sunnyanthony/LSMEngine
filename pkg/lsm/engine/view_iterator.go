// View-based iterators and merge logic for range scans.

package engine

import (
	"bytes"
	"container/heap"

	"lsmengine/internal/lsm/memory"
	memtable "lsmengine/internal/lsm/memtable"
	"lsmengine/internal/lsm/sstable"
	"lsmengine/pkg/lsm/types"
)

// Iterator walks range scan results in key order.
type Iterator interface {
	Next() bool
	Entry() types.Entry
	Err() error
}

type emptyIterator struct{}

func (it *emptyIterator) Next() bool {
	return false
}

func (it *emptyIterator) Entry() types.Entry {
	return types.Entry{}
}

func (it *emptyIterator) Err() error {
	return nil
}

func newEmptyIterator() Iterator {
	return &emptyIterator{}
}

type viewIterator interface {
	Next() bool
	View() memory.EntryView
	Err() error
}

type viewErrorIterator struct {
	err error
}

func (it *viewErrorIterator) Next() bool { return false }
func (it *viewErrorIterator) View() memory.EntryView {
	return memory.EntryView{}
}
func (it *viewErrorIterator) Err() error { return it.err }

type viewToEntryIterator struct {
	iter viewIterator
	cur  types.Entry
}

func newViewToEntryIterator(iter viewIterator) Iterator {
	if iter == nil {
		return newEmptyIterator()
	}
	return &viewToEntryIterator{iter: iter}
}

func (it *viewToEntryIterator) Next() bool {
	if it.iter == nil {
		return false
	}
	if !it.iter.Next() {
		return false
	}
	view := it.iter.View()
	it.cur = copyEntry(view.Entry())
	return true
}

func (it *viewToEntryIterator) Entry() types.Entry {
	return it.cur
}

func (it *viewToEntryIterator) Err() error {
	if it.iter == nil {
		return nil
	}
	return it.iter.Err()
}

type memtableViewIter struct {
	iter memtable.Iterator
	cur  memory.EntryView
}

func (it *memtableViewIter) Next() bool {
	if it.iter == nil {
		return false
	}
	if !it.iter.Next() {
		return false
	}
	entry := it.iter.Entry()
	it.cur = memory.EntryView{
		Key:       entry.Key,
		Value:     entry.Value,
		Tombstone: entry.Tombstone,
		Seq:       entry.Seq,
	}
	return true
}

func (it *memtableViewIter) View() memory.EntryView { return it.cur }
func (it *memtableViewIter) Err() error             { return nil }

type sstableViewIter struct {
	iter *sstable.RangeIterator
	cur  memory.EntryView
}

func (it *sstableViewIter) Next() bool {
	if it.iter == nil {
		return false
	}
	if !it.iter.Next() {
		return false
	}
	it.cur = it.iter.EntryView()
	return true
}

func (it *sstableViewIter) View() memory.EntryView { return it.cur }
func (it *sstableViewIter) Err() error {
	if it.iter == nil {
		return nil
	}
	return it.iter.Err()
}

type viewMergeIterator struct {
	h       viewMergeHeap
	cur     memory.EntryView
	err     error
	lastKey []byte
	pending []viewMergeItem
}

func newViewMergeIterator(iters []viewIterator) viewIterator {
	if len(iters) == 0 {
		return &viewErrorIterator{}
	}
	h := make(viewMergeHeap, 0, len(iters))
	for idx, it := range iters {
		if it == nil {
			continue
		}
		if it.Next() {
			h = append(h, viewMergeItem{
				idx:   idx,
				iter:  it,
				entry: it.View(),
			})
		} else if err := it.Err(); err != nil {
			return &viewErrorIterator{err: err}
		}
	}
	if len(h) == 0 {
		return &viewErrorIterator{}
	}
	heap.Init(&h)
	return &viewMergeIterator{h: h}
}

func (it *viewMergeIterator) Next() bool {
	if it.err != nil {
		return false
	}
	if !it.advancePending() {
		return false
	}
	for it.h.Len() > 0 {
		item := heap.Pop(&it.h).(viewMergeItem)
		key := item.entry.Key
		best := item
		items := []viewMergeItem{item}

		for it.h.Len() > 0 && bytes.Equal(it.h[0].entry.Key, key) {
			next := heap.Pop(&it.h).(viewMergeItem)
			items = append(items, next)
			if next.idx < best.idx {
				best = next
			}
		}

		if it.isDuplicate(key) {
			it.pending = append(it.pending[:0], items...)
			if !it.advancePending() {
				return false
			}
			continue
		}

		if best.entry.Tombstone {
			it.setLastKey(key)
			it.pending = append(it.pending[:0], items...)
			if !it.advancePending() {
				return false
			}
			continue
		}
		it.setLastKey(key)
		it.cur = best.entry
		it.pending = append(it.pending[:0], items...)
		return true
	}
	return false
}

func (it *viewMergeIterator) View() memory.EntryView {
	return it.cur
}

func (it *viewMergeIterator) Err() error {
	return it.err
}

func (it *viewMergeIterator) isDuplicate(key []byte) bool {
	return len(it.lastKey) > 0 && bytes.Equal(it.lastKey, key)
}

func (it *viewMergeIterator) setLastKey(key []byte) {
	it.lastKey = append(it.lastKey[:0], key...)
}

func (it *viewMergeIterator) advancePending() bool {
	if len(it.pending) == 0 {
		return true
	}
	for _, cur := range it.pending {
		if cur.iter.Next() {
			cur.entry = cur.iter.View()
			heap.Push(&it.h, cur)
		} else if err := cur.iter.Err(); err != nil {
			it.err = err
			return false
		}
	}
	it.pending = it.pending[:0]
	return true
}

type viewMergeItem struct {
	idx   int
	iter  viewIterator
	entry memory.EntryView
}

type viewMergeHeap []viewMergeItem

func (h viewMergeHeap) Len() int { return len(h) }
func (h viewMergeHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].entry.Key, h[j].entry.Key)
	if cmp != 0 {
		return cmp < 0
	}
	return h[i].idx < h[j].idx
}
func (h viewMergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *viewMergeHeap) Push(x interface{}) {
	*h = append(*h, x.(viewMergeItem))
}
func (h *viewMergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
