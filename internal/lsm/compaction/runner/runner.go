package runner

import (
	"bytes"
	"container/heap"
	"fmt"
	"sort"

	"lsmengine/internal/lsm/compaction/data"
	"lsmengine/internal/lsm/compaction/model"
	"lsmengine/internal/lsm/metadata"
	"lsmengine/internal/lsm/sstable"
	"lsmengine/pkg/lsm/types"
)

// SimpleRunner performs a k-way merge across input tables.
// It emits the newest version per key and optionally drops tombstones.
type SimpleRunner struct {
	Flusher        sstable.Flusher
	DropTombstones bool
}

// Run merges the plan inputs and produces a new SSTable.
func (r *SimpleRunner) Run(p model.Plan, inputs []sstable.SSTable) (data.Result, error) {
	if r == nil || r.Flusher == nil {
		return data.Result{}, fmt.Errorf("compaction runner: flusher required")
	}
	if len(p.Inputs) == 0 {
		return data.Result{}, nil
	}
	if len(inputs) == 0 {
		return data.Result{}, fmt.Errorf("compaction runner: input tables required")
	}
	inputs = append([]sstable.SSTable(nil), inputs...)
	sort.Slice(inputs, func(i, j int) bool {
		return inputs[i].Seq > inputs[j].Seq
	})
	iters := make([]entryIterator, 0, len(inputs))
	for _, table := range inputs {
		iters = append(iters, table.Range(nil, nil))
	}
	entries, err := mergeEntries(iters, r.DropTombstones)
	if err != nil {
		return data.Result{}, err
	}
	if len(entries) == 0 {
		return data.Result{
			OutputLevel: p.OutputLevel,
			Obsolete:    append([]metadata.TableMeta(nil), p.Inputs...),
		}, nil
	}
	out, err := r.Flusher.Flush(entries)
	if err != nil {
		return data.Result{}, err
	}
	return data.Result{
		Output:      []sstable.SSTable{out},
		Obsolete:    append([]metadata.TableMeta(nil), p.Inputs...),
		OutputLevel: p.OutputLevel,
	}, nil
}

type entryIterator interface {
	Next() bool
	Entry() types.Entry
	Err() error
}

type mergeItem struct {
	iter  entryIterator
	entry types.Entry
}

type mergeHeap []mergeItem

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].entry.Key, h[j].entry.Key)
	if cmp != 0 {
		return cmp < 0
	}
	if h[i].entry.Seq != h[j].entry.Seq {
		return h[i].entry.Seq > h[j].entry.Seq
	}
	if h[i].entry.Tombstone != h[j].entry.Tombstone {
		return h[i].entry.Tombstone
	}
	return false
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

func mergeEntries(iters []entryIterator, dropTombstones bool) ([]types.Entry, error) {
	h := make(mergeHeap, 0, len(iters))
	for _, it := range iters {
		if it.Next() {
			h = append(h, mergeItem{iter: it, entry: it.Entry()})
		} else if err := it.Err(); err != nil {
			return nil, err
		}
	}
	heap.Init(&h)
	out := make([]types.Entry, 0, 128)
	for h.Len() > 0 {
		item := heap.Pop(&h).(mergeItem)
		key := item.entry.Key
		best := item.entry
		group := []mergeItem{item}
		for h.Len() > 0 && bytes.Equal(h[0].entry.Key, key) {
			next := heap.Pop(&h).(mergeItem)
			group = append(group, next)
		}
		for _, g := range group[1:] {
			if g.entry.Seq > best.Seq {
				best = g.entry
			} else if g.entry.Seq == best.Seq && g.entry.Tombstone && !best.Tombstone {
				best = g.entry
			}
		}
		for _, g := range group {
			next, ok, err := advanceIterator(g, key)
			if err != nil {
				return nil, err
			}
			if ok {
				heap.Push(&h, next)
			}
		}
		if best.Tombstone && dropTombstones {
			continue
		}
		out = append(out, best)
	}
	return out, nil
}

func advanceIterator(item mergeItem, key []byte) (mergeItem, bool, error) {
	for {
		if item.iter.Next() {
			entry := item.iter.Entry()
			if bytes.Equal(entry.Key, key) {
				item.entry = entry
				continue
			}
			item.entry = entry
			return item, true, nil
		}
		if err := item.iter.Err(); err != nil {
			return mergeItem{}, false, err
		}
		return mergeItem{}, false, nil
	}
}
