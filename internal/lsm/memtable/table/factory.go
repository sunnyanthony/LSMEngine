// Memtable factory selection helpers.

package table

import (
	"fmt"
	"strings"

	"lsmengine/internal/lsm/memtable"
	"lsmengine/pkg/lsm/types"
)

const (
	KindMap             = "map"
	KindSkipList        = "skiplist"
	KindShardedSkipList = "sharded-skiplist"
)

// FactoryForKind returns a memtable factory based on a short kind string.
func FactoryForKind(kind string, concurrency int, shards int, arenaBlockSize int) (memtable.Factory, error) {
	switch strings.ToLower(kind) {
	case "", "sharded", KindShardedSkipList:
		if shards > 0 {
			return func() memtable.Table { return NewShardedSkipListTableWithShardsAndArena(shards, arenaBlockSize) }, nil
		}
		return func() memtable.Table { return NewShardedSkipListTableWithArena(concurrency, arenaBlockSize) }, nil
	case KindSkipList:
		return func() memtable.Table { return NewSkipListTableWithArena(arenaBlockSize) }, nil
	case KindMap:
		return func() memtable.Table { return NewMapTableWithArena(arenaBlockSize) }, nil
	default:
		return nil, fmt.Errorf("unknown memtable kind %q", kind)
	}
}

type sliceIterator struct {
	entries []types.Entry
	idx     int
	curr    types.Entry
}

func newSliceIterator(entries []types.Entry) memtable.Iterator {
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
