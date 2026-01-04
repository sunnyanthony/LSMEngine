package memtable

import (
	"fmt"
	"strings"
)

const (
	KindMap             = "map"
	KindSkipList        = "skiplist"
	KindShardedSkipList = "sharded-skiplist"
)

// FactoryForKind returns a memtable factory based on a short kind string.
func FactoryForKind(kind string, concurrency int, shards int, arenaBlockSize int) (Factory, error) {
	switch strings.ToLower(kind) {
	case "", "sharded", KindShardedSkipList:
		if shards > 0 {
			return func() Table { return NewShardedSkipListTableWithShardsAndArena(shards, arenaBlockSize) }, nil
		}
		return func() Table { return NewShardedSkipListTableWithArena(concurrency, arenaBlockSize) }, nil
	case KindSkipList:
		return func() Table { return NewSkipListTableWithArena(arenaBlockSize) }, nil
	case KindMap:
		return func() Table { return NewMapTableWithArena(arenaBlockSize) }, nil
	default:
		return nil, fmt.Errorf("unknown memtable kind %q", kind)
	}
}
