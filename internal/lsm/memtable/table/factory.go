package table

import (
	"fmt"
	"strings"

	"lsmengine/internal/lsm/memtable/core"
)

const (
	KindMap             = "map"
	KindSkipList        = "skiplist"
	KindShardedSkipList = "sharded-skiplist"
)

// FactoryForKind returns a memtable factory based on a short kind string.
func FactoryForKind(kind string, concurrency int, shards int, arenaBlockSize int) (core.Factory, error) {
	switch strings.ToLower(kind) {
	case "", "sharded", KindShardedSkipList:
		if shards > 0 {
			return func() core.Table { return NewShardedSkipListTableWithShardsAndArena(shards, arenaBlockSize) }, nil
		}
		return func() core.Table { return NewShardedSkipListTableWithArena(concurrency, arenaBlockSize) }, nil
	case KindSkipList:
		return func() core.Table { return NewSkipListTableWithArena(arenaBlockSize) }, nil
	case KindMap:
		return func() core.Table { return NewMapTableWithArena(arenaBlockSize) }, nil
	default:
		return nil, fmt.Errorf("unknown memtable kind %q", kind)
	}
}
