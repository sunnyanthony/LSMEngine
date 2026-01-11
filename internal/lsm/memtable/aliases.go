package memtable

import (
	"lsmengine/internal/lsm/memtable/arena"
	"lsmengine/internal/lsm/memtable/core"
	"lsmengine/internal/lsm/memtable/table"
)

type Table = core.Table
type Factory = core.Factory
type Iterator = core.Iterator
type Freezer = core.Freezer
type StatsProvider = core.StatsProvider
type Resetter = core.Resetter
type Compare = core.Compare
type ShardStats = core.ShardStats
type TableStats = core.TableStats
type Arena = arena.Arena
type ArenaOptions = arena.Options
type ArenaStats = arena.Stats
type MapTable = table.MapTable
type SkipListTable = table.SkipListTable
type ShardedSkipListTable = table.ShardedSkipListTable

var DefaultCompare = core.DefaultCompare

const DefaultArenaBlockSize = arena.DefaultArenaBlockSize

const (
	KindMap             = table.KindMap
	KindSkipList        = table.KindSkipList
	KindShardedSkipList = table.KindShardedSkipList
)

func NewArena(blockSize int) *Arena {
	return arena.NewArena(blockSize)
}

func NewArenaWithOptions(opt ArenaOptions) *Arena {
	return arena.NewArenaWithOptions(opt)
}

func FactoryForKind(kind string, concurrency int, shards int, arenaBlockSize int) (Factory, error) {
	return table.FactoryForKind(kind, concurrency, shards, arenaBlockSize)
}

func NewMapTable() Table {
	return table.NewMapTable()
}

func NewMapTableWithArena(blockSize int) Table {
	return table.NewMapTableWithArena(blockSize)
}

func NewSkipListTable() Table {
	return table.NewSkipListTable()
}

func NewSkipListTableWithArena(blockSize int) Table {
	return table.NewSkipListTableWithArena(blockSize)
}

func NewShardedSkipListTable(concurrency int) Table {
	return table.NewShardedSkipListTable(concurrency)
}

func NewShardedSkipListTableWithShards(shards int) Table {
	return table.NewShardedSkipListTableWithShards(shards)
}

func NewShardedSkipListTableWithArena(concurrency int, blockSize int) Table {
	return table.NewShardedSkipListTableWithArena(concurrency, blockSize)
}

func NewShardedSkipListTableWithShardsAndArena(shards int, blockSize int) Table {
	return table.NewShardedSkipListTableWithShardsAndArena(shards, blockSize)
}
