// Memtable interfaces and stats types.

package memtable

import (
	"context"
	"lsmengine/internal/lsm/memtable/skiplist"
	"lsmengine/pkg/lsm/types"
)

// Table defines the operations required by the LSM for an in-memory index.
type Table interface {
	Get(key []byte) (types.Entry, bool)
	ApplyOwned(entry types.Entry)
	ApplyBatchOwned(entries []types.Entry)
	Size() int
	Drain() []types.Entry
	Iter() Iterator
	Range(start, end []byte) Iterator
	CopyEntry(entry types.Entry) types.Entry

	// Writer tracking
	IncWriter() error
	DecWriter()
	WaitWriters(ctx context.Context) error
}

// Factory constructs a new memtable implementation.
type Factory func() Table

// Iterator walks entries in sorted key order.
type Iterator interface {
	Next() bool
	Entry() types.Entry
}

// Freezer marks a table immutable to allow fast-path iteration.
type Freezer interface {
	Freeze()
}

// StatsProvider exposes table metrics for tuning.
type StatsProvider interface {
	Stats() TableStats
}

// Resetter clears table state so it can be reused.
type Resetter interface {
	Reset()
}

// Compare orders keys. It should return -1, 0, or 1.
type Compare = skiplist.Compare

// DefaultCompare is lexicographic byte comparison.
var DefaultCompare = skiplist.DefaultCompare

type ShardStats struct {
	Entries int
	Bytes   int
}

type TableStats struct {
	Entries     int
	Bytes       int
	ArenaBytes  int64
	ArenaBlocks int
	Shards      []ShardStats
}
