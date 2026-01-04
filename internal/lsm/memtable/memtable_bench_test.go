package memtable

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"testing"

	"lsmengine/pkg/lsm/types"
)

type benchTable struct {
	name string
	new  func() Table
}

const benchShardConcurrency = 2

var benchArenaSizes = []int{
	64 * 1024,
	256 * 1024,
}

var benchShardCounts = []int{
	0,
	16,
}

var benchTables = buildBenchTables()

func buildBenchTables() []benchTable {
	var tables []benchTable
	for _, size := range benchArenaSizes {
		arenaSize := size
		label := fmt.Sprintf("%dk", arenaSize/1024)
		tables = append(tables,
			benchTable{name: "map/arena" + label, new: func() Table { return NewMapTableWithArena(arenaSize) }},
			benchTable{name: "skiplist/arena" + label, new: func() Table { return NewSkipListTableWithArena(arenaSize) }},
		)
		for _, shards := range benchShardCounts {
			shardCount := shards
			if shardCount == 0 {
				name := "sharded/auto/arena" + label
				tables = append(tables, benchTable{
					name: name,
					new:  func() Table { return NewShardedSkipListTableWithArena(benchShardConcurrency, arenaSize) },
				})
				continue
			}
			name := fmt.Sprintf("sharded/%dshards/arena%s", shardCount, label)
			tables = append(tables, benchTable{
				name: name,
				new:  func() Table { return NewShardedSkipListTableWithShardsAndArena(shardCount, arenaSize) },
			})
		}
	}
	return tables
}

func BenchmarkMemtableApplySmall(b *testing.B) {
	benchmarkApply(b, 16)
}

func BenchmarkMemtableApplyLarge(b *testing.B) {
	benchmarkApply(b, 4*1024)
}

func BenchmarkMemtableApplyParallelSmall(b *testing.B) {
	benchmarkApplyParallel(b, 16)
}

func BenchmarkMemtableApplyParallelLarge(b *testing.B) {
	benchmarkApplyParallel(b, 4*1024)
}

func BenchmarkMemtableApplyOwnedSmall(b *testing.B) {
	benchmarkApplyOwned(b, 16)
}

func BenchmarkMemtableApplyOwnedLarge(b *testing.B) {
	benchmarkApplyOwned(b, 4*1024)
}

func benchmarkApply(b *testing.B, valueSize int) {
	for _, tc := range benchTables {
		b.Run(tc.name, func(b *testing.B) {
			table := tc.new()
			val := make([]byte, valueSize)
			var keyBuf [16]byte
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				binary.LittleEndian.PutUint64(keyBuf[:8], uint64(i))
				entry := table.CopyEntry(types.Entry{Key: keyBuf[:], Value: val, Seq: uint64(i + 1)})
				table.ApplyOwned(entry)
			}
		})
	}
}

func benchmarkApplyParallel(b *testing.B, valueSize int) {
	for _, tc := range benchTables {
		b.Run(tc.name, func(b *testing.B) {
			table := tc.new()
			val := make([]byte, valueSize)
			var seq uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var keyBuf [16]byte
				for pb.Next() {
					idx := atomic.AddUint64(&seq, 1)
					binary.LittleEndian.PutUint64(keyBuf[:8], idx)
					entry := table.CopyEntry(types.Entry{Key: keyBuf[:], Value: val, Seq: idx})
					table.ApplyOwned(entry)
				}
			})
		})
	}
}

func benchmarkApplyOwned(b *testing.B, valueSize int) {
	for _, tc := range benchTables {
		b.Run(tc.name, func(b *testing.B) {
			table := tc.new()
			val := make([]byte, valueSize)
			entries := make([]types.Entry, b.N)
			for i := 0; i < b.N; i++ {
				key := make([]byte, 16)
				binary.LittleEndian.PutUint64(key[:8], uint64(i))
				entries[i] = types.Entry{Key: key, Value: val, Seq: uint64(i + 1)}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				table.ApplyOwned(entries[i])
			}
		})
	}
}
