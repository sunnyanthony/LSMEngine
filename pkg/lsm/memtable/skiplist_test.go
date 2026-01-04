package memtable

import (
	"bytes"
	"testing"

	"lsmengine/pkg/lsm/types"
)

func TestSkipListTableIter(t *testing.T) {
	table := NewSkipListTable()
	table.Put([]byte("b"), []byte("2"))
	table.Put([]byte("a"), []byte("1"))
	table.Put([]byte("c"), []byte("3"))

	it := table.Iter()
	var keys [][]byte
	for it.Next() {
		keys = append(keys, it.Entry().Key)
	}
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}
	if !bytes.Equal(keys[0], []byte("a")) || !bytes.Equal(keys[1], []byte("b")) || !bytes.Equal(keys[2], []byte("c")) {
		t.Fatalf("unexpected order: %v", keys)
	}
}

func TestSkipListTableRangeBounds(t *testing.T) {
	table := NewSkipListTable()
	table.Put([]byte("a"), []byte("1"))
	table.Put([]byte("b"), []byte("2"))
	table.Put([]byte("c"), []byte("3"))
	table.Put([]byte("d"), []byte("4"))

	it := table.Range([]byte("b"), []byte("d"))
	var keys [][]byte
	for it.Next() {
		keys = append(keys, it.Entry().Key)
	}
	if len(keys) != 2 || !bytes.Equal(keys[0], []byte("b")) || !bytes.Equal(keys[1], []byte("c")) {
		t.Fatalf("unexpected range keys: %v", keys)
	}
}

func TestSkipListTableRangeEmpty(t *testing.T) {
	table := NewSkipListTable()
	table.Put([]byte("a"), []byte("1"))
	table.Put([]byte("b"), []byte("2"))

	it := table.Range([]byte("d"), []byte("f"))
	if it.Next() {
		t.Fatalf("expected empty range")
	}
}

func TestShardedSkipListIter(t *testing.T) {
	table := NewShardedSkipListTable(2)
	table.Put([]byte("b"), []byte("2"))
	table.Put([]byte("a"), []byte("1"))
	table.Put([]byte("c"), []byte("3"))

	it := table.Iter()
	var keys [][]byte
	for it.Next() {
		keys = append(keys, it.Entry().Key)
	}
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}
	if !bytes.Equal(keys[0], []byte("a")) || !bytes.Equal(keys[1], []byte("b")) || !bytes.Equal(keys[2], []byte("c")) {
		t.Fatalf("unexpected order: %v", keys)
	}
}

func TestShardedSkipListRange(t *testing.T) {
	table := NewShardedSkipListTable(2)
	table.Put([]byte("a"), []byte("1"))
	table.Put([]byte("b"), []byte("2"))
	table.Put([]byte("c"), []byte("3"))
	table.Put([]byte("d"), []byte("4"))

	it := table.Range([]byte("b"), []byte("d"))
	var keys [][]byte
	for it.Next() {
		keys = append(keys, it.Entry().Key)
	}
	if len(keys) != 2 || !bytes.Equal(keys[0], []byte("b")) || !bytes.Equal(keys[1], []byte("c")) {
		t.Fatalf("unexpected range keys: %v", keys)
	}
}

func TestShardedSkipListRangeEmpty(t *testing.T) {
	table := NewShardedSkipListTable(2)
	table.Put([]byte("a"), []byte("1"))
	table.Put([]byte("b"), []byte("2"))

	it := table.Range([]byte("d"), []byte("f"))
	if it.Next() {
		t.Fatalf("expected empty range")
	}
}

func TestShardedSkipListRangeNilStart(t *testing.T) {
	table := NewShardedSkipListTable(2)
	table.Put([]byte("a"), []byte("1"))
	table.Put([]byte("b"), []byte("2"))
	table.Put([]byte("c"), []byte("3"))

	it := table.Range(nil, []byte("c"))
	var keys [][]byte
	for it.Next() {
		keys = append(keys, it.Entry().Key)
	}
	if len(keys) != 2 || !bytes.Equal(keys[0], []byte("a")) || !bytes.Equal(keys[1], []byte("b")) {
		t.Fatalf("unexpected range keys: %v", keys)
	}
}

func TestShardedSkipListRangeNilEnd(t *testing.T) {
	table := NewShardedSkipListTable(2)
	table.Put([]byte("a"), []byte("1"))
	table.Put([]byte("b"), []byte("2"))
	table.Put([]byte("c"), []byte("3"))

	it := table.Range([]byte("b"), nil)
	var keys [][]byte
	for it.Next() {
		keys = append(keys, it.Entry().Key)
	}
	if len(keys) != 2 || !bytes.Equal(keys[0], []byte("b")) || !bytes.Equal(keys[1], []byte("c")) {
		t.Fatalf("unexpected range keys: %v", keys)
	}
}

func TestSkipListTableSizeBytesOverwrite(t *testing.T) {
	table := NewSkipListTable().(*SkipListTable)
	table.Put([]byte("a"), []byte("1"))
	if table.Size() != 2 {
		t.Fatalf("expected size 2, got %d", table.Size())
	}
	table.Put([]byte("a"), []byte("123"))
	if table.Size() != 4 {
		t.Fatalf("expected size 4 after overwrite, got %d", table.Size())
	}
}

func TestSkipListTableApplyBatchOwned(t *testing.T) {
	table := NewSkipListTable().(*SkipListTable)
	table.ApplyBatchOwned([]types.Entry{
		{Key: []byte("a"), Value: []byte("1"), Seq: 1},
		{Key: []byte("b"), Value: []byte("2"), Seq: 2},
	})
	if got, ok := table.Get([]byte("a")); !ok || !bytes.Equal(got.Value, []byte("1")) {
		t.Fatalf("expected batch applied entry a")
	}
	if got, ok := table.Get([]byte("b")); !ok || !bytes.Equal(got.Value, []byte("2")) {
		t.Fatalf("expected batch applied entry b")
	}
}

func TestShardedSkipListStats(t *testing.T) {
	table := NewShardedSkipListTableWithShards(4).(*ShardedSkipListTable)
	table.Put([]byte("a"), []byte("1"))
	table.Put([]byte("b"), []byte("22"))
	stats := table.Stats()
	if stats.Entries != 2 {
		t.Fatalf("expected 2 entries, got %d", stats.Entries)
	}
	if stats.Bytes != 1+1+1+2 {
		t.Fatalf("expected bytes=5, got %d", stats.Bytes)
	}
	if len(stats.Shards) != 4 {
		t.Fatalf("expected 4 shard stats, got %d", len(stats.Shards))
	}
}

func TestShardedSkipListRangeInvalidBounds(t *testing.T) {
	table := NewShardedSkipListTable(2)
	table.Put([]byte("a"), []byte("1"))
	table.Put([]byte("b"), []byte("2"))

	it := table.Range([]byte("d"), []byte("b"))
	if it.Next() {
		t.Fatalf("expected empty range for start >= end")
	}
}

func TestSkipListTableRangeInvalidBounds(t *testing.T) {
	table := NewSkipListTable()
	table.Put([]byte("a"), []byte("1"))
	table.Put([]byte("b"), []byte("2"))

	it := table.Range([]byte("d"), []byte("b"))
	if it.Next() {
		t.Fatalf("expected empty range for start >= end")
	}
}

func TestSkipListTableApplyCopiesEntry(t *testing.T) {
	table := NewSkipListTable().(*SkipListTable)
	key := []byte("alpha")
	val := []byte("one")

	table.Apply(types.Entry{Key: key, Value: val, Seq: 1})
	key[0] = 'z'
	val[0] = 'x'

	got, ok := table.Get([]byte("alpha"))
	if !ok {
		t.Fatalf("expected key to exist")
	}
	if !bytes.Equal(got.Value, []byte("one")) {
		t.Fatalf("expected value copy, got %q", got.Value)
	}
}

func TestShardedSkipListApplyCopiesEntry(t *testing.T) {
	table := NewShardedSkipListTable(2)
	key := []byte("alpha")
	val := []byte("one")

	table.Apply(types.Entry{Key: key, Value: val, Seq: 1})
	key[0] = 'z'
	val[0] = 'x'

	got, ok := table.Get([]byte("alpha"))
	if !ok {
		t.Fatalf("expected key to exist")
	}
	if !bytes.Equal(got.Value, []byte("one")) {
		t.Fatalf("expected value copy, got %q", got.Value)
	}
}

func TestSkipListTableStats(t *testing.T) {
	table := NewSkipListTable().(*SkipListTable)
	table.Put([]byte("a"), []byte("1"))
	table.Put([]byte("b"), []byte("22"))

	stats := table.Stats()
	if stats.Entries != 2 {
		t.Fatalf("expected 2 entries, got %d", stats.Entries)
	}
	if stats.Bytes != 5 {
		t.Fatalf("expected bytes=5, got %d", stats.Bytes)
	}
}

func TestSkipListTableApplyConcurrentDoesNotPanic(t *testing.T) {
	runConcurrentApply(t, NewSkipListTable(), 4*1024)
}

func TestShardedSkipListApplyConcurrentDoesNotPanic(t *testing.T) {
	runConcurrentApply(t, NewShardedSkipListTable(2), 4*1024)
}
