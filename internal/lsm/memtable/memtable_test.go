package memtable_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	memtable "lsmengine/internal/lsm/memtable"
	memtabletable "lsmengine/internal/lsm/memtable/table"
	"lsmengine/pkg/lsm/types"
)

func TestMemTablePutGet(t *testing.T) {
	mt := memtabletable.NewMapTable()
	entry := applyOwnedEntry(mt, []byte("alpha"), []byte("one"), false, 1)
	got, ok := mt.Get([]byte("alpha"))
	if !ok {
		t.Fatalf("expected key to exist")
	}
	if got.Seq != entry.Seq {
		t.Fatalf("seq mismatch")
	}
	if !bytes.Equal(got.Value, []byte("one")) {
		t.Fatalf("value mismatch")
	}
}

func TestMemTableDelete(t *testing.T) {
	mt := memtabletable.NewMapTable()
	applyOwnedEntry(mt, []byte("alpha"), []byte("one"), false, 1)
	del := applyOwnedEntry(mt, []byte("alpha"), nil, true, 2)
	if !del.Tombstone {
		t.Fatalf("expected tombstone")
	}
	got, ok := mt.Get([]byte("alpha"))
	if !ok || !got.Tombstone {
		t.Fatalf("expected tombstone in table, got %+v (ok=%v)", got, ok)
	}
}

func TestMemTableDrainSorted(t *testing.T) {
	mt := memtabletable.NewMapTable()
	applyOwnedEntry(mt, []byte("b"), []byte("2"), false, 1)
	applyOwnedEntry(mt, []byte("a"), []byte("1"), false, 2)
	entries := mt.Drain()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries")
	}
	if !bytes.Equal(entries[0].Key, []byte("a")) || !bytes.Equal(entries[1].Key, []byte("b")) {
		t.Fatalf("expected sorted keys, got %v %v", entries[0].Key, entries[1].Key)
	}
	if mt.Size() != 0 {
		t.Fatalf("expected empty after drain")
	}
}

func TestMemTableIterSorted(t *testing.T) {
	mt := memtabletable.NewMapTable()
	applyOwnedEntry(mt, []byte("b"), []byte("2"), false, 1)
	applyOwnedEntry(mt, []byte("a"), []byte("1"), false, 2)
	applyOwnedEntry(mt, []byte("c"), []byte("3"), false, 3)

	it := mt.Iter()
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

func TestMemTableRange(t *testing.T) {
	mt := memtabletable.NewMapTable()
	applyOwnedEntry(mt, []byte("a"), []byte("1"), false, 1)
	applyOwnedEntry(mt, []byte("b"), []byte("2"), false, 2)
	applyOwnedEntry(mt, []byte("c"), []byte("3"), false, 3)
	applyOwnedEntry(mt, []byte("d"), []byte("4"), false, 4)

	it := mt.Range([]byte("b"), []byte("d"))
	var keys [][]byte
	for it.Next() {
		keys = append(keys, it.Entry().Key)
	}
	if len(keys) != 2 || !bytes.Equal(keys[0], []byte("b")) || !bytes.Equal(keys[1], []byte("c")) {
		t.Fatalf("unexpected range keys: %v", keys)
	}
}

func TestMemTableRangeEmptyBounds(t *testing.T) {
	mt := memtabletable.NewMapTable()
	applyOwnedEntry(mt, []byte("b"), []byte("2"), false, 1)
	applyOwnedEntry(mt, []byte("a"), []byte("1"), false, 2)
	applyOwnedEntry(mt, []byte("c"), []byte("3"), false, 3)

	it := mt.Range(nil, nil)
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

func TestMemTableRangeNoMatch(t *testing.T) {
	mt := memtabletable.NewMapTable()
	applyOwnedEntry(mt, []byte("a"), []byte("1"), false, 1)
	applyOwnedEntry(mt, []byte("b"), []byte("2"), false, 2)

	it := mt.Range([]byte("d"), []byte("f"))
	if it.Next() {
		t.Fatalf("expected empty range")
	}
}

func TestMapTableSizeBytesOverwrite(t *testing.T) {
	mt := memtabletable.NewMapTable().(*memtabletable.MapTable)
	applyOwnedEntry(mt, []byte("a"), []byte("1"), false, 1)
	if mt.Size() != 2 {
		t.Fatalf("expected size 2, got %d", mt.Size())
	}
	applyOwnedEntry(mt, []byte("a"), []byte("123"), false, 2)
	if mt.Size() != 4 {
		t.Fatalf("expected size 4 after overwrite, got %d", mt.Size())
	}
}

func TestMapTableSizeBytesTombstone(t *testing.T) {
	mt := memtabletable.NewMapTable().(*memtabletable.MapTable)
	applyOwnedEntry(mt, []byte("a"), []byte("1"), false, 1)
	applyOwnedEntry(mt, []byte("a"), nil, true, 2)
	if mt.Size() != 1 {
		t.Fatalf("expected size 1 for tombstone, got %d", mt.Size())
	}
}

func TestMapTableApplyBatchOwned(t *testing.T) {
	mt := memtabletable.NewMapTable().(*memtabletable.MapTable)
	entries := []types.Entry{
		{Key: []byte("a"), Value: []byte("1"), Seq: 1},
		{Key: []byte("b"), Value: []byte("2"), Seq: 2},
	}
	for i := range entries {
		entries[i] = mt.CopyEntry(entries[i])
	}
	mt.ApplyBatchOwned(entries)
	if got, ok := mt.Get([]byte("a")); !ok || !bytes.Equal(got.Value, []byte("1")) {
		t.Fatalf("expected batch applied entry a")
	}
	if got, ok := mt.Get([]byte("b")); !ok || !bytes.Equal(got.Value, []byte("2")) {
		t.Fatalf("expected batch applied entry b")
	}
}

func TestMapTableRangeInvalidBounds(t *testing.T) {
	mt := memtabletable.NewMapTable()
	applyOwnedEntry(mt, []byte("a"), []byte("1"), false, 1)
	applyOwnedEntry(mt, []byte("b"), []byte("2"), false, 2)

	it := mt.Range([]byte("d"), []byte("b"))
	if it.Next() {
		t.Fatalf("expected empty range for start >= end")
	}
}

func TestMapTableApplyCopiesEntry(t *testing.T) {
	mt := memtabletable.NewMapTable().(*memtabletable.MapTable)
	key := []byte("alpha")
	val := []byte("one")

	entry := mt.CopyEntry(types.Entry{Key: key, Value: val, Seq: 1})
	mt.ApplyOwned(entry)
	key[0] = 'z'
	val[0] = 'x'

	got, ok := mt.Get([]byte("alpha"))
	if !ok {
		t.Fatalf("expected key to exist")
	}
	if !bytes.Equal(got.Value, []byte("one")) {
		t.Fatalf("expected value copy, got %q", got.Value)
	}
}

func TestMapTableStats(t *testing.T) {
	mt := memtabletable.NewMapTable().(*memtabletable.MapTable)
	applyOwnedEntry(mt, []byte("a"), []byte("1"), false, 1)
	applyOwnedEntry(mt, []byte("b"), []byte("22"), false, 2)

	stats := mt.Stats()
	if stats.Entries != 2 {
		t.Fatalf("expected 2 entries, got %d", stats.Entries)
	}
	if stats.Bytes != 5 {
		t.Fatalf("expected bytes=5, got %d", stats.Bytes)
	}
}

func TestMapTableApplyConcurrentDoesNotPanic(t *testing.T) {
	runConcurrentApply(t, memtabletable.NewMapTable(), 4*1024)
}

func TestMemtableCopyEntryEmptyValue(t *testing.T) {
	tables := []memtable.Table{
		memtabletable.NewMapTable(),
		memtabletable.NewSkipListTable(),
		memtabletable.NewShardedSkipListTable(2),
	}
	for _, table := range tables {
		entry := types.Entry{Key: []byte("k"), Value: nil, Seq: 1}
		copied := table.CopyEntry(entry)
		entry.Key[0] = 'z'
		if !bytes.Equal(copied.Key, []byte("k")) {
			t.Fatalf("expected copied key for %T", table)
		}
		if copied.Value != nil && len(copied.Value) != 0 {
			t.Fatalf("expected empty value for %T", table)
		}
	}
}

func TestMemtableCopyEntryEmptyKey(t *testing.T) {
	tables := []memtable.Table{
		memtabletable.NewMapTable(),
		memtabletable.NewSkipListTable(),
		memtabletable.NewShardedSkipListTable(2),
	}
	for _, table := range tables {
		val := []byte("v")
		entry := types.Entry{Key: nil, Value: val, Seq: 1}
		copied := table.CopyEntry(entry)
		val[0] = 'x'
		if len(copied.Key) != 0 {
			t.Fatalf("expected empty key for %T", table)
		}
		if !bytes.Equal(copied.Value, []byte("v")) {
			t.Fatalf("expected copied value for %T", table)
		}
	}
}

func TestMapTableConcurrentPutGet(t *testing.T) {
	runConcurrentPutGet(t, memtabletable.NewMapTable())
}

func TestSkipListTableConcurrentPutGet(t *testing.T) {
	runConcurrentPutGet(t, memtabletable.NewSkipListTable())
}

func TestShardedSkipListTableConcurrentPutGet(t *testing.T) {
	runConcurrentPutGet(t, memtabletable.NewShardedSkipListTable(2))
}

func runConcurrentApply(t *testing.T, table memtable.Table, valueSize int) {
	t.Helper()

	val := make([]byte, valueSize)
	var seq uint64
	goroutines := runtime.GOMAXPROCS(0) * 2
	if goroutines < 4 {
		goroutines = 4
	}
	iters := 1000
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			<-start
			var keyBuf [16]byte
			for j := 0; j < iters; j++ {
				idx := atomic.AddUint64(&seq, 1)
				binary.LittleEndian.PutUint64(keyBuf[:8], idx)
				entry := table.CopyEntry(types.Entry{Key: keyBuf[:], Value: val, Seq: idx})
				table.ApplyOwned(entry)
			}
		}()
	}
	close(start)
	wg.Wait()
	if table.Size() == 0 {
		t.Fatalf("expected entries after concurrent apply")
	}
}

func runConcurrentPutGet(t *testing.T, table memtable.Table) {
	t.Helper()

	val := []byte("v")
	var seq uint64
	goroutines := runtime.GOMAXPROCS(0) * 2
	if goroutines < 4 {
		goroutines = 4
	}
	iters := 500
	start := make(chan struct{})
	errCh := make(chan error, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			<-start
			var keyBuf [8]byte
			for j := 0; j < iters; j++ {
				idx := atomic.AddUint64(&seq, 1)
				binary.LittleEndian.PutUint64(keyBuf[:], idx)
				entry := table.CopyEntry(types.Entry{Key: keyBuf[:], Value: val, Seq: idx})
				table.ApplyOwned(entry)
				if _, ok := table.Get(keyBuf[:]); !ok {
					errCh <- errors.New("missing key after put")
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent put/get: %v", err)
		}
	}
	if table.Size() == 0 {
		t.Fatalf("expected entries after concurrent put/get")
	}
}

func applyOwnedEntry(table memtable.Table, key, value []byte, tombstone bool, seq uint64) types.Entry {
	entry := types.Entry{
		Key:       key,
		Value:     value,
		Tombstone: tombstone,
		Seq:       seq,
	}
	entry = table.CopyEntry(entry)
	table.ApplyOwned(entry)
	return entry
}
