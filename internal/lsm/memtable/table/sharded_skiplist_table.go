package table

import (
	"container/heap"
	"runtime"
	"sync"
	"sync/atomic"

	"lsmengine/internal/lsm/memtable/arena"
	"lsmengine/internal/lsm/memtable/core"
	"lsmengine/internal/lsm/memtable/skiplist"
	"lsmengine/pkg/lsm/types"
)

type shard struct {
	mu      sync.RWMutex
	list    *skiplist.SkipList
	entries int
	bytes   int
	arena   *arena.Arena
}

// ShardedSkipListTable shards keys across multiple skiplists for lower contention.
type ShardedSkipListTable struct {
	shards    []shard
	mask      uint64
	seq       uint64
	sizeBytes int64
	cmp       core.Compare
}

func NewShardedSkipListTable(concurrency int) core.Table {
	return NewShardedSkipListTableWithArena(concurrency, arena.DefaultArenaBlockSize)
}

func NewShardedSkipListTableWithShards(shards int) core.Table {
	return NewShardedSkipListTableWithShardsAndArena(shards, arena.DefaultArenaBlockSize)
}

func NewShardedSkipListTableWithArena(concurrency int, blockSize int) core.Table {
	return newShardedSkipListTable(shardCount(concurrency), blockSize)
}

func NewShardedSkipListTableWithShardsAndArena(shards int, blockSize int) core.Table {
	if shards < 1 {
		shards = 1
	}
	return newShardedSkipListTable(nextPow2(shards), blockSize)
}

func newShardedSkipListTable(shards int, blockSize int) *ShardedSkipListTable {
	s := make([]shard, shards)
	for i := range s {
		s[i].list = skiplist.New()
		s[i].arena = arena.NewArena(blockSize)
	}
	return &ShardedSkipListTable{
		shards: s,
		mask:   uint64(shards - 1),
		cmp:    core.DefaultCompare,
	}
}

// ApplyOwned inserts an entry without copying key/value.
func (t *ShardedSkipListTable) ApplyOwned(entry types.Entry) {
	t.bumpSeq(entry.Seq)
	s := t.pick(entry.Key)
	s.mu.Lock()
	inserted, prev, replaced := s.list.Upsert(entry)
	if inserted {
		s.entries++
		s.bytes += entrySize(entry)
		atomic.AddInt64(&t.sizeBytes, int64(entrySize(entry)))
	} else if replaced {
		s.bytes += entrySize(entry) - entrySize(prev)
		atomic.AddInt64(&t.sizeBytes, int64(entrySize(entry)-entrySize(prev)))
	}
	s.mu.Unlock()
}

// CopyEntry copies key/value into the shard-owned arena without inserting.
func (t *ShardedSkipListTable) CopyEntry(entry types.Entry) types.Entry {
	s := t.pick(entry.Key)
	return s.copyEntry(entry)
}

// ApplyBatchOwned applies entries without copying key/value.
func (t *ShardedSkipListTable) ApplyBatchOwned(entries []types.Entry) {
	if len(entries) == 0 {
		return
	}
	maxSeq := entries[0].Seq
	sharded := make([][]types.Entry, len(t.shards))
	for _, entry := range entries {
		if entry.Seq > maxSeq {
			maxSeq = entry.Seq
		}
		idx := hashKey(entry.Key) & t.mask
		sharded[idx] = append(sharded[idx], entry)
	}
	t.bumpSeq(maxSeq)
	for i := range sharded {
		if len(sharded[i]) == 0 {
			continue
		}
		s := &t.shards[i]
		s.mu.Lock()
		for _, entry := range sharded[i] {
			inserted, prev, replaced := s.list.Upsert(entry)
			if inserted {
				s.entries++
				s.bytes += entrySize(entry)
				atomic.AddInt64(&t.sizeBytes, int64(entrySize(entry)))
			} else if replaced {
				s.bytes += entrySize(entry) - entrySize(prev)
				atomic.AddInt64(&t.sizeBytes, int64(entrySize(entry)-entrySize(prev)))
			}
		}
		s.mu.Unlock()
	}
}

func (t *ShardedSkipListTable) Get(key []byte) (types.Entry, bool) {
	s := t.pick(key)
	s.mu.RLock()
	entry, ok := s.list.Get(key)
	s.mu.RUnlock()
	return entry, ok
}

func (t *ShardedSkipListTable) Size() int {
	return int(atomic.LoadInt64(&t.sizeBytes))
}

func (t *ShardedSkipListTable) Stats() core.TableStats {
	stats := core.TableStats{
		Bytes: int(atomic.LoadInt64(&t.sizeBytes)),
	}
	if len(t.shards) == 0 {
		return stats
	}
	stats.Shards = make([]core.ShardStats, 0, len(t.shards))
	var arenaBytes int64
	var arenaBlocks int
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.RLock()
		shardStats := core.ShardStats{
			Entries: s.entries,
			Bytes:   s.bytes,
		}
		s.mu.RUnlock()
		if s.arena != nil {
			a := s.arena.Stats()
			arenaBytes += a.UsedBytes
			arenaBlocks += a.Blocks
		}
		stats.Shards = append(stats.Shards, shardStats)
		stats.Entries += shardStats.Entries
	}
	stats.ArenaBytes = arenaBytes
	stats.ArenaBlocks = arenaBlocks
	return stats
}

// Drain returns entries in sorted key order and clears the table.
func (t *ShardedSkipListTable) Drain() []types.Entry {
	it := t.Iter()
	var out []types.Entry
	for it.Next() {
		out = append(out, it.Entry())
	}
	for i := range t.shards {
		t.shards[i].mu.Lock()
		t.shards[i].list = skiplist.New()
		t.shards[i].entries = 0
		t.shards[i].bytes = 0
		t.shards[i].mu.Unlock()
	}
	atomic.StoreInt64(&t.sizeBytes, 0)
	return out
}

func (t *ShardedSkipListTable) Iter() core.Iterator {
	return t.Range(nil, nil)
}

func (t *ShardedSkipListTable) Range(start, end []byte) core.Iterator {
	if len(start) > 0 && len(end) > 0 && t.cmp(start, end) >= 0 {
		return newSliceIterator(nil)
	}
	entries := t.snapshotRange(start, end)
	return newSliceIterator(entries)
}

// Freeze marks the table immutable; no-op for sharded skiplists.
func (t *ShardedSkipListTable) Freeze() {
}

// Reset clears the table so it can be reused.
func (t *ShardedSkipListTable) Reset() {
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.Lock()
		s.list = skiplist.New()
		s.entries = 0
		s.bytes = 0
		if s.arena != nil {
			s.arena.Reset()
		}
		s.mu.Unlock()
	}
	atomic.StoreInt64(&t.sizeBytes, 0)
	atomic.StoreUint64(&t.seq, 0)
}

func (t *ShardedSkipListTable) pick(key []byte) *shard {
	idx := hashKey(key) & t.mask
	return &t.shards[idx]
}

func (t *ShardedSkipListTable) bumpSeq(seq uint64) {
	for {
		cur := atomic.LoadUint64(&t.seq)
		if seq <= cur || atomic.CompareAndSwapUint64(&t.seq, cur, seq) {
			return
		}
	}
}

func shardCount(concurrency int) int {
	if concurrency < 1 {
		concurrency = 1
	}
	n := runtime.GOMAXPROCS(0) * concurrency
	return nextPow2(n)
}

func nextPow2(v int) int {
	if v <= 1 {
		return 1
	}
	n := 1
	for n < v {
		n <<= 1
	}
	return n
}

func (t *ShardedSkipListTable) snapshotRange(start, end []byte) []types.Entry {
	if len(t.shards) == 0 {
		return nil
	}
	segments := make([][]types.Entry, 0, len(t.shards))
	total := 0
	for i := range t.shards {
		s := &t.shards[i]
		entries := t.collectShardEntries(s, start, end)
		if len(entries) == 0 {
			continue
		}
		total += len(entries)
		segments = append(segments, entries)
	}
	if len(segments) == 0 {
		return nil
	}
	if len(segments) == 1 {
		return segments[0]
	}
	return mergeEntries(segments, total, t.cmp)
}

func (t *ShardedSkipListTable) collectShardEntries(s *shard, start, end []byte) []types.Entry {
	s.mu.RLock()
	iter := s.list.IterFrom(start)
	var entries []types.Entry
	for iter.Next() {
		entry := iter.Entry()
		if len(end) > 0 && t.cmp(entry.Key, end) >= 0 {
			break
		}
		entries = append(entries, entry)
	}
	s.mu.RUnlock()
	return entries
}

func (s *shard) copyEntry(entry types.Entry) types.Entry {
	entry.Key = s.copyBytes(entry.Key)
	entry.Value = s.copyBytes(entry.Value)
	return entry
}

func (s *shard) copyBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	if s.arena == nil {
		return append([]byte(nil), src...)
	}
	if dst := s.arena.AllocCopy(src); dst != nil {
		return dst
	}
	return append([]byte(nil), src...)
}

type sliceCursor struct {
	entries []types.Entry
	idx     int
}

type sliceItem struct {
	cursor *sliceCursor
	entry  types.Entry
}

type sliceHeap struct {
	items []sliceItem
	cmp   core.Compare
}

func (h sliceHeap) Len() int { return len(h.items) }
func (h sliceHeap) Less(i, j int) bool {
	return h.cmp(h.items[i].entry.Key, h.items[j].entry.Key) < 0
}
func (h sliceHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *sliceHeap) Push(x any) {
	h.items = append(h.items, x.(sliceItem))
}
func (h *sliceHeap) Pop() any {
	n := len(h.items)
	item := h.items[n-1]
	h.items = h.items[:n-1]
	return item
}

func mergeEntries(segments [][]types.Entry, total int, cmp core.Compare) []types.Entry {
	h := &sliceHeap{cmp: cmp}
	h.items = make([]sliceItem, 0, len(segments))
	for _, entries := range segments {
		cursor := &sliceCursor{entries: entries}
		h.items = append(h.items, sliceItem{
			cursor: cursor,
			entry:  entries[0],
		})
	}
	heap.Init(h)
	out := make([]types.Entry, 0, total)
	for h.Len() > 0 {
		item := heap.Pop(h).(sliceItem)
		out = append(out, item.entry)
		item.cursor.idx++
		if item.cursor.idx < len(item.cursor.entries) {
			item.entry = item.cursor.entries[item.cursor.idx]
			heap.Push(h, item)
		}
	}
	return out
}
