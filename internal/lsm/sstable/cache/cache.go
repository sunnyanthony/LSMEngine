package cache

import (
	"container/list"
	"sync"

	"lsmengine/internal/lsm/sstable/block"
	"lsmengine/internal/lsm/sstable/bloom"
	"lsmengine/internal/lsm/sstable/index"
)

// BlockCache caches on-disk data blocks only (no memtable overlap).
type BlockCache struct {
	mu    sync.Mutex
	cap   int64
	size  int64
	ll    *list.List
	items map[uint64]*list.Element
}

type cacheEntry struct {
	key   uint64
	block *block.Block
	size  int64
}

func NewBlockCache(capBytes int64) *BlockCache {
	if capBytes <= 0 {
		return nil
	}
	return &BlockCache{
		cap:   capBytes,
		ll:    list.New(),
		items: make(map[uint64]*list.Element),
	}
}

func (c *BlockCache) Get(key uint64) (*block.Block, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if ele, ok := c.items[key]; ok {
		c.ll.MoveToFront(ele)
		return ele.Value.(*cacheEntry).block, true
	}
	return nil, false
}

func (c *BlockCache) Add(key uint64, blk *block.Block) {
	if c == nil || blk == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if ele, ok := c.items[key]; ok {
		c.ll.MoveToFront(ele)
		ele.Value.(*cacheEntry).block = blk
		return
	}
	ent := &cacheEntry{
		key:   key,
		block: blk,
		size:  int64(blk.DataLen()),
	}
	ele := c.ll.PushFront(ent)
	c.items[key] = ele
	c.size += ent.size
	for c.size > c.cap && c.ll.Len() > 0 {
		tail := c.ll.Back()
		if tail == nil {
			break
		}
		c.ll.Remove(tail)
		entry := tail.Value.(*cacheEntry)
		delete(c.items, entry.key)
		c.size -= entry.size
	}
}

// IndexCache caches on-disk index blocks only.
type IndexCache struct {
	mu    sync.Mutex
	cap   int64
	size  int64
	ll    *list.List
	items map[uint64]*list.Element
}

type indexCacheEntry struct {
	key   uint64
	value []index.Entry
	size  int64
}

func NewIndexCache(capBytes int64) *IndexCache {
	if capBytes <= 0 {
		return nil
	}
	return &IndexCache{
		cap:   capBytes,
		ll:    list.New(),
		items: make(map[uint64]*list.Element),
	}
}

func (c *IndexCache) Get(key uint64) ([]index.Entry, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if ele, ok := c.items[key]; ok {
		c.ll.MoveToFront(ele)
		return ele.Value.(*indexCacheEntry).value, true
	}
	return nil, false
}

func (c *IndexCache) Add(key uint64, value []index.Entry) {
	if c == nil || value == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if ele, ok := c.items[key]; ok {
		c.ll.MoveToFront(ele)
		ent := ele.Value.(*indexCacheEntry)
		c.size -= ent.size
		ent.value = value
		ent.size = indexEntriesSize(value)
		c.size += ent.size
		c.evict()
		return
	}
	ent := &indexCacheEntry{
		key:   key,
		value: value,
		size:  indexEntriesSize(value),
	}
	ele := c.ll.PushFront(ent)
	c.items[key] = ele
	c.size += ent.size
	c.evict()
}

func (c *IndexCache) evict() {
	for c.size > c.cap && c.ll.Len() > 0 {
		tail := c.ll.Back()
		if tail == nil {
			break
		}
		c.ll.Remove(tail)
		ent := tail.Value.(*indexCacheEntry)
		delete(c.items, ent.key)
		c.size -= ent.size
	}
}

// FilterCache caches on-disk bloom/filter blocks only.
type FilterCache struct {
	mu    sync.Mutex
	cap   int64
	size  int64
	ll    *list.List
	items map[uint64]*list.Element
}

type filterCacheEntry struct {
	key   uint64
	value *bloom.Filter
	size  int64
}

func NewFilterCache(capBytes int64) *FilterCache {
	if capBytes <= 0 {
		return nil
	}
	return &FilterCache{
		cap:   capBytes,
		ll:    list.New(),
		items: make(map[uint64]*list.Element),
	}
}

func (c *FilterCache) Get(key uint64) (*bloom.Filter, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if ele, ok := c.items[key]; ok {
		c.ll.MoveToFront(ele)
		return ele.Value.(*filterCacheEntry).value, true
	}
	return nil, false
}

func (c *FilterCache) Add(key uint64, value *bloom.Filter) {
	if c == nil || value == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if ele, ok := c.items[key]; ok {
		c.ll.MoveToFront(ele)
		ent := ele.Value.(*filterCacheEntry)
		c.size -= ent.size
		ent.value = value
		ent.size = bloomFilterSize(value)
		c.size += ent.size
		c.evict()
		return
	}
	ent := &filterCacheEntry{
		key:   key,
		value: value,
		size:  bloomFilterSize(value),
	}
	ele := c.ll.PushFront(ent)
	c.items[key] = ele
	c.size += ent.size
	c.evict()
}

func (c *FilterCache) evict() {
	for c.size > c.cap && c.ll.Len() > 0 {
		tail := c.ll.Back()
		if tail == nil {
			break
		}
		c.ll.Remove(tail)
		ent := tail.Value.(*filterCacheEntry)
		delete(c.items, ent.key)
		c.size -= ent.size
	}
}

func indexEntriesSize(entries []index.Entry) int64 {
	var size int64
	for _, e := range entries {
		size += int64(index.HeaderSize + len(e.Key))
	}
	return size
}

func bloomFilterSize(filter *bloom.Filter) int64 {
	if filter == nil {
		return 0
	}
	return int64(filter.SizeBytes())
}
