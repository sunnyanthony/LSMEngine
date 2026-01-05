package sstable

import (
	"container/list"
	"sync"
)

type blockCache struct {
	mu    sync.Mutex
	cap   int64
	size  int64
	ll    *list.List
	items map[uint64]*list.Element
}

type cacheEntry struct {
	key   uint64
	block *block
	size  int64
}

func newBlockCache(capBytes int64) *blockCache {
	if capBytes <= 0 {
		return nil
	}
	return &blockCache{
		cap:   capBytes,
		ll:    list.New(),
		items: make(map[uint64]*list.Element),
	}
}

func (c *blockCache) get(key uint64) (*block, bool) {
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

func (c *blockCache) add(key uint64, blk *block) {
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
		size:  int64(len(blk.data)),
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
