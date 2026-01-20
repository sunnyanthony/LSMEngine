package cache

import (
	"testing"

	"lsmengine/internal/lsm/sstable/bloom"
	"lsmengine/internal/lsm/sstable/format"
	"lsmengine/pkg/lsm/types"
)

func newTestBlock(t *testing.T, key string) *format.Block {
	t.Helper()
	builder := format.NewBuilder(4, false, 0, 0)
	builder.Add(types.Entry{Key: []byte(key), Value: []byte("v")})
	payload := builder.Finish()
	blk, err := format.Decode(payload)
	if err != nil {
		t.Fatalf("decode block: %v", err)
	}
	return blk
}

func TestBlockCacheEviction(t *testing.T) {
	blk1 := newTestBlock(t, "a")
	blk2 := newTestBlock(t, "b")
	cache := NewBlockCache(int64(blk1.DataLen() + blk2.DataLen()))
	if cache == nil {
		t.Fatalf("expected cache")
	}
	cache.Add(1, blk1)
	cache.Add(2, blk2)
	if _, ok := cache.Get(1); !ok {
		t.Fatalf("expected key 1 to be cached")
	}
	// Touch key 1 to make key 2 LRU, then add a new block to trigger eviction.
	cache.Get(1)
	cache.Add(3, newTestBlock(t, "c"))
	if _, ok := cache.Get(2); ok {
		t.Fatalf("expected key 2 to be evicted")
	}
	if _, ok := cache.Get(1); !ok {
		t.Fatalf("expected key 1 to remain")
	}
}

func TestIndexCacheEviction(t *testing.T) {
	cache := NewIndexCache(40)
	entries := []format.IndexEntry{{Key: []byte("a"), Offset: 1, Length: 1}}
	cache.Add(1, entries)
	cache.Add(2, []format.IndexEntry{{Key: []byte("b"), Offset: 2, Length: 1}})
	if _, ok := cache.Get(1); !ok {
		t.Fatalf("expected key 1 cached")
	}
	cache.Add(3, []format.IndexEntry{{Key: []byte("c"), Offset: 3, Length: 1}})
	if _, ok := cache.Get(2); ok {
		t.Fatalf("expected key 2 evicted")
	}
}

func TestFilterCacheEviction(t *testing.T) {
	f1 := bloom.NewFilter(16, 8)
	capBytes := int64(f1.SizeBytes() * 2)
	cache := NewFilterCache(capBytes)
	cache.Add(1, f1)
	cache.Add(2, bloom.NewFilter(16, 8))
	if _, ok := cache.Get(1); !ok {
		t.Fatalf("expected key 1 cached")
	}
	cache.Add(3, bloom.NewFilter(16, 8))
	if _, ok := cache.Get(2); ok {
		t.Fatalf("expected key 2 evicted")
	}
}
