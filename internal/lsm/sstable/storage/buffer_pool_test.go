package storage

import "testing"

func TestBufferPoolGetPut(t *testing.T) {
	pool := newBufferPool(64)
	if pool == nil {
		t.Fatalf("expected pool")
	}
	buf := pool.get(32)
	if len(buf) != 32 {
		t.Fatalf("expected buffer len 32, got %d", len(buf))
	}
	pool.put(buf)
	buf2 := pool.get(16)
	if len(buf2) != 16 {
		t.Fatalf("expected buffer len 16, got %d", len(buf2))
	}
}
