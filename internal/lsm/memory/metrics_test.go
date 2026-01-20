package memory

import (
	"testing"

	"lsmengine/pkg/lsm/types"
)

func TestMetricsCopyCounts(t *testing.T) {
	ResetMetrics()
	EnableMetrics()
	defer DisableMetrics()

	CopyBytes(nil, []byte("abc"))
	entry := typesEntry("k", "v")
	_ = CopyEntry(nil, entry)

	snap := MetricsSnapshot()
	if snap.EntryCopies != 3 {
		t.Fatalf("expected 3 copies, got %d", snap.EntryCopies)
	}
	if snap.EntryBytes != 5 {
		t.Fatalf("expected 5 bytes copied, got %d", snap.EntryBytes)
	}
}

func TestMetricsBufferPoolReuse(t *testing.T) {
	ResetMetrics()
	EnableMetrics()
	defer DisableMetrics()

	pool := NewBufferPool(16)
	buf := pool.Get(8)
	pool.Put(buf)
	_ = pool.Get(4)

	snap := MetricsSnapshot()
	if snap.BufferGets != 2 {
		t.Fatalf("expected 2 buffer gets, got %d", snap.BufferGets)
	}
	if snap.BufferHits != 1 {
		t.Fatalf("expected 1 buffer hit, got %d", snap.BufferHits)
	}
	if snap.BufferMiss != 1 {
		t.Fatalf("expected 1 buffer miss, got %d", snap.BufferMiss)
	}
	if snap.BufferPuts != 1 {
		t.Fatalf("expected 1 buffer put, got %d", snap.BufferPuts)
	}
}

func TestMetricsReaderPoolReuse(t *testing.T) {
	ResetMetrics()
	EnableMetrics()
	defer DisableMetrics()

	pool := NewReaderPool(8)
	r := pool.Get(nil)
	pool.Put(r)
	_ = pool.Get(nil)

	snap := MetricsSnapshot()
	if snap.ReaderGets != 2 {
		t.Fatalf("expected 2 reader gets, got %d", snap.ReaderGets)
	}
	if snap.ReaderHits != 1 {
		t.Fatalf("expected 1 reader hit, got %d", snap.ReaderHits)
	}
	if snap.ReaderMiss != 1 {
		t.Fatalf("expected 1 reader miss, got %d", snap.ReaderMiss)
	}
	if snap.ReaderPuts != 1 {
		t.Fatalf("expected 1 reader put, got %d", snap.ReaderPuts)
	}
}

func typesEntry(key, value string) types.Entry {
	return types.Entry{
		Key:   []byte(key),
		Value: []byte(value),
	}
}
