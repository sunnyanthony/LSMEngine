package sstable

import (
	"fmt"
	"testing"

	"lsmengine/pkg/lsm/types"
)

func TestSSTableWriterReaderGet(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.BlockTargetBytes = 64
	opts.BlockMaxBytes = 128

	writer, err := NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	entries := []types.Entry{
		{Key: []byte("b"), Value: []byte("2"), Seq: 2},
		{Key: []byte("a"), Value: []byte("1"), Seq: 1},
		{Key: []byte("c"), Value: []byte("3"), Seq: 3, Tombstone: true},
	}
	table, err := writer.Flush(entries)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	defer table.Close()

	got, ok := table.Get([]byte("a"))
	if !ok || string(got.Value) != "1" {
		t.Fatalf("get a: ok=%v val=%q", ok, got.Value)
	}
	got, ok = table.Get([]byte("c"))
	if !ok || !got.Tombstone {
		t.Fatalf("get c: ok=%v tombstone=%v", ok, got.Tombstone)
	}
	if _, ok := table.Get([]byte("z")); ok {
		t.Fatalf("get z: expected missing")
	}
}

func TestSSTableRange(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.BlockTargetBytes = 96
	opts.BlockMaxBytes = 160
	opts.PrefetchBlocks = 1
	opts.BlockCacheBytes = 1 << 20

	writer, err := NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	table, err := writer.Flush(makeEntries(50))
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	defer table.Close()

	it := table.Range([]byte("k010"), []byte("k020"))
	var got []string
	for it.Next() {
		got = append(got, string(it.Entry().Key))
	}
	if len(got) != 10 {
		t.Fatalf("range count=%d", len(got))
	}
	if got[0] != "k010" || got[len(got)-1] != "k019" {
		t.Fatalf("range bounds: %v", got)
	}
}

func TestSSTableOptionsDisableFeatures(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.BloomBitsPerKey = 0
	opts.BlockCacheBytes = 0
	opts.Compression = CompressionNone

	writer, err := NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	table, err := writer.Flush(makeEntries(10))
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	defer table.Close()

	if table.reader.filter != nil {
		t.Fatalf("expected bloom filter disabled")
	}
	if table.reader.cache != nil {
		t.Fatalf("expected block cache disabled")
	}
}

func makeEntries(n int) []types.Entry {
	entries := make([]types.Entry, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%03d", i)
		val := fmt.Sprintf("v%03d", i)
		entries[i] = types.Entry{
			Key:   []byte(key),
			Value: []byte(val),
			Seq:   uint64(i + 1),
		}
	}
	return entries
}
