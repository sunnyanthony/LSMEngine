package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"testing"

	"lsmengine/internal/lsm/sstable/format"
	"lsmengine/internal/lsm/sstable/index"
	"lsmengine/internal/lsm/sstable/meta"
	"lsmengine/pkg/lsm/errs"
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

func TestSSTableGetView(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.BlockTargetBytes = 64
	opts.BlockMaxBytes = 128

	writer, err := NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	entries := []types.Entry{
		{Key: []byte("a"), Value: []byte("1"), Seq: 1},
		{Key: []byte("b"), Value: []byte("2"), Seq: 2},
	}
	table, err := writer.Flush(entries)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	defer table.Close()

	view, ok := table.GetView([]byte("a"))
	if !ok || string(view.Value) != "1" || view.Tombstone {
		t.Fatalf("get view: ok=%v val=%q tombstone=%v", ok, view.Value, view.Tombstone)
	}
	if _, ok := table.GetView([]byte("z")); ok {
		t.Fatalf("get view z: expected missing")
	}
}

func TestSSTableAdaptiveRestartInterval(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.BlockTargetBytes = 64
	opts.BlockMaxBytes = 128
	opts.RestartIntervalAdaptive = true
	opts.RestartIntervalMin = 4
	opts.RestartIntervalMax = 32

	writer, err := NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	table, err := writer.Flush(makeEntries(100))
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	defer table.Close()

	got, ok := table.Get([]byte("k050"))
	if !ok || string(got.Value) != "v050" {
		t.Fatalf("get k050: ok=%v val=%q", ok, got.Value)
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

func TestSSTableRangePrefetchBudget(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.BlockTargetBytes = 96
	opts.BlockMaxBytes = 160
	opts.BlockCacheBytes = 1 << 20
	opts.PrefetchAsync = true
	opts.PrefetchBudgetBlocks = 2

	writer, err := NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	table, err := writer.Flush(makeEntries(50))
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	defer table.Close()

	it := table.Range([]byte("k005"), []byte("k015"))
	for it.Next() {
		_ = it.Entry()
	}
	if err := it.Err(); err != nil {
		t.Fatalf("range err: %v", err)
	}
}

func TestSSTableUseMmap(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.UseMmap = true
	opts.BlockTargetBytes = 64
	opts.BlockMaxBytes = 128

	writer, err := NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	table, err := writer.Flush(makeEntries(20))
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	defer table.Close()

	got, ok := table.Get([]byte("k010"))
	if !ok || string(got.Value) != "v010" {
		t.Fatalf("get k010: ok=%v val=%q", ok, got.Value)
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

	if table.reader.meta.BloomLen != 0 || table.reader.meta.BloomOffset != 0 {
		t.Fatalf("expected bloom filter disabled")
	}
	if table.reader.opts.BlockCacheBytes != 0 {
		t.Fatalf("expected block cache disabled")
	}
}

func TestSSTableGetPrefersHighestSeq(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.BlockTargetBytes = 64
	opts.BlockMaxBytes = 128

	writer, err := NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	entries := []types.Entry{
		{Key: []byte("k"), Value: []byte("old"), Seq: 1},
		{Key: []byte("k"), Value: []byte("mid"), Seq: 2},
		{Key: []byte("k"), Value: nil, Seq: 3, Tombstone: true},
	}
	table, err := writer.Flush(entries)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	defer table.Close()

	got, ok := table.Get([]byte("k"))
	if !ok {
		t.Fatalf("expected key to exist")
	}
	if !got.Tombstone || got.Seq != 3 {
		t.Fatalf("expected tombstone seq=3, got tombstone=%v seq=%d", got.Tombstone, got.Seq)
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

func TestSSTableCorruptDataBlockFailFast(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.BlockTargetBytes = 64
	opts.BlockMaxBytes = 96

	writer, err := NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	table, err := writer.Flush(makeEntries(60))
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	table.Close()

	ft := readFooterForTest(t, table.Path)
	index := readIndexForTest(t, table.Path, ft)
	if len(index) == 0 {
		t.Fatalf("expected index entries")
	}
	corruptByte(t, table.Path, int64(index[0].Offset)+int64(index[0].Length)-1)

	table, err = LoadSSTable(table.Path, opts)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	defer table.Close()

	it := table.Range(nil, nil)
	if it.Next() {
		t.Fatalf("expected range to stop on corrupt block")
	}
	if !errors.Is(it.Err(), errs.ErrSSTableBadBlock) {
		t.Fatalf("expected bad block error, got %v", it.Err())
	}
}

func TestSSTableCorruptDataBlockSkipBlock(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.BlockTargetBytes = 64
	opts.BlockMaxBytes = 96

	writer, err := NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	table, err := writer.Flush(makeEntries(80))
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	table.Close()

	ft := readFooterForTest(t, table.Path)
	index := readIndexForTest(t, table.Path, ft)
	if len(index) < 2 {
		t.Fatalf("expected multiple data blocks")
	}
	corruptByte(t, table.Path, int64(index[0].Offset)+int64(index[0].Length)-1)

	opts.CorruptionPolicy = CorruptionSkipBlock
	table, err = LoadSSTable(table.Path, opts)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	defer table.Close()

	it := table.Range(nil, nil)
	if !it.Next() {
		t.Fatalf("expected range to continue after corrupt block")
	}
	if string(it.Entry().Key) != string(index[1].Key) {
		t.Fatalf("expected range to start at %q, got %q", index[1].Key, it.Entry().Key)
	}
	for it.Next() {
	}
	if it.Err() != nil {
		t.Fatalf("expected no error, got %v", it.Err())
	}
}

func TestSSTableCorruptDataBlockDropTable(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.BlockTargetBytes = 64
	opts.BlockMaxBytes = 96

	writer, err := NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	table, err := writer.Flush(makeEntries(80))
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	table.Close()

	ft := readFooterForTest(t, table.Path)
	index := readIndexForTest(t, table.Path, ft)
	if len(index) < 2 {
		t.Fatalf("expected multiple data blocks")
	}
	corruptByte(t, table.Path, int64(index[0].Offset)+int64(index[0].Length)-1)

	opts.CorruptionPolicy = CorruptionDropTable
	table, err = LoadSSTable(table.Path, opts)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	defer table.Close()

	it := table.Range(nil, nil)
	if it.Next() {
		t.Fatalf("expected range to stop on corrupt block")
	}
	if it.Err() != nil {
		t.Fatalf("expected no error, got %v", it.Err())
	}
	if _, ok := table.Get(index[1].Key); ok {
		t.Fatalf("expected dropped table to return miss")
	}
}

func TestSSTableCorruptCompressionID(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.BlockTargetBytes = 64
	opts.BlockMaxBytes = 96

	writer, err := NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	table, err := writer.Flush(makeEntries(40))
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	table.Close()

	ft := readFooterForTest(t, table.Path)
	index := readIndexForTest(t, table.Path, ft)
	if len(index) == 0 {
		t.Fatalf("expected index entries")
	}
	corruptCompressionID(t, table.Path, index[0])

	table, err = LoadSSTable(table.Path, opts)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	defer table.Close()

	it := table.Range(nil, nil)
	if it.Next() {
		t.Fatalf("expected range to stop on corrupt compression id")
	}
	if !errors.Is(it.Err(), errs.ErrSSTableUnknownCompression) {
		t.Fatalf("expected unknown compression error, got %v", it.Err())
	}
}

func TestSSTableCorruptMetaBlockFailsOpen(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)

	writer, err := NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	table, err := writer.Flush(makeEntries(10))
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	table.Close()

	ft := readFooterForTest(t, table.Path)
	corruptByte(t, table.Path, int64(ft.MetaOffset)+int64(ft.MetaLen)-1)

	if _, err := LoadSSTable(table.Path, opts); !errors.Is(err, errs.ErrSSTableBadMeta) {
		t.Fatalf("expected bad meta error, got %v", err)
	}
}

func TestSSTableCorruptIndexBlockFailsOpen(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)

	writer, err := NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	table, err := writer.Flush(makeEntries(10))
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	table.Close()

	ft := readFooterForTest(t, table.Path)
	corruptByte(t, table.Path, int64(ft.IndexOffset)+int64(ft.IndexLen)-1)

	if _, err := LoadSSTable(table.Path, opts); !errors.Is(err, errs.ErrSSTableBadIndex) {
		t.Fatalf("expected bad index error, got %v", err)
	}
}

func TestSSTableCorruptBloomBlockFailsOpen(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)

	writer, err := NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	table, err := writer.Flush(makeEntries(50))
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	table.Close()

	ft := readFooterForTest(t, table.Path)
	meta := readMetaForTest(t, table.Path, ft)
	if meta.BloomLen == 0 || meta.BloomOffset == 0 {
		t.Fatalf("expected bloom filter")
	}
	corruptByte(t, table.Path, int64(meta.BloomOffset)+int64(meta.BloomLen)-1)

	if _, err := LoadSSTable(table.Path, opts); !errors.Is(err, errs.ErrSSTableBadMeta) {
		t.Fatalf("expected bad meta error, got %v", err)
	}
}

func TestSSTablePartitionedIndexGetRange(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.BlockTargetBytes = 64
	opts.BlockMaxBytes = 96
	opts.IndexPartitionEntries = 2

	writer, err := NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	table, err := writer.Flush(makeEntries(80))
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	defer table.Close()

	ft := readFooterForTest(t, table.Path)
	if ft.Flags&format.FooterFlagIndexPartitioned == 0 {
		t.Fatalf("expected partitioned index")
	}
	if ft.Flags&format.FooterFlagFilterPartitioned == 0 {
		t.Fatalf("expected partitioned filter")
	}
	meta := readMetaForTest(t, table.Path, ft)
	if meta.BloomLen == 0 || meta.BloomOffset == 0 {
		t.Fatalf("expected partitioned filter index")
	}
	got, ok := table.Get([]byte("k010"))
	if !ok || string(got.Value) != "v010" {
		t.Fatalf("get: ok=%v val=%q", ok, got.Value)
	}
	it := table.Range([]byte("k005"), []byte("k015"))
	var keys []string
	for it.Next() {
		keys = append(keys, string(it.Entry().Key))
	}
	if it.Err() != nil {
		t.Fatalf("range err: %v", it.Err())
	}
	if len(keys) != 10 || keys[0] != "k005" || keys[len(keys)-1] != "k014" {
		t.Fatalf("range keys: %v", keys)
	}
}

func readFooterForTest(t *testing.T, path string) format.Footer {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	buf := make([]byte, format.FooterSizeBytes)
	if _, err := f.ReadAt(buf, info.Size()-format.FooterSizeBytes); err != nil {
		t.Fatalf("read footer: %v", err)
	}
	ft, err := format.DecodeFooter(buf)
	if err != nil {
		t.Fatalf("decode footer: %v", err)
	}
	return ft
}

func readMetaForTest(t *testing.T, path string, ft format.Footer) meta.Meta {
	t.Helper()
	buf := readBlockForTest(t, path, int64(ft.MetaOffset), ft.MetaLen)
	payload, err := format.DecodeBlockPayload(buf, format.BlockTypeMeta, errs.ErrSSTableBadMeta)
	if err != nil {
		t.Fatalf("decode meta payload: %v", err)
	}
	m, err := meta.Decode(payload)
	if err != nil {
		t.Fatalf("decode meta: %v", err)
	}
	return m
}

func readIndexForTest(t *testing.T, path string, ft format.Footer) []index.Entry {
	t.Helper()
	buf := readBlockForTest(t, path, int64(ft.IndexOffset), ft.IndexLen)
	payload, err := format.DecodeBlockPayload(buf, format.BlockTypeIndex, errs.ErrSSTableBadIndex)
	if err != nil {
		t.Fatalf("decode index payload: %v", err)
	}
	indexEntries, err := index.Decode(payload)
	if err != nil {
		t.Fatalf("decode index: %v", err)
	}
	return indexEntries
}

func readBlockForTest(t *testing.T, path string, offset int64, length uint32) []byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	buf := make([]byte, length)
	if _, err := f.ReadAt(buf, offset); err != nil {
		t.Fatalf("read: %v", err)
	}
	return buf
}

func corruptByte(t *testing.T, path string, offset int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	var b [1]byte
	if _, err := f.ReadAt(b[:], offset); err != nil {
		t.Fatalf("read: %v", err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b[:], offset); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func corruptCompressionID(t *testing.T, path string, entry index.Entry) {
	t.Helper()

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	data := make([]byte, entry.Length)
	if _, err := f.ReadAt(data, int64(entry.Offset)); err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(data) < format.BlockHeaderSize+format.BlockTypeSize+format.BlockCRCLen {
		t.Fatalf("block too small")
	}
	data[format.BlockMagicSize] = 0xff
	crc := format.Checksum(data[:len(data)-format.BlockCRCLen])
	binary.LittleEndian.PutUint32(data[len(data)-format.BlockCRCLen:], crc)
	if _, err := f.WriteAt(data, int64(entry.Offset)); err != nil {
		t.Fatalf("write: %v", err)
	}
}
