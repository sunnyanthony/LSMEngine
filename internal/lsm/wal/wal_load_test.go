package wal

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"lsmengine/internal/lsm/wal/codec"
	"lsmengine/internal/lsm/wal/segment"
	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/types"
)

// Benchmark-style smoke to exercise append+replay for many small records.
func TestWALLoadManySmallRecords(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := NewWAL(Options{Path: path, Sync: false, BlockSize: 32})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	const total = 5000
	for i := 0; i < total; i++ {
		key := []byte{byte(i % 128)}
		e := types.Entry{
			Key:   key,
			Value: []byte{byte(i % 251)},
			Seq:   uint64(i + 1),
		}
		appendOwnedLoad(t, w, e)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	wal := OpenReplay(path, false)
	var count int
	if err := wal.Replay(func(e types.Entry) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if count != total {
		t.Fatalf("expected %d records replayed, got %d", total, count)
	}
}

// Exercises large values and many appends to ensure low-copy path works.
func TestWALLoadLargeValues(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := NewWAL(Options{Path: path, Sync: false, BlockSize: 1024 * 1024})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	large := make([]byte, 512*1024)
	for i := range large {
		large[i] = byte(i % 253)
	}
	for i := 0; i < 8; i++ {
		appendOwnedLoad(t, w, types.Entry{Key: []byte("big"), Value: large, Seq: uint64(i + 1)})
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	wal := OpenReplay(path, false)
	var count int
	if err := wal.Replay(func(e types.Entry) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if count != 8 {
		t.Fatalf("expected 8 large entries, got %d", count)
	}
}

// Stress rotation by using a tiny segment size and ensuring replay reads all segments.
func TestWALRotationReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := NewWAL(Options{Path: path, Sync: false, MaxSegment: 512})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	for i := 0; i < 100; i++ {
		appendOwnedLoad(t, w, types.Entry{Key: []byte("k"), Value: []byte{byte(i)}, Seq: uint64(i + 1)})
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	wal := OpenReplay(path, false)
	var seqs []uint64
	if err := wal.Replay(func(e types.Entry) error {
		seqs = append(seqs, e.Seq)
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(seqs) != 100 || seqs[0] != 1 || seqs[len(seqs)-1] != 100 {
		t.Fatalf("unexpected seqs len=%d first=%d last=%d", len(seqs), seqs[0], seqs[len(seqs)-1])
	}
}

// Corruption tolerance: truncated tail should still replay prior records.
func TestWALReplayStopsOnCorruptTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := NewWAL(Options{Path: path, Sync: false, BlockSize: 32})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}

	entries := []types.Entry{
		{Key: []byte("a"), Value: []byte("1"), Seq: 1},
		{Key: []byte("b"), Value: []byte("2"), Seq: 2},
	}
	for _, e := range entries {
		appendOwnedLoad(t, w, e)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	// Truncate file to corrupt the second block.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat wal: %v", err)
	}
	if err := os.Truncate(path, info.Size()-5); err != nil {
		t.Fatalf("truncate wal: %v", err)
	}

	wal := OpenReplay(path, false)
	var replayed []types.Entry
	err = wal.Replay(func(e types.Entry) error {
		replayed = append(replayed, e)
		return nil
	})
	if err != nil && !errors.Is(err, errs.ErrWALCorruptSegment) {
		t.Fatalf("replay: %v", err)
	}

	if len(replayed) != 1 || !bytes.Equal(replayed[0].Key, []byte("a")) {
		t.Fatalf("expected only first entry after corrupt tail, got %+v", replayed)
	}
}

// Checksum mismatch should stop replay at the corrupted record but keep prior ones.
func TestWALReplayChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := NewWAL(Options{Path: path, Sync: false})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	entries := []types.Entry{
		{Key: []byte("a"), Value: []byte("1"), Seq: 1},
		{Key: []byte("b"), Value: []byte("2"), Seq: 2},
	}
	for _, e := range entries {
		appendOwnedLoad(t, w, e)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	// Build a segment with two blocks, then corrupt the first block CRC.
	buf := bytes.NewBuffer(nil)
	if _, err := codec.WriteSegmentHeader(buf, 64*1024, 1); err != nil {
		t.Fatalf("write segment header: %v", err)
	}
	block1 := bytes.NewBuffer(nil)
	_, _ = codec.WriteBlock(block1, []codec.RecordBuffer{codec.NewRecordBuffer(entries[0])})
	block2 := bytes.NewBuffer(nil)
	_, _ = codec.WriteBlock(block2, []codec.RecordBuffer{codec.NewRecordBuffer(entries[1])})
	b1 := block1.Bytes()
	b1[8] ^= 0xFF // corrupt block CRC
	data := append(buf.Bytes(), b1...)
	data = append(data, block2.Bytes()...)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write wal: %v", err)
	}

	wal := OpenReplay(path, false)
	var replayed []types.Entry
	err = wal.Replay(func(e types.Entry) error {
		replayed = append(replayed, e)
		return nil
	})
	if err != nil && !errors.Is(err, errs.ErrWALCorruptSegment) {
		t.Fatalf("replay: %v", err)
	}
	if len(replayed) != 1 || !bytes.Equal(replayed[0].Key, []byte("b")) {
		t.Fatalf("expected resync to recover second entry only, got %+v", replayed)
	}
}

// Placeholders for broader load/integration scenarios:
func TestWALLoadManySmallWithTombstones(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := NewWAL(Options{Path: path, Sync: false, BlockSize: 64})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	const total = 2000
	tombstones := 0
	for i := 0; i < total; i++ {
		entry := types.Entry{
			Key: []byte{byte(i % 128)},
			Seq: uint64(i + 1),
		}
		if i%5 == 0 {
			entry.Tombstone = true
			tombstones++
		} else {
			entry.Value = []byte{byte(i % 251)}
		}
		appendOwnedLoad(t, w, entry)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	wal := OpenReplay(path, false)
	var count, tombCount int
	if err := wal.Replay(func(e types.Entry) error {
		count++
		if e.Tombstone {
			tombCount++
		}
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if count != total {
		t.Fatalf("expected %d records replayed, got %d", total, count)
	}
	if tombCount != tombstones {
		t.Fatalf("expected %d tombstones, got %d", tombstones, tombCount)
	}
}

func TestWALLoadHandlerErrorMidReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := NewWAL(Options{Path: path, Sync: true, BlockSize: 64, MaxSegment: 128})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	const total = 50
	for i := 0; i < total; i++ {
		appendOwnedLoad(t, w, types.Entry{Key: []byte("k"), Value: []byte{byte(i)}, Seq: uint64(i + 1)})
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	wal := OpenReplay(path, false)
	stopErr := errors.New("stop")
	count := 0
	err = wal.Replay(func(e types.Entry) error {
		count++
		if count == 7 {
			return stopErr
		}
		return nil
	})
	if err == nil || !errors.Is(err, stopErr) {
		t.Fatalf("expected handler error, got %v", err)
	}
	if count != 7 {
		t.Fatalf("expected stop after 7 entries, got %d", count)
	}
}

func TestWALRotationWithMissingSegment(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := NewWAL(Options{Path: path, Sync: true, BlockSize: 64, MaxSegment: 128})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	const total = 60
	for i := 0; i < total; i++ {
		appendOwnedLoad(t, w, types.Entry{Key: []byte("k"), Value: []byte{byte(i)}, Seq: uint64(i + 1)})
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	segs, _, err := segment.ListSegments(path)
	if err != nil {
		t.Fatalf("list segments: %v", err)
	}
	if len(segs) < 3 {
		t.Fatalf("expected >= 3 segments, got %d", len(segs))
	}
	if err := os.Remove(segs[1]); err != nil {
		t.Fatalf("remove segment: %v", err)
	}

	wal := OpenReplay(path, false)
	count := 0
	err = wal.Replay(func(e types.Entry) error {
		count++
		return nil
	})
	if err == nil || !errors.Is(err, errs.ErrWALMissingSegment) {
		t.Fatalf("expected missing segment error, got %v", err)
	}
	if count == 0 || count >= total {
		t.Fatalf("expected partial replay with missing segment, got %d entries", count)
	}
}

func appendOwnedLoad(t *testing.T, w *WAL, entry types.Entry) {
	t.Helper()
	entry = copyEntryLoad(entry)
	if err := w.AppendOwned(entry); err != nil {
		t.Fatalf("append: %v", err)
	}
}

func copyEntryLoad(entry types.Entry) types.Entry {
	entry.Key = append([]byte(nil), entry.Key...)
	entry.Value = append([]byte(nil), entry.Value...)
	return entry
}
