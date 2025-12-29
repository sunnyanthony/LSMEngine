package wal

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

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
		e := types.Entry{
			Key:   "k" + string(rune(i%128)),
			Value: []byte{byte(i % 251)},
			Seq:   uint64(i + 1),
		}
		if err := w.Append(e); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	wal := &WAL{path: path}
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
		if err := w.Append(types.Entry{Key: "big", Value: large, Seq: uint64(i + 1)}); err != nil {
			t.Fatalf("append large %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	wal := &WAL{path: path}
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
		if err := w.Append(types.Entry{Key: "k", Value: []byte{byte(i)}, Seq: uint64(i + 1)}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	wal := &WAL{path: path}
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
		{Key: "a", Value: []byte("1"), Seq: 1},
		{Key: "b", Value: []byte("2"), Seq: 2},
	}
	for _, e := range entries {
		if err := w.Append(e); err != nil {
			t.Fatalf("append: %v", err)
		}
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

	wal := &WAL{path: path}
	var replayed []types.Entry
	err = wal.Replay(func(e types.Entry) error {
		replayed = append(replayed, e)
		return nil
	})
	if err != nil && !errors.Is(err, errs.ErrWALCorruptSegment) {
		t.Fatalf("replay: %v", err)
	}

	if len(replayed) != 1 || replayed[0].Key != "a" {
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
		{Key: "a", Value: []byte("1"), Seq: 1},
		{Key: "b", Value: []byte("2"), Seq: 2},
	}
	for _, e := range entries {
		if err := w.Append(e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	// Build a segment with two blocks, then corrupt the first block CRC.
	buf := bytes.NewBuffer(nil)
	if _, err := writeSegmentHeader(buf, 64*1024, 1); err != nil {
		t.Fatalf("write segment header: %v", err)
	}
	block1 := bytes.NewBuffer(nil)
	_, _ = writeBlock(block1, []recordBuffer{newRecordBuffer(entries[0])})
	block2 := bytes.NewBuffer(nil)
	_, _ = writeBlock(block2, []recordBuffer{newRecordBuffer(entries[1])})
	b1 := block1.Bytes()
	b1[8] ^= 0xFF // corrupt block CRC
	data := append(buf.Bytes(), b1...)
	data = append(data, block2.Bytes()...)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write wal: %v", err)
	}

	wal := &WAL{path: path}
	var replayed []types.Entry
	err = wal.Replay(func(e types.Entry) error {
		replayed = append(replayed, e)
		return nil
	})
	if err != nil && !errors.Is(err, errs.ErrWALCorruptSegment) {
		t.Fatalf("replay: %v", err)
	}
	if len(replayed) != 1 || replayed[0].Key != "b" {
		t.Fatalf("expected resync to recover second entry only, got %+v", replayed)
	}
}

// Placeholders for broader load/integration scenarios:
func TestWALLoadManySmallWithTombstones(t *testing.T) {}
func TestWALLoadHandlerErrorMidReplay(t *testing.T)   {}
func TestWALRotationWithMissingSegment(t *testing.T)  {}
