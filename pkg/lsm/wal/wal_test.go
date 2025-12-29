package wal

import (
	"bytes"
	"errors"
	"fmt"
	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/types"
	"os"
	"path/filepath"
	"testing"
)

func TestWALAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := NewWAL(Options{Path: path, Sync: true})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	defer w.Close()

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

	wal := &WAL{path: path}
	var replayed []types.Entry
	err = wal.Replay(func(e types.Entry) error {
		replayed = append(replayed, e)
		return nil
	})
	if err != nil && !errors.Is(err, errs.ErrWALCorruptSegment) {
		t.Fatalf("replay: %v", err)
	}
	if len(replayed) != 2 {
		t.Fatalf("expected 2 entries replayed, got %d", len(replayed))
	}
	if replayed[0].Key != "a" || string(replayed[0].Value) != "1" {
		t.Fatalf("bad replay entry: %+v", replayed[0])
	}
}

func TestWALAppendLargeValue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := NewWAL(Options{Path: path, Sync: false, BlockSize: 512 * 1024})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	large := make([]byte, 256*1024)
	for i := range large {
		large[i] = byte(i % 251)
	}
	if err := w.Append(types.Entry{Key: "big", Value: large, Seq: 1}); err != nil {
		t.Fatalf("append large: %v", err)
	}
	_ = w.Close()

	wal := &WAL{path: path}
	var replayed []types.Entry
	err = wal.Replay(func(e types.Entry) error {
		replayed = append(replayed, e)
		return nil
	})
	if err != nil && !errors.Is(err, errs.ErrWALCorruptSegment) {
		t.Fatalf("replay: %v", err)
	}
	if len(replayed) != 1 || len(replayed[0].Value) != len(large) {
		t.Fatalf("expected 1 large entry, got %d len=%d", len(replayed), len(replayed[0].Value))
	}
}

func TestWALEmptyKeyRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := NewWAL(Options{Path: path, Sync: false})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	err = w.Append(types.Entry{Key: "", Value: []byte("v"), Seq: 1})
	if err == nil {
		t.Fatalf("expected error for empty key")
	}
}

func TestWALEmptyValueRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := NewWAL(Options{Path: path, Sync: false})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	err = w.Append(types.Entry{Key: "k", Value: nil, Seq: 1})
	if err == nil {
		t.Fatalf("expected error for empty value")
	}
}

func TestWALTombstoneReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := NewWAL(Options{Path: path, Sync: false})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	if err := w.Append(types.Entry{Key: "k", Value: []byte("v"), Seq: 1}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.Append(types.Entry{Key: "k", Tombstone: true, Seq: 2}); err != nil {
		t.Fatalf("append tombstone: %v", err)
	}
	_ = w.Close()

	wal := &WAL{path: path}
	var last types.Entry
	if err := wal.Replay(func(e types.Entry) error {
		last = e
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !last.Tombstone || last.Seq != 2 {
		t.Fatalf("expected tombstone seq=2, got %+v", last)
	}
}

func TestWALReplayHandlerError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, _ := NewWAL(Options{Path: path, Sync: false})
	_ = w.Append(types.Entry{Key: "k", Value: []byte("v"), Seq: 1})
	_ = w.Append(types.Entry{Key: "k", Value: []byte("v2"), Seq: 2})
	_ = w.Close()

	wal := &WAL{path: path}
	count := 0
	err := wal.Replay(func(e types.Entry) error {
		count++
		return fmt.Errorf("stop")
	})
	if err == nil {
		t.Fatalf("expected handler error")
	}
	if count != 1 {
		t.Fatalf("expected stop after first entry, got %d", count)
	}
}

func TestWALSyncTrueFlushes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := NewWAL(Options{Path: path, Sync: true})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	if err := w.Append(types.Entry{Key: "k", Value: []byte("v"), Seq: 1}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat wal: %v", err)
	}
	if info.Size() == 0 {
		t.Fatalf("expected wal to be flushed with sync")
	}
}

func TestWALCorruptHeaderLenStops(t *testing.T) {
	entries := []types.Entry{
		{Key: "a", Value: []byte("1"), Seq: 1},
		{Key: "b", Value: []byte("2"), Seq: 2},
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	buf := bytes.NewBuffer(nil)
	if _, err := writeSegmentHeader(buf, 64*1024, 1); err != nil {
		t.Fatalf("write segment header: %v", err)
	}
	block1 := bytes.NewBuffer(nil)
	_, _ = writeBlock(block1, []recordBuffer{newRecordBuffer(entries[0])})
	block2 := bytes.NewBuffer(nil)
	_, _ = writeBlock(block2, []recordBuffer{newRecordBuffer(entries[1])})
	b1 := block1.Bytes()
	b1[4] = 0xFF // corrupt block length to exceed block size
	b1[5] = 0xFF
	b1[6] = 0x01
	b1[7] = 0x00
	data := append(buf.Bytes(), b1...)
	data = append(data, block2.Bytes()...)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write wal: %v", err)
	}

	wal := &WAL{path: path}
	var replayed []types.Entry
	err := wal.Replay(func(e types.Entry) error {
		replayed = append(replayed, e)
		return nil
	})
	if err != nil && !errors.Is(err, errs.ErrWALCorruptSegment) {
		t.Fatalf("replay: %v", err)
	}
	if len(replayed) != 1 || replayed[0].Key != "b" {
		t.Fatalf("expected resync to recover second record, got %+v", replayed)
	}
}

func TestWALMissingSegmentCausesError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	seg1 := filepath.Join(dir, "wal.log.1")
	seg3 := filepath.Join(dir, "wal.log.3")
	buf1 := bytes.NewBuffer(nil)
	_, _ = writeSegmentHeader(buf1, 64*1024, 1)
	_, _ = writeBlock(buf1, []recordBuffer{newRecordBuffer(types.Entry{Key: "a", Value: []byte("1"), Seq: 1})})
	if err := os.WriteFile(seg1, buf1.Bytes(), 0o644); err != nil {
		t.Fatalf("write seg1: %v", err)
	}
	buf3 := bytes.NewBuffer(nil)
	_, _ = writeSegmentHeader(buf3, 64*1024, 3)
	_, _ = writeBlock(buf3, []recordBuffer{newRecordBuffer(types.Entry{Key: "b", Value: []byte("2"), Seq: 2})})
	if err := os.WriteFile(seg3, buf3.Bytes(), 0o644); err != nil {
		t.Fatalf("write seg3: %v", err)
	}
	wal := &WAL{path: path}
	err := wal.Replay(func(e types.Entry) error { return nil })
	if !errors.Is(err, errs.ErrWALMissingSegment) {
		t.Fatalf("expected ErrMissingSegment, got %v", err)
	}
}

func TestWALOversizedRecordRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := NewWAL(Options{Path: path, Sync: false, MaxRecordBytes: 64})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	err = w.Append(types.Entry{Key: "k", Value: make([]byte, 128), Seq: 1})
	if err == nil {
		t.Fatalf("expected oversized record error")
	}
}

func TestWALBlockSizeTooSmallRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	_, err := NewWAL(Options{Path: path, BlockSize: uint32(recordHeaderSize + recordCRCSize)})
	if err == nil {
		t.Fatalf("expected block size validation error")
	}
}

func TestWALMaxRecordExceedsBlockSizeRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	_, err := NewWAL(Options{Path: path, BlockSize: 64, MaxRecordBytes: 128})
	if err == nil {
		t.Fatalf("expected max record > block size error")
	}
}

func TestWALReplayPartialMagicReportsCorrupt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	buf := bytes.NewBuffer(nil)
	if _, err := writeSegmentHeader(buf, 64*1024, 1); err != nil {
		t.Fatalf("write segment header: %v", err)
	}
	buf.Write([]byte{'L', 'S'})
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write wal: %v", err)
	}
	wal := &WAL{path: path}
	err := wal.Replay(func(e types.Entry) error { return nil })
	if !errors.Is(err, errs.ErrWALCorruptSegment) {
		t.Fatalf("expected corrupt segment error, got %v", err)
	}
}

func TestWALReplayPartialBlockHeaderReportsCorrupt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	buf := bytes.NewBuffer(nil)
	if _, err := writeSegmentHeader(buf, 64*1024, 1); err != nil {
		t.Fatalf("write segment header: %v", err)
	}
	buf.Write(blockMagic)
	buf.Write([]byte{0x01, 0x00, 0x00, 0x00}) // payload len, missing CRC/payload
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write wal: %v", err)
	}
	wal := &WAL{path: path}
	err := wal.Replay(func(e types.Entry) error { return nil })
	if !errors.Is(err, errs.ErrWALCorruptSegment) {
		t.Fatalf("expected corrupt segment error, got %v", err)
	}
}
