package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"lsmengine/internal/lsm/wal/codec"
	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/types"
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
		{Key: []byte("a"), Value: []byte("1"), Seq: 1},
		{Key: []byte("b"), Value: []byte("2"), Seq: 2},
	}
	for _, e := range entries {
		appendOwned(t, w, e)
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
	if !bytes.Equal(replayed[0].Key, []byte("a")) || !bytes.Equal(replayed[0].Value, []byte("1")) {
		t.Fatalf("bad replay entry: %+v", replayed[0])
	}
}

func TestWALAsyncAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := NewWAL(Options{Path: path, Sync: true, Async: true, QueueDepth: 4})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	entries := []types.Entry{
		{Key: []byte("a"), Value: []byte("1"), Seq: 1},
		{Key: []byte("b"), Value: []byte("2"), Seq: 2},
	}
	for _, e := range entries {
		appendOwned(t, w, e)
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
	if !bytes.Equal(replayed[0].Key, []byte("a")) || !bytes.Equal(replayed[0].Value, []byte("1")) {
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
	appendOwned(t, w, types.Entry{Key: []byte("big"), Value: large, Seq: 1})
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

func TestWALAppendOwnedEmptyKeyRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := NewWAL(Options{Path: path, Sync: false})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	err = w.AppendOwned(types.Entry{Key: nil, Value: []byte("v"), Seq: 1})
	if err == nil {
		t.Fatalf("expected error for empty key")
	}
}

func TestWALAppendOwnedEmptyValueRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := NewWAL(Options{Path: path, Sync: false})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	err = w.AppendOwned(types.Entry{Key: []byte("k"), Value: nil, Seq: 1})
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
	appendOwned(t, w, types.Entry{Key: []byte("k"), Value: []byte("v"), Seq: 1})
	appendOwned(t, w, types.Entry{Key: []byte("k"), Tombstone: true, Seq: 2})
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
	_ = w.AppendOwned(copyEntry(types.Entry{Key: []byte("k"), Value: []byte("v"), Seq: 1}))
	_ = w.AppendOwned(copyEntry(types.Entry{Key: []byte("k"), Value: []byte("v2"), Seq: 2}))
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
	appendOwned(t, w, types.Entry{Key: []byte("k"), Value: []byte("v"), Seq: 1})
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
		{Key: []byte("a"), Value: []byte("1"), Seq: 1},
		{Key: []byte("b"), Value: []byte("2"), Seq: 2},
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	buf := bytes.NewBuffer(nil)
	if _, err := codec.WriteSegmentHeader(buf, 64*1024, 1); err != nil {
		t.Fatalf("write segment header: %v", err)
	}
	block1 := bytes.NewBuffer(nil)
	_, _ = codec.WriteBlock(block1, []codec.RecordBuffer{codec.NewRecordBuffer(entries[0])})
	block2 := bytes.NewBuffer(nil)
	_, _ = codec.WriteBlock(block2, []codec.RecordBuffer{codec.NewRecordBuffer(entries[1])})
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
	if len(replayed) != 1 || !bytes.Equal(replayed[0].Key, []byte("b")) {
		t.Fatalf("expected resync to recover second record, got %+v", replayed)
	}
}

func TestWALReplayAutoRepairTruncatesTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := NewWAL(Options{Path: path, Sync: false, BlockSize: 64})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	appendOwned(t, w, types.Entry{Key: []byte("a"), Value: []byte("1"), Seq: 1})
	appendOwned(t, w, types.Entry{Key: []byte("b"), Value: []byte("2"), Seq: 2})
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat wal: %v", err)
	}
	if err := os.Truncate(path, before.Size()-5); err != nil {
		t.Fatalf("truncate wal: %v", err)
	}

	wal := &WAL{path: path, repairOnReplay: true}
	_ = wal.Replay(func(e types.Entry) error { return nil })

	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat wal: %v", err)
	}
	if after.Size() >= before.Size() {
		t.Fatalf("expected wal to truncate corrupt tail, before=%d after=%d", before.Size(), after.Size())
	}
}

func TestWALMissingSegmentCausesError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	seg1 := filepath.Join(dir, "wal.log.1")
	seg3 := filepath.Join(dir, "wal.log.3")
	buf1 := bytes.NewBuffer(nil)
	_, _ = codec.WriteSegmentHeader(buf1, 64*1024, 1)
	_, _ = codec.WriteBlock(buf1, []codec.RecordBuffer{codec.NewRecordBuffer(types.Entry{Key: []byte("a"), Value: []byte("1"), Seq: 1})})
	if err := os.WriteFile(seg1, buf1.Bytes(), 0o644); err != nil {
		t.Fatalf("write seg1: %v", err)
	}
	buf3 := bytes.NewBuffer(nil)
	_, _ = codec.WriteSegmentHeader(buf3, 64*1024, 3)
	_, _ = codec.WriteBlock(buf3, []codec.RecordBuffer{codec.NewRecordBuffer(types.Entry{Key: []byte("b"), Value: []byte("2"), Seq: 2})})
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
	err = w.AppendOwned(copyEntry(types.Entry{Key: []byte("k"), Value: make([]byte, 128), Seq: 1}))
	if err == nil {
		t.Fatalf("expected oversized record error")
	}
}

func TestWALBlockSizeTooSmallRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	_, err := NewWAL(Options{Path: path, BlockSize: uint32(codec.MinBlockSize - 1)})
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
	if _, err := codec.WriteSegmentHeader(buf, 64*1024, 1); err != nil {
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
	if _, err := codec.WriteSegmentHeader(buf, 64*1024, 1); err != nil {
		t.Fatalf("write segment header: %v", err)
	}
	buf.Write(codec.BlockMagic())
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

func TestWALConcurrentAppend(t *testing.T) {
	runConcurrentAppend(t, false)
}

func TestWALConcurrentAppendAsync(t *testing.T) {
	runConcurrentAppend(t, true)
}

func runConcurrentAppend(t *testing.T, async bool) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	goroutines := runtime.GOMAXPROCS(0) * 2
	if goroutines < 4 {
		goroutines = 4
	}
	opts := Options{Path: path, Sync: false, Async: async}
	if async {
		opts.QueueDepth = goroutines
	}
	w, err := NewWAL(opts)
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}

	var seq uint64
	iters := 200
	start := make(chan struct{})
	errCh := make(chan error, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			<-start
			var keyBuf [8]byte
			for j := 0; j < iters; j++ {
				idx := atomic.AddUint64(&seq, 1)
				binary.LittleEndian.PutUint64(keyBuf[:], idx)
				if err := w.AppendOwned(copyEntry(types.Entry{Key: keyBuf[:], Value: []byte{byte(idx % 251)}, Seq: idx})); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	wal := &WAL{path: path}
	var count int
	err = wal.Replay(func(e types.Entry) error {
		count++
		return nil
	})
	if err != nil && !errors.Is(err, errs.ErrWALCorruptSegment) {
		t.Fatalf("replay: %v", err)
	}
	expected := int(atomic.LoadUint64(&seq))
	if count != expected {
		t.Fatalf("expected %d entries replayed, got %d", expected, count)
	}
}

func appendOwned(t *testing.T, w *WAL, entry types.Entry) {
	t.Helper()
	entry = copyEntry(entry)
	if err := w.AppendOwned(entry); err != nil {
		t.Fatalf("append: %v", err)
	}
}

func copyEntry(entry types.Entry) types.Entry {
	entry.Key = append([]byte(nil), entry.Key...)
	entry.Value = append([]byte(nil), entry.Value...)
	return entry
}
