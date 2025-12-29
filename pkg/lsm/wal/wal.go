package wal

import (
	"bufio"
	"errors"
	"fmt"
	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/types"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// WAL appends mutations for durability and supports replay.
type WAL struct {
	mu        sync.Mutex
	f         *os.File
	path      string
	sync      bool
	maxBytes  uint64
	sizeBytes uint64
	maxRecord uint64
	blockSize uint32
	segmentID uint64
	records   []recordBuffer
	blockLen  int
}

type Options struct {
	Path           string
	Sync           bool
	MaxSegment     uint64 // rotate when bytes exceed; 0 means no rotation
	MaxRecordBytes uint64 // per-record cap; 0 means no limit
	BlockSize      uint32 // block size for framing; 0 means default
}

func NewWAL(opts Options) (*WAL, error) {
	if opts.Path == "" {
		return nil, fmt.Errorf("wal path required")
	}
	blockSize := opts.BlockSize
	if blockSize == 0 {
		blockSize = 64 * 1024
	}
	minBlockSize := uint32(recordHeaderSize + recordCRCSize + 1)
	if blockSize < minBlockSize {
		return nil, fmt.Errorf("wal block size too small (%d < %d)", blockSize, minBlockSize)
	}
	if err := os.MkdirAll(filepath.Dir(opts.Path), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir wal dir: %w", err)
	}
	f, err := os.OpenFile(opts.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat wal: %w", err)
	}
	segmentID := nextSegmentID(opts.Path)
	if info.Size() == 0 {
		if _, err := writeSegmentHeader(f, blockSize, segmentID); err != nil {
			f.Close()
			return nil, fmt.Errorf("write segment header: %w", err)
		}
		info, _ = f.Stat()
	} else {
		r, err := os.Open(opts.Path)
		if err == nil {
			if hdr, err := readSegmentHeader(r); err == nil {
				blockSize = hdr.BlockSize
				segmentID = hdr.SegmentID
			}
			_ = r.Close()
		}
		if blockSize < minBlockSize {
			f.Close()
			return nil, fmt.Errorf("wal block size too small (%d < %d)", blockSize, minBlockSize)
		}
	}
	if opts.MaxRecordBytes > 0 && opts.MaxRecordBytes > uint64(blockSize) {
		f.Close()
		return nil, fmt.Errorf("wal max record bytes (%d) exceeds block size (%d)", opts.MaxRecordBytes, blockSize)
	}
	return &WAL{
		f:         f,
		path:      opts.Path,
		sync:      opts.Sync,
		maxBytes:  opts.MaxSegment,
		sizeBytes: uint64(info.Size()),
		maxRecord: opts.MaxRecordBytes,
		blockSize: blockSize,
		segmentID: segmentID,
	}, nil
}

func (w *WAL) Append(entry types.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return errs.ErrWALClosed
	}
	if entry.Key == "" {
		return errs.ErrWALEmptyKey
	}
	if len(entry.Value) == 0 && !entry.Tombstone {
		return errs.ErrWALEmptyValue
	}

	rb := newRecordBuffer(entry)
	if uint32(rb.total) > w.blockSize {
		return fmt.Errorf("%w (record %d > block %d)", errs.ErrWALRecordTooLarge, rb.total, w.blockSize)
	}
	if w.maxRecord > 0 && uint64(rb.total) > w.maxRecord {
		return fmt.Errorf("%w (%d > %d)", errs.ErrWALRecordTooLarge, rb.total, w.maxRecord)
	}
	// flush block if needed
	if w.blockLen > 0 && w.blockLen+rb.total > int(w.blockSize) {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}
	w.records = append(w.records, rb)
	w.blockLen += rb.total

	if w.sync {
		if err := w.flushBlock(); err != nil {
			return err
		}
		if err := w.f.Sync(); err != nil {
			return fmt.Errorf("wal sync: %w", err)
		}
	}
	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	if err := w.flushBlock(); err != nil {
		return err
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// Replay reads entries from the WAL in order and calls fn for each.
func (w *WAL) Replay(fn func(types.Entry) error) error {
	// Replay rotated segments first (oldest), then active file.
	segs, missing, err := listSegments(w.path)
	if err != nil {
		return err
	}
	var corrupt bool
	for _, seg := range segs {
		if err := replaySegment(seg, fn); err != nil {
			if errors.Is(err, errs.ErrWALCorruptSegment) {
				corrupt = true
				continue
			}
			return err
		}
	}
	if err := replaySegment(w.path, fn); err != nil {
		if errors.Is(err, errs.ErrWALCorruptSegment) {
			corrupt = true
		} else {
			return err
		}
	}
	if missing {
		return errs.ErrWALMissingSegment
	}
	if corrupt {
		return errs.ErrWALCorruptSegment
	}
	return nil
}

func replaySegment(path string, fn func(types.Entry) error) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open wal for replay: %w", err)
	}
	defer f.Close()

	br := bufio.NewReader(f)
	// Ensure segment header is valid before reading blocks.
	hdr, err := readSegmentHeader(br)
	if err != nil {
		return errs.ErrWALCorruptSegment
	}
	blockSize := hdr.BlockSize

	corrupt := false
	for {
		payload, ok, err := decodeBlock(br, blockSize)
		if err != nil {
			if errors.Is(err, errs.ErrWALCorrupt) {
				corrupt = true
				found, err := replayFromResync(br, blockSize, fn)
				if err != nil {
					return err
				}
				if !found {
					break
				}
				continue
			}
			return fmt.Errorf("decode block: %w", err)
		}
		if !ok {
			break
		}
		if err := applyPayload(payload, fn); err != nil {
			if errors.Is(err, errs.ErrWALCorrupt) {
				corrupt = true
				found, rerr := replayFromResync(br, blockSize, fn)
				if rerr != nil {
					return rerr
				}
				if !found {
					break
				}
				continue
			}
			return err
		}
	}
	if corrupt {
		return errs.ErrWALCorruptSegment
	}
	return nil
}

func replayFromResync(r *bufio.Reader, blockSize uint32, fn func(types.Entry) error) (bool, error) {
	for {
		found, err := resyncBlock(r)
		if err != nil {
			return false, fmt.Errorf("resync: %w", err)
		}
		if !found {
			return false, nil
		}
		payload, ok, err := decodeBlockAfterMagic(r, blockSize)
		if err != nil {
			if errors.Is(err, errs.ErrWALCorrupt) {
				continue
			}
			return true, fmt.Errorf("decode block: %w", err)
		}
		if ok {
			if err := applyPayload(payload, fn); err != nil {
				return true, err
			}
		}
		return true, nil
	}
}

func applyPayload(payload []byte, fn func(types.Entry) error) error {
	entries, err := decodeRecords(payload)
	if err != nil {
		return errs.ErrWALCorrupt
	}
	for _, entry := range entries {
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

func listSegments(path string) ([]string, bool, error) {
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, false, fmt.Errorf("list segments: %w", err)
	}
	var nums []int
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, base+".") {
			part := strings.TrimPrefix(name, base+".")
			if part == "" {
				continue
			}
			if n, err := strconv.Atoi(part); err == nil {
				nums = append(nums, n)
			}
		}
	}
	if len(nums) == 0 {
		return nil, false, nil
	}
	sort.Ints(nums)
	missing := false
	for i := 0; i < len(nums); i++ {
		if nums[i] != i+1 {
			missing = true
			break
		}
	}
	segs := make([]string, 0, len(nums))
	for _, n := range nums {
		segs = append(segs, filepath.Join(dir, fmt.Sprintf("%s.%d", base, n)))
	}
	return segs, missing, nil
}

func nextSegmentID(path string) uint64 {
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 1
	}
	max := 0
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, base+".") {
			part := strings.TrimPrefix(name, base+".")
			if n, err := strconv.Atoi(part); err == nil && n > max {
				max = n
			}
		}
	}
	return uint64(max + 1)
}

func (w *WAL) flushBlock() error {
	if w.blockLen == 0 {
		return nil
	}
	n, err := writeBlock(w.f, w.records)
	if err != nil {
		return fmt.Errorf("write block: %w", err)
	}
	w.sizeBytes += uint64(n)
	w.records = w.records[:0]
	w.blockLen = 0
	if w.maxBytes > 0 && w.sizeBytes >= w.maxBytes {
		if err := w.rotate(); err != nil {
			return err
		}
	}
	return nil
}

// rotate closes the current WAL and renames it with a sequence suffix, then opens a fresh file.
func (w *WAL) rotate() error {
	if w.f == nil {
		return errs.ErrWALClosed
	}
	dir := filepath.Dir(w.path)
	base := filepath.Base(w.path)
	seg := filepath.Join(dir, fmt.Sprintf("%s.%d", base, w.segmentID))
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("close wal before rotate: %w", err)
	}
	if err := os.Rename(w.path, seg); err != nil {
		return fmt.Errorf("rename wal segment: %w", err)
	}
	w.segmentID++
	newFile, err := os.OpenFile(w.path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open new wal: %w", err)
	}
	if _, err := writeSegmentHeader(newFile, w.blockSize, w.segmentID); err != nil {
		_ = newFile.Close()
		return fmt.Errorf("write segment header: %w", err)
	}
	w.f = newFile
	w.sizeBytes = uint64(segmentHeaderSize)
	return nil
}
