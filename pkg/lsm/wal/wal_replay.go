package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"

	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/types"
	"lsmengine/pkg/lsm/wal/codec"
)

// Replay reads entries from the WAL in order and calls fn for each.
func (w *WAL) Replay(fn func(types.Entry) error) error {
	// Replay rotated segments first (oldest), then active file.
	segs, missing, err := listSegments(w.path)
	if err != nil {
		return err
	}
	var corrupt bool
	for _, seg := range segs {
		if _, err := replaySegment(seg, fn); err != nil {
			if errors.Is(err, errs.ErrWALCorruptSegment) {
				corrupt = true
				continue
			}
			return err
		}
	}
	if lastGood, err := replaySegment(w.path, fn); err != nil {
		if errors.Is(err, errs.ErrWALCorruptSegment) {
			corrupt = true
			if w.repairOnReplay && lastGood > 0 {
				_ = repairSegment(w.path, lastGood)
			}
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

func replaySegment(path string, fn func(types.Entry) error) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("open wal for replay: %w", err)
	}
	defer f.Close()

	br := bufio.NewReader(f)
	// Ensure segment header is valid before reading blocks.
	hdr, err := codec.ReadSegmentHeader(br)
	if err != nil {
		return 0, errs.ErrWALCorruptSegment
	}
	blockSize := hdr.BlockSize
	lastGoodOffset := readerOffset(f, br)

	corrupt := false
	for {
		payload, ok, err := codec.DecodeBlock(br, blockSize)
		if err != nil {
			if errors.Is(err, errs.ErrWALCorrupt) {
				corrupt = true
				found, err := replayFromResync(br, blockSize, fn)
				if err != nil {
					return lastGoodOffset, err
				}
				if !found {
					break
				}
				lastGoodOffset = readerOffset(f, br)
				continue
			}
			return lastGoodOffset, fmt.Errorf("decode block: %w", err)
		}
		if !ok {
			break
		}
		if err := applyPayload(payload, fn); err != nil {
			if errors.Is(err, errs.ErrWALCorrupt) {
				corrupt = true
				found, rerr := replayFromResync(br, blockSize, fn)
				if rerr != nil {
					return lastGoodOffset, rerr
				}
				if !found {
					break
				}
				lastGoodOffset = readerOffset(f, br)
				continue
			}
			return lastGoodOffset, err
		}
		lastGoodOffset = readerOffset(f, br)
	}
	if corrupt {
		return lastGoodOffset, errs.ErrWALCorruptSegment
	}
	return lastGoodOffset, nil
}

func replayFromResync(r *bufio.Reader, blockSize uint32, fn func(types.Entry) error) (bool, error) {
	for {
		found, err := codec.ResyncBlock(r)
		if err != nil {
			return false, fmt.Errorf("resync: %w", err)
		}
		if !found {
			return false, nil
		}
		payload, ok, err := codec.DecodeBlockAfterMagic(r, blockSize)
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
	entries, err := codec.DecodeRecords(payload)
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

func readerOffset(f *os.File, br *bufio.Reader) int64 {
	offset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0
	}
	return offset - int64(br.Buffered())
}

func repairSegment(path string, offset int64) error {
	if offset <= 0 {
		return nil
	}
	return os.Truncate(path, offset)
}
