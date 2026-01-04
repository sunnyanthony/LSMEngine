package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/types"
	"lsmengine/pkg/lsm/wal/codec"
)

type appendRequest struct {
	entry types.Entry
	owned bool
	done  chan error
}

// Append writes a record and copies key/value so callers can reuse buffers.
func (w *WAL) Append(entry types.Entry) error {
	return w.append(entry, false)
}

// AppendOwned writes a record without copying key/value. Callers must not
// mutate or reuse the slices after calling this method.
func (w *WAL) AppendOwned(entry types.Entry) error {
	return w.append(entry, true)
}

func (w *WAL) append(entry types.Entry, owned bool) error {
	if w.async {
		return w.appendAsync(entry, owned)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.appendLocked(entry, owned); err != nil {
		return err
	}
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

func (w *WAL) appendAsync(entry types.Entry, owned bool) error {
	if atomic.LoadUint32(&w.closed) == 1 {
		return errs.ErrWALClosed
	}
	if !owned {
		entry = copyEntry(entry)
		owned = true
	}
	req := appendRequest{
		entry: entry,
		owned: owned,
		done:  make(chan error, 1),
	}
	select {
	case w.reqCh <- req:
		return <-req.done
	case <-w.doneCh:
		return errs.ErrWALClosed
	}
}

func (w *WAL) appendLocked(entry types.Entry, owned bool) error {
	if w.f == nil {
		return errs.ErrWALClosed
	}
	if len(entry.Key) == 0 {
		return errs.ErrWALEmptyKey
	}
	if len(entry.Value) == 0 && !entry.Tombstone {
		return errs.ErrWALEmptyValue
	}

	var rb codec.RecordBuffer
	if owned {
		rb = codec.NewRecordBufferOwned(entry)
	} else {
		rb = codec.NewRecordBuffer(entry)
	}
	if uint32(rb.Total) > w.blockSize {
		return fmt.Errorf("%w (record %d > block %d)", errs.ErrWALRecordTooLarge, rb.Total, w.blockSize)
	}
	if w.maxRecord > 0 && uint64(rb.Total) > w.maxRecord {
		return fmt.Errorf("%w (%d > %d)", errs.ErrWALRecordTooLarge, rb.Total, w.maxRecord)
	}
	// flush block if needed
	if w.blockLen > 0 && w.blockLen+rb.Total > int(w.blockSize) {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}
	w.records = append(w.records, rb)
	w.blockLen += rb.Total
	return nil
}

func copyEntry(entry types.Entry) types.Entry {
	if len(entry.Key) > 0 {
		entry.Key = append([]byte(nil), entry.Key...)
	}
	if len(entry.Value) > 0 {
		entry.Value = append([]byte(nil), entry.Value...)
	}
	return entry
}

func (w *WAL) Close() error {
	if w.async {
		return w.closeAsync()
	}
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

func (w *WAL) closeAsync() error {
	if !atomic.CompareAndSwapUint32(&w.closed, 0, 1) {
		return nil
	}
	resp := make(chan error, 1)
	w.closeCh <- resp
	return <-resp
}

func (w *WAL) flushBlock() error {
	if w.blockLen == 0 {
		return nil
	}
	n, err := codec.WriteBlock(w.f, w.records)
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

func (w *WAL) runWriter() {
	defer close(w.doneCh)
	for {
		select {
		case req := <-w.reqCh:
			batch := w.collectBatch(req)
			_ = w.processBatch(batch, w.sync)
		case resp := <-w.closeCh:
			resp <- w.drainAndClose()
			return
		}
	}
}

func (w *WAL) collectBatch(first appendRequest) []appendRequest {
	batch := []appendRequest{first}
	for w.batchMax == 0 || len(batch) < w.batchMax {
		select {
		case req := <-w.reqCh:
			batch = append(batch, req)
		default:
			return batch
		}
	}
	return batch
}

func (w *WAL) processBatch(batch []appendRequest, syncNow bool) error {
	errs := make([]error, len(batch))
	var fatal error
	w.mu.Lock()
	for i, req := range batch {
		if fatal != nil {
			errs[i] = fatal
			continue
		}
		if err := w.appendLocked(req.entry, req.owned); err != nil {
			if isFatalAppendError(err) {
				fatal = err
			}
			errs[i] = err
		}
	}
	if fatal == nil && syncNow {
		if err := w.flushBlock(); err != nil {
			fatal = err
		} else if err := w.f.Sync(); err != nil {
			fatal = fmt.Errorf("wal sync: %w", err)
		}
	}
	if fatal != nil {
		for i := range errs {
			if errs[i] == nil {
				errs[i] = fatal
			}
		}
	}
	w.mu.Unlock()
	for i, req := range batch {
		req.done <- errs[i]
	}
	return fatal
}

func (w *WAL) drainAndClose() error {
	var pending []appendRequest
	for {
		select {
		case req := <-w.reqCh:
			pending = append(pending, req)
			if w.batchMax > 0 && len(pending) >= w.batchMax {
				if err := w.processBatch(pending, false); err != nil {
					w.closeFile()
					return err
				}
				pending = pending[:0]
			}
		default:
			if len(pending) > 0 {
				if err := w.processBatch(pending, false); err != nil {
					w.closeFile()
					return err
				}
			}
			w.mu.Lock()
			err := w.flushBlock()
			if err == nil && w.sync {
				if serr := w.f.Sync(); serr != nil {
					err = fmt.Errorf("wal sync: %w", serr)
				}
			}
			w.mu.Unlock()
			cerr := w.closeFile()
			if err == nil {
				err = cerr
			}
			return err
		}
	}
}

func (w *WAL) closeFile() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

func isFatalAppendError(err error) bool {
	if errors.Is(err, errs.ErrWALEmptyKey) || errors.Is(err, errs.ErrWALEmptyValue) || errors.Is(err, errs.ErrWALRecordTooLarge) {
		return false
	}
	return true
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
	if _, err := codec.WriteSegmentHeader(newFile, w.blockSize, w.segmentID); err != nil {
		_ = newFile.Close()
		return fmt.Errorf("write segment header: %w", err)
	}
	w.f = newFile
	w.sizeBytes = uint64(codec.SegmentHeaderSize)
	return nil
}
