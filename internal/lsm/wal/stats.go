// WAL operational statistics.

package wal

import (
	"strconv"
	"strings"
	"sync/atomic"

	"lsmengine/internal/lsm/wal/segment"
)

// Stats describes WAL segment and write-path configuration state.
type Stats struct {
	SegmentID            uint64
	SegmentCount         int
	ArchivedSegmentCount int
	ActiveSegmentBytes   uint64
	ArchivedSegmentBytes uint64
	TotalBytes           uint64
	MaxSegmentBytes      uint64
	BlockSize            uint32
	PendingBlockBytes    int
	PendingBlockRecords  int
	Sync                 bool
	Async                bool
	Closed               bool
	SegmentScanError     string
}

// Stats samples active WAL state and then scans older segment files. Scanning
// is best-effort; SegmentScanError identifies partial archived counts/bytes.
// Concurrent rotation never counts the sampled active segment again.
func (w *WAL) Stats() Stats {
	if w == nil {
		return Stats{}
	}
	w.mu.Lock()
	out := Stats{
		SegmentID:           w.segmentID,
		SegmentCount:        1,
		ActiveSegmentBytes:  w.sizeBytes,
		TotalBytes:          w.sizeBytes,
		MaxSegmentBytes:     w.maxBytes,
		BlockSize:           w.blockSize,
		PendingBlockBytes:   w.blockLen,
		PendingBlockRecords: len(w.records),
		Sync:                w.sync,
		Async:               w.async,
		Closed:              w.f == nil || atomic.LoadUint32(&w.closed) == 1,
	}
	path := w.path
	w.mu.Unlock()

	segments, _, err := segment.ListSegments(path)
	if err != nil {
		out.SegmentScanError = err.Error()
		return out
	}
	for _, archivedPath := range segments {
		id, err := strconv.ParseUint(strings.TrimPrefix(archivedPath, path+"."), 10, 64)
		if err != nil || id >= out.SegmentID {
			// Rotation after the snapshot must not count the active segment twice.
			continue
		}
		info, err := w.fs.Stat(archivedPath)
		if err != nil {
			out.SegmentScanError = err.Error()
			return out
		}
		out.ArchivedSegmentBytes += uint64(info.Size())
		out.TotalBytes += uint64(info.Size())
		out.ArchivedSegmentCount++
		out.SegmentCount++
	}
	return out
}
