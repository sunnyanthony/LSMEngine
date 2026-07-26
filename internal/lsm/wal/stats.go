// WAL operational statistics.

package wal

import (
	"os"
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

// Stats returns a point-in-time view of the WAL. Segment file scanning is
// best-effort; in-memory active segment state remains available if scanning
// archived segment files fails.
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
	out.ArchivedSegmentCount = len(segments)
	out.SegmentCount += out.ArchivedSegmentCount
	for _, path := range segments {
		info, err := os.Stat(path)
		if err != nil {
			out.SegmentScanError = err.Error()
			return out
		}
		out.ArchivedSegmentBytes += uint64(info.Size())
	}
	out.TotalBytes += out.ArchivedSegmentBytes
	return out
}
