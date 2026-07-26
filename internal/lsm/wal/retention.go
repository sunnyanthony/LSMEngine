// WAL archived segment retention.

package wal

import (
	"errors"
	"fmt"
	"os"

	"lsmengine/internal/lsm/iofs"
	"lsmengine/internal/lsm/wal/segment"
	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/types"
)

// RetentionStats describes one archived segment retention pass.
type RetentionStats struct {
	ScannedSegments  int
	RemovedSegments  int
	RetainedSegments int
	PrunedThrough    uint64
}

type segmentSeqRange struct {
	path    string
	id      uint64
	entries int
	maxSeq  uint64
}

// PruneArchivedSegments removes archived WAL segments whose entries are fully
// covered by a durable manifest checkpoint. Only a contiguous archived prefix is
// pruned, and the newest retain archived segments are always kept.
func (w *WAL) PruneArchivedSegments(checkpoint uint64, retain int) (RetentionStats, error) {
	if w == nil {
		return RetentionStats{}, nil
	}
	if retain < 0 {
		return RetentionStats{}, fmt.Errorf("wal retain archived segments must be non-negative")
	}
	if checkpoint == 0 {
		return RetentionStats{}, nil
	}
	fs := w.fs
	if fs == nil {
		fs = iofs.OSFS{}
	}
	segments, missing, err := segment.ListSegments(w.path)
	if err != nil {
		return RetentionStats{}, err
	}
	if missing {
		return RetentionStats{}, errs.ErrWALMissingSegment
	}
	out := RetentionStats{
		ScannedSegments:  len(segments),
		RetainedSegments: len(segments),
	}
	if len(segments) == 0 || len(segments) <= retain {
		return out, nil
	}

	ranges := make([]segmentSeqRange, 0, len(segments))
	for _, path := range segments {
		id, ok := segment.SegmentID(path)
		if !ok {
			return out, fmt.Errorf("parse wal segment id: %s", path)
		}
		entries, maxSeq, err := w.scanArchivedSegment(fs, path)
		if err != nil {
			return out, err
		}
		ranges = append(ranges, segmentSeqRange{
			path:    path,
			id:      id,
			entries: entries,
			maxSeq:  maxSeq,
		})
	}

	deleteLimit := len(ranges) - retain
	deleteCount := 0
	for deleteCount < deleteLimit {
		r := ranges[deleteCount]
		if r.entries == 0 || r.maxSeq > checkpoint {
			break
		}
		deleteCount++
	}
	if deleteCount == 0 {
		return out, nil
	}

	prunedThrough := ranges[deleteCount-1].id
	if err := segment.WritePrunedThrough(w.path, prunedThrough); err != nil {
		return out, err
	}
	for _, r := range ranges[:deleteCount] {
		if err := fs.Remove(r.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return out, fmt.Errorf("remove wal segment %s: %w", r.path, err)
		}
	}
	out.RemovedSegments = deleteCount
	out.RetainedSegments = len(segments) - deleteCount
	out.PrunedThrough = prunedThrough
	return out, nil
}

func (w *WAL) scanArchivedSegment(fs iofs.FS, path string) (entries int, maxSeq uint64, err error) {
	if _, err := fs.Stat(path); err != nil {
		return 0, 0, fmt.Errorf("stat wal segment %s: %w", path, err)
	}
	_, err = replaySegment(fs, w.replayPool, path, func(entry types.Entry) error {
		entries++
		if entry.Seq > maxSeq {
			maxSeq = entry.Seq
		}
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	return entries, maxSeq, nil
}
