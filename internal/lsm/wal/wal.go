// WAL lifecycle and options.

package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"lsmengine/internal/lsm/iofs"
	"lsmengine/internal/lsm/memory"
	"lsmengine/internal/lsm/wal/codec"
	"lsmengine/internal/lsm/wal/segment"
)

// WAL appends mutations for durability and supports replay.
type WAL struct {
	mu        sync.Mutex
	f         iofs.File
	fs        iofs.FS
	path      string
	sync      bool
	maxBytes  uint64
	sizeBytes uint64
	maxRecord uint64
	blockSize uint32
	segmentID uint64
	records   []codec.RecordBuffer
	blockLen  int

	async    bool
	batchMax int
	reqCh    chan appendRequest
	closeCh  chan chan error
	doneCh   chan struct{}
	closed   uint32

	repairOnReplay bool
	replayPool     *memory.ReaderPool
}

type Options struct {
	Path           string
	Sync           bool
	MaxSegment     uint64 // rotate when bytes exceed; 0 means no rotation
	MaxRecordBytes uint64 // per-record cap; 0 means no limit
	BlockSize      uint32 // block size for framing; 0 means default
	ReplayBuffer   int    // replay read buffer size; 0 means block size
	Async          bool
	QueueDepth     int // async queue size; 0 means default
	BatchMax       int // max requests per batch; 0 means drain queue
	RepairOnReplay bool
	FS             iofs.FS
}

func NewWAL(opts Options) (*WAL, error) {
	if opts.Path == "" {
		return nil, fmt.Errorf("wal path required")
	}
	blockSize := opts.BlockSize
	if blockSize == 0 {
		blockSize = 64 * 1024
	}
	minBlockSize := uint32(codec.MinBlockSize)
	if blockSize < minBlockSize {
		return nil, fmt.Errorf("wal block size too small (%d < %d)", blockSize, minBlockSize)
	}
	fs := opts.FS
	if fs == nil {
		fs = iofs.OSFS{}
	}
	if err := fs.MkdirAll(filepath.Dir(opts.Path), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir wal dir: %w", err)
	}
	f, err := fs.OpenFile(opts.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat wal: %w", err)
	}
	segmentID := segment.NextSegmentID(opts.Path)
	if info.Size() == 0 {
		if _, err := codec.WriteSegmentHeader(f, blockSize, segmentID); err != nil {
			f.Close()
			return nil, fmt.Errorf("write segment header: %w", err)
		}
		info, _ = f.Stat()
	} else {
		r, err := fs.Open(opts.Path)
		if err == nil {
			if hdr, err := codec.ReadSegmentHeader(r); err == nil {
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
	replayBuffer := opts.ReplayBuffer
	if replayBuffer <= 0 {
		replayBuffer = int(blockSize)
	}
	w := &WAL{
		f:              f,
		fs:             fs,
		path:           opts.Path,
		sync:           opts.Sync,
		maxBytes:       opts.MaxSegment,
		sizeBytes:      uint64(info.Size()),
		maxRecord:      opts.MaxRecordBytes,
		blockSize:      blockSize,
		segmentID:      segmentID,
		async:          opts.Async,
		batchMax:       opts.BatchMax,
		repairOnReplay: opts.RepairOnReplay,
		replayPool:     memory.NewReaderPool(replayBuffer),
	}
	if opts.Async {
		queueDepth := opts.QueueDepth
		if queueDepth == 0 {
			queueDepth = runtime.GOMAXPROCS(0)
		}
		if queueDepth < 1 {
			queueDepth = 1
		}
		w.reqCh = make(chan appendRequest, queueDepth)
		w.closeCh = make(chan chan error, 1)
		w.doneCh = make(chan struct{})
		go w.runWriter()
	}
	return w, nil
}

// OpenReplay returns a WAL handle suitable for replay-only usage.
func OpenReplay(path string, repairOnReplay bool) *WAL {
	return &WAL{
		fs:             iofs.OSFS{},
		path:           path,
		repairOnReplay: repairOnReplay,
	}
}
