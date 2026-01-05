package lsm

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"

	"lsmengine/internal/lsm/dispatch"
	"lsmengine/internal/lsm/logging"
	"lsmengine/internal/lsm/manifest"
	"lsmengine/internal/lsm/memtable"
	"lsmengine/internal/lsm/sstable"
	"lsmengine/internal/lsm/wal"
	"lsmengine/pkg/lsm/bus"
)

func New(opts Options) (*LSM, error) {
	if opts.DataDir == "" {
		return nil, fmt.Errorf("data dir required")
	}
	if opts.MemtableLimit == 0 {
		opts.MemtableLimit = 1024
	}
	if opts.MemtableConcurrency == 0 {
		opts.MemtableConcurrency = 2
	}
	if opts.MemtableArenaBlockSize == 0 {
		opts.MemtableArenaBlockSize = memtable.DefaultArenaBlockSize
	}
	mtFactory := opts.MemtableFactory
	if mtFactory == nil {
		var err error
		mtFactory, err = memtable.FactoryForKind(opts.MemtableKind, opts.MemtableConcurrency, opts.MemtableShards, opts.MemtableArenaBlockSize)
		if err != nil {
			return nil, err
		}
	}
	var mtPool *sync.Pool
	if opts.MemtableFactory == nil {
		baseFactory := mtFactory
		pool := &sync.Pool{
			New: func() any {
				return baseFactory()
			},
		}
		mtFactory = func() memtable.Table {
			return pool.Get().(memtable.Table)
		}
		mtPool = pool
	}
	if opts.WALBlockSize == 0 {
		opts.WALBlockSize = 64 * 1024
	}
	if opts.WALMaxRecord == 0 {
		opts.WALMaxRecord = uint64(opts.WALBlockSize)
	}
	autoRepair := true
	if opts.WALAutoRepair != nil {
		autoRepair = *opts.WALAutoRepair
	}
	missingPolicy := MissingSegmentError
	if opts.WALMissingSegmentPolicy != nil {
		missingPolicy = *opts.WALMissingSegmentPolicy
	} else if autoRepair {
		missingPolicy = MissingSegmentIgnore
	}
	if opts.FlushQueueSize == 0 {
		opts.FlushQueueSize = 4
	}
	if opts.BusBuffer == 0 {
		opts.BusBuffer = 16
	}
	if opts.ReplayBatchSize == 0 {
		opts.ReplayBatchSize = 256
	}

	logger := opts.Logger
	var logCloser io.Closer
	if logger == nil {
		l, closer, err := logging.NewDefaultLogger(opts.DataDir, opts.LogDir)
		if err != nil {
			return nil, fmt.Errorf("init logger: %w", err)
		}
		logger, logCloser = l, closer
	}

	walPath := filepath.Join(opts.DataDir, "wal.log")
	w, err := wal.NewWAL(wal.Options{
		Path:           walPath,
		Sync:           opts.WALSync,
		MaxRecordBytes: opts.WALMaxRecord,
		BlockSize:      opts.WALBlockSize,
		Async:          opts.WALAsync,
		QueueDepth:     opts.WALQueueDepth,
		BatchMax:       opts.WALBatchMax,
		RepairOnReplay: autoRepair,
	})
	if err != nil {
		return nil, err
	}
	sstableDir := filepath.Join(opts.DataDir, "sstables")
	sstOpts := sstable.DefaultOptions(sstableDir)
	if opts.SSTable != nil {
		if opts.SSTable.BlockTargetBytes != nil {
			sstOpts.BlockTargetBytes = *opts.SSTable.BlockTargetBytes
		}
		if opts.SSTable.BlockMaxBytes != nil {
			sstOpts.BlockMaxBytes = *opts.SSTable.BlockMaxBytes
		}
		if opts.SSTable.Compression != nil {
			sstOpts.Compression = sstable.Compression(*opts.SSTable.Compression)
		}
		if opts.SSTable.BloomBitsPerKey != nil {
			sstOpts.BloomBitsPerKey = *opts.SSTable.BloomBitsPerKey
		}
		if opts.SSTable.BlockCacheBytes != nil {
			sstOpts.BlockCacheBytes = *opts.SSTable.BlockCacheBytes
		}
		if opts.SSTable.PrefetchBlocks != nil {
			sstOpts.PrefetchBlocks = *opts.SSTable.PrefetchBlocks
		}
		if opts.SSTable.Checksum != nil {
			sstOpts.Checksum = sstable.Checksum(*opts.SSTable.Checksum)
		}
	}
	flusher, err := sstable.NewSSTableWriter(sstOpts)
	if err != nil {
		return nil, err
	}
	manifestPath := filepath.Join(opts.DataDir, "manifest.json")
	manifestStore, err := manifest.NewFileStore(manifestPath)
	if err != nil {
		return nil, err
	}

	eventBus := bus.NewBus(opts.BusBuffer)
	ctx, cancel := context.WithCancel(context.Background())
	lsm := &LSM{
		mem:                  mtFactory(),
		mtFactory:            mtFactory,
		mtPool:               mtPool,
		wal:                  w,
		flusher:              flusher,
		manifest:             manifestStore,
		bus:                  eventBus,
		logger:               logger,
		logCloser:            logCloser,
		sstableOpts:          sstOpts,
		mtLimit:              opts.MemtableLimit,
		autoRepair:           autoRepair,
		missingSegmentPolicy: missingPolicy,
		replayBatchSize:      opts.ReplayBatchSize,
		ctx:                  ctx,
		cancel:               cancel,
	}

	lsm.dispatch = dispatch.NewDispatcher(opts.FlushQueueSize, eventBus, lsm.appendTable)

	m, err := lsm.loadManifest()
	if err != nil {
		return nil, err
	}
	lsm.lastFlush = m.WALSeq
	lsm.seq = m.WALSeq

	if err := lsm.replayWAL(m.WALSeq); err != nil {
		return nil, err
	}

	go lsm.dispatch.Run(ctx, lsm.flusher, lsm.manifest)

	return lsm, nil
}

func (l *LSM) loadManifest() (manifest.Manifest, error) {
	m, err := l.manifest.Load()
	if err != nil {
		return manifest.Manifest{}, err
	}
	if len(m.Tables) == 0 {
		return m, nil
	}
	tables := make([]sstable.SSTable, 0, len(m.Tables))
	for _, t := range m.Tables {
		table, err := sstable.LoadSSTable(t.Path, l.sstableOpts)
		if err != nil {
			return manifest.Manifest{}, err
		}
		tables = append(tables, table)
	}
	l.tablesMu.Lock()
	l.tables = tables
	l.tablesMu.Unlock()
	return m, nil
}

// appendTable is called when a flush completes to keep in-memory list fresh.
func (l *LSM) appendTable(t sstable.SSTable) {
	l.tablesMu.Lock()
	l.tables = append([]sstable.SSTable{t}, l.tables...)
	l.tablesMu.Unlock()
	flushed := l.popFlushedTable()
	l.recycleMemtable(flushed)
}
