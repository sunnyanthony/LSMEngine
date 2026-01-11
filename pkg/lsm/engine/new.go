package engine

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
	"lsmengine/internal/lsm/metadata"
	"lsmengine/internal/lsm/sstable"
	"lsmengine/internal/lsm/tableset"
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
	if opts.ManifestCheckpointEvery == 0 {
		opts.ManifestCheckpointEvery = 128
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
		if opts.SSTable.RestartInterval != nil {
			sstOpts.RestartInterval = *opts.SSTable.RestartInterval
		}
		if opts.SSTable.IndexPartitionEntries != nil {
			sstOpts.IndexPartitionEntries = *opts.SSTable.IndexPartitionEntries
		}
		if opts.SSTable.FilterPartitioned != nil {
			sstOpts.FilterPartitioned = *opts.SSTable.FilterPartitioned
		}
		if opts.SSTable.ReadBlockMaxBytes != nil {
			sstOpts.ReadBlockMaxBytes = *opts.SSTable.ReadBlockMaxBytes
		}
		if opts.SSTable.ReadBufferMaxBytes != nil {
			sstOpts.ReadBufferMaxBytes = *opts.SSTable.ReadBufferMaxBytes
		}
		if opts.SSTable.UseMmap != nil {
			sstOpts.UseMmap = *opts.SSTable.UseMmap
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
		if opts.SSTable.IndexCacheBytes != nil {
			sstOpts.IndexCacheBytes = *opts.SSTable.IndexCacheBytes
		}
		if opts.SSTable.FilterCacheBytes != nil {
			sstOpts.FilterCacheBytes = *opts.SSTable.FilterCacheBytes
		}
		if opts.SSTable.PrefetchBlocks != nil {
			sstOpts.PrefetchBlocks = *opts.SSTable.PrefetchBlocks
		}
		if opts.SSTable.PrefetchBytes != nil {
			sstOpts.PrefetchBytes = *opts.SSTable.PrefetchBytes
		}
		if opts.SSTable.PrefetchBudgetBlocks != nil {
			sstOpts.PrefetchBudgetBlocks = *opts.SSTable.PrefetchBudgetBlocks
		}
		if opts.SSTable.PrefetchBudgetBytes != nil {
			sstOpts.PrefetchBudgetBytes = *opts.SSTable.PrefetchBudgetBytes
		}
		if opts.SSTable.PrefetchAsync != nil {
			sstOpts.PrefetchAsync = *opts.SSTable.PrefetchAsync
		}
		if opts.SSTable.PrefetchQueueDepth != nil {
			sstOpts.PrefetchQueueDepth = *opts.SSTable.PrefetchQueueDepth
		}
		if opts.SSTable.PrefetchWorkers != nil {
			sstOpts.PrefetchWorkers = *opts.SSTable.PrefetchWorkers
		}
		if opts.SSTable.RestartIntervalAdaptive != nil {
			sstOpts.RestartIntervalAdaptive = *opts.SSTable.RestartIntervalAdaptive
		}
		if opts.SSTable.RestartIntervalMin != nil {
			sstOpts.RestartIntervalMin = *opts.SSTable.RestartIntervalMin
		}
		if opts.SSTable.RestartIntervalMax != nil {
			sstOpts.RestartIntervalMax = *opts.SSTable.RestartIntervalMax
		}
		if opts.SSTable.Checksum != nil {
			sstOpts.Checksum = sstable.Checksum(*opts.SSTable.Checksum)
		}
		if opts.SSTable.PolicyOverride != nil {
			sstOpts.PolicyOverride = opts.SSTable.PolicyOverride
		}
		// Allow caller to attach a FlowObserver via nested SSTable options.
		if opts.SSTable.FlowObserver != nil {
			sstOpts.FlowObserver = opts.SSTable.FlowObserver
		}
	}
	// Or via the top-level convenience field.
	if opts.SSTableFlowObserver != nil {
		sstOpts.FlowObserver = opts.SSTableFlowObserver
	}
	if opts.SSTablePolicyOverride != nil {
		sstOpts.PolicyOverride = opts.SSTablePolicyOverride
	}
	var flowMetrics *sstable.FlowMetrics
	if sstOpts.FlowObserver == nil {
		flowMetrics = &sstable.FlowMetrics{}
		sstOpts.FlowObserver = sstable.NewMetricsObserver(flowMetrics)
	}
	flusher, err := sstable.NewSSTableWriter(sstOpts)
	if err != nil {
		return nil, err
	}
	manifestLogPath := filepath.Join(opts.DataDir, "manifest.log")
	manifestPath := filepath.Join(opts.DataDir, "manifest.json")
	rawManifest, err := manifest.NewLogStore(manifest.LogOptions{
		LogPath:          manifestLogPath,
		CheckpointPath:   manifestPath,
		CheckpointEveryN: opts.ManifestCheckpointEvery,
	})
	if err != nil {
		return nil, err
	}
	manifestStore := manifest.NewLockedStore(rawManifest)

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
		tables:               tableset.NewSet(nil),
		sstableOpts:          sstOpts,
		flowMetrics:          flowMetrics,
		mtLimit:              opts.MemtableLimit,
		autoRepair:           autoRepair,
		missingSegmentPolicy: missingPolicy,
		replayBatchSize:      opts.ReplayBatchSize,
		nodeTerm:             opts.NodeTerm,
		termProvider:         opts.TermProvider,
		ctx:                  ctx,
		cancel:               cancel,
	}

	lsm.dispatch = dispatch.NewDispatcher(opts.FlushQueueSize, eventBus, lsm.onFlush)

	m, err := lsm.loadManifest()
	if err != nil {
		return nil, err
	}
	lsm.lastFlush = m.WALSeq
	lsm.seq = m.WALSeq

	if err := lsm.replayWAL(m.WALSeq); err != nil {
		return nil, err
	}

	go lsm.dispatch.Run(ctx, lsm.flusher)
	lsm.startCompaction(ctx, opts)
	if err := lsm.startReplication(ctx, opts); err != nil {
		_ = lsm.Close()
		return nil, err
	}

	return lsm, nil
}

func (l *LSM) loadManifest() (manifest.Manifest, error) {
	m, err := l.manifest.Load()
	if err != nil {
		return manifest.Manifest{}, err
	}
	if len(m.Tables) == 0 {
		l.replicationState = copyReplicationState(m.Replication)
		return m, nil
	}
	tables := make([]tableset.Table, 0, len(m.Tables))
	for _, t := range m.Tables {
		table, err := sstable.LoadSSTable(t.Path, l.sstableOpts)
		if err != nil {
			return manifest.Manifest{}, err
		}
		meta := metadata.TableMeta{
			Path:      t.Path,
			Level:     t.Level,
			MinKey:    t.MinKey,
			MaxKey:    t.MaxKey,
			SeqMin:    t.SeqMin,
			SeqMax:    t.SeqMax,
			SizeBytes: t.SizeBytes,
		}
		info := table.Info()
		if meta.SeqMax == 0 {
			meta.SeqMax = info.SeqMax
		}
		if meta.SeqMin == 0 {
			meta.SeqMin = info.SeqMin
		}
		if len(meta.MinKey) == 0 {
			meta.MinKey = info.MinKey
		}
		if len(meta.MaxKey) == 0 {
			meta.MaxKey = info.MaxKey
		}
		if meta.SizeBytes == 0 {
			meta.SizeBytes = info.SizeBytes
		}
		tables = append(tables, tableset.Table{Meta: meta, Handle: table})
	}
	l.tables = tableset.NewSet(tables)
	l.replicationState = copyReplicationState(m.Replication)
	return m, nil
}

// onFlush applies a newly flushed table to the table set and manifest.
func (l *LSM) onFlush(t sstable.SSTable) {
	meta := tableMetaFromSSTable(t, 0)
	if err := l.applyTableEdit([]tableset.Table{{Meta: meta, Handle: t}}, nil, t.Seq); err != nil && l.logger != nil {
		l.logger.Printf("flush apply: %v", err)
	}
	if l.compactionService != nil {
		l.compactionService.Trigger()
	}
	flushed := l.popFlushedTable()
	l.recycleMemtable(flushed)
}
