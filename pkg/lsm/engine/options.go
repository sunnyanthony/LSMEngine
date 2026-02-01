// Options normalization and component defaults.

package engine

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"lsmengine/internal/lsm/memory/arena"
	memtable "lsmengine/internal/lsm/memtable"
	memtabletable "lsmengine/internal/lsm/memtable/table"
	sstableconfig "lsmengine/internal/lsm/sstable/config"
)

func normalizeOptions(opts Options) (Options, error) {
	if opts.DataDir == "" {
		return opts, fmt.Errorf("data dir required")
	}
	if opts.MemtableLimit == 0 {
		opts.MemtableLimit = 1024
	}
	if opts.MemtableConcurrency == 0 {
		opts.MemtableConcurrency = 2
	}
	if opts.MemtableArenaBlockSize == 0 {
		opts.MemtableArenaBlockSize = arena.DefaultArenaBlockSize
	}
	if opts.WALBlockSize == 0 {
		opts.WALBlockSize = 64 * 1024
	}
	if opts.WALMaxRecord == 0 {
		opts.WALMaxRecord = uint64(opts.WALBlockSize)
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
	if opts.CloseTimeout <= 0 {
		opts.CloseTimeout = 5 * time.Second
	}
	if opts.WebhookTimeout <= 0 {
		opts.WebhookTimeout = 2 * time.Second
	}
	if opts.WebhookQueueDepth <= 0 {
		opts.WebhookQueueDepth = 128
	}
	if opts.WriteEventQueueDepth <= 0 {
		opts.WriteEventQueueDepth = opts.WebhookQueueDepth
	}
	if opts.UDSWriteEventTimeout <= 0 {
		opts.UDSWriteEventTimeout = opts.WebhookTimeout
	}
	if opts.TrashDir == "" {
		opts.TrashDir = filepath.Join(opts.DataDir, "trash")
	}
	if opts.TrashMaxBytes == 0 {
		opts.TrashMaxBytes = 512 << 20
	}
	if opts.TrashMaxFiles == 0 {
		opts.TrashMaxFiles = 1024
	}
	return opts, nil
}

func prepareMemtableFactory(opts Options) (memtable.Factory, *sync.Pool, error) {
	mtFactory := opts.MemtableFactory
	if mtFactory == nil {
		var err error
		mtFactory, err = memtabletable.FactoryForKind(
			opts.MemtableKind,
			opts.MemtableConcurrency,
			opts.MemtableShards,
			opts.MemtableArenaBlockSize,
		)
		if err != nil {
			return nil, nil, err
		}
	}
	if opts.MemtableFactory != nil {
		return mtFactory, nil, nil
	}
	baseFactory := mtFactory
	pool := &sync.Pool{
		New: func() any {
			return baseFactory()
		},
	}
	mtFactory = func() memtable.Table {
		return pool.Get().(memtable.Table)
	}
	return mtFactory, pool, nil
}

func walRepairPolicy(opts Options) (bool, MissingSegmentPolicy) {
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
	return autoRepair, missingPolicy
}

func buildSSTableOptions(opts Options) (sstableconfig.Options, *sstableconfig.FlowMetrics) {
	sstableDir := filepath.Join(opts.DataDir, "sstables")
	sstOpts := sstableconfig.DefaultOptions(sstableDir)
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
			sstOpts.Compression = sstableconfig.Compression(*opts.SSTable.Compression)
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
			sstOpts.Checksum = sstableconfig.Checksum(*opts.SSTable.Checksum)
		}
		if opts.SSTable.PolicyOverride != nil {
			sstOpts.PolicyOverride = opts.SSTable.PolicyOverride
		}
		if opts.SSTable.FlowObserver != nil {
			sstOpts.FlowObserver = opts.SSTable.FlowObserver
		}
	}
	if opts.SSTableFlowObserver != nil {
		sstOpts.FlowObserver = opts.SSTableFlowObserver
	}
	if opts.SSTablePolicyOverride != nil {
		sstOpts.PolicyOverride = opts.SSTablePolicyOverride
	}
	var flowMetrics *sstableconfig.FlowMetrics
	if sstOpts.FlowObserver == nil {
		flowMetrics = &sstableconfig.FlowMetrics{}
		sstOpts.FlowObserver = sstableconfig.NewMetricsObserver(flowMetrics)
	}
	return sstOpts, flowMetrics
}
