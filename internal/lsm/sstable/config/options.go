// SSTable options, defaults, and validation.

package config

import (
	"fmt"
	"sync/atomic"

	"lsmengine/internal/lsm/iofs"
)

type Compression string

const (
	CompressionNone   Compression = "none"
	CompressionSnappy Compression = "snappy"
)

type Checksum string

const (
	ChecksumCRC32C Checksum = "crc32c"
)

type CorruptionPolicy string

const (
	CorruptionFailFast  CorruptionPolicy = "fail-fast"
	CorruptionSkipBlock CorruptionPolicy = "skip-block"
	CorruptionDropTable CorruptionPolicy = "drop-table"
)

type Options struct {
	Dir                     string
	FS                      iofs.FS
	BlockTargetBytes        int
	BlockMaxBytes           int
	RestartInterval         int
	IndexPartitionEntries   int
	FilterPartitioned       bool
	ReadBlockMaxBytes       int
	ReadBufferMaxBytes      int
	UseMmap                 bool
	Compression             Compression
	BloomBitsPerKey         int
	BlockCacheBytes         int64
	IndexCacheBytes         int64
	FilterCacheBytes        int64
	PrefetchBlocks          int
	PrefetchBytes           int
	PrefetchBudgetBlocks    int
	PrefetchBudgetBytes     int
	PrefetchAsync           bool
	PrefetchQueueDepth      int
	PrefetchWorkers         int
	Checksum                Checksum
	CorruptionPolicy        CorruptionPolicy
	RestartIntervalAdaptive bool
	RestartIntervalMin      int
	RestartIntervalMax      int
	FlowObserver            FlowObserver
	// PolicyOverride lets a controller supply a precomputed policy snapshot
	// without mutating the data-path options.
	PolicyOverride *PolicySnapshot
}

func DefaultOptions(dir string) Options {
	return Options{
		Dir:                     dir,
		FS:                      iofs.OSFS{},
		BlockTargetBytes:        64 * 1024,
		BlockMaxBytes:           256 * 1024,
		RestartInterval:         16,
		IndexPartitionEntries:   256,
		FilterPartitioned:       true,
		ReadBlockMaxBytes:       0,
		ReadBufferMaxBytes:      0,
		Compression:             CompressionSnappy,
		BloomBitsPerKey:         10,
		BlockCacheBytes:         64 * 1024 * 1024,
		IndexCacheBytes:         0,
		FilterCacheBytes:        0,
		PrefetchBlocks:          2,
		PrefetchBytes:           0,
		PrefetchBudgetBlocks:    0,
		PrefetchBudgetBytes:     0,
		PrefetchAsync:           false,
		PrefetchQueueDepth:      64,
		PrefetchWorkers:         1,
		Checksum:                ChecksumCRC32C,
		CorruptionPolicy:        CorruptionFailFast,
		RestartIntervalAdaptive: false,
		RestartIntervalMin:      0,
		RestartIntervalMax:      0,
	}
}

func (o *Options) Normalize() {
	if o.FS == nil {
		o.FS = iofs.OSFS{}
	}
	indexCacheDisabled := o.IndexCacheBytes < 0
	filterCacheDisabled := o.FilterCacheBytes < 0
	readBufferDisabled := o.ReadBufferMaxBytes < 0
	if o.BlockTargetBytes <= 0 {
		o.BlockTargetBytes = 64 * 1024
	}
	if o.BlockMaxBytes <= 0 {
		o.BlockMaxBytes = 256 * 1024
	}
	if o.BlockMaxBytes < o.BlockTargetBytes {
		o.BlockMaxBytes = o.BlockTargetBytes
	}
	if o.RestartInterval <= 0 {
		o.RestartInterval = 16
	}
	if o.RestartIntervalMin <= 0 {
		o.RestartIntervalMin = o.RestartInterval
	}
	if o.RestartIntervalMax <= 0 {
		o.RestartIntervalMax = o.RestartInterval
	}
	if o.RestartIntervalMin > o.RestartIntervalMax {
		o.RestartIntervalMin = o.RestartIntervalMax
	}
	if o.RestartInterval < o.RestartIntervalMin {
		o.RestartInterval = o.RestartIntervalMin
	}
	if o.RestartInterval > o.RestartIntervalMax {
		o.RestartInterval = o.RestartIntervalMax
	}
	if o.IndexPartitionEntries < 0 {
		o.IndexPartitionEntries = 0
	}
	if o.ReadBlockMaxBytes <= 0 && o.BlockMaxBytes > 0 {
		o.ReadBlockMaxBytes = o.BlockMaxBytes * 4
	}
	if o.ReadBlockMaxBytes > 0 && o.ReadBlockMaxBytes < o.BlockMaxBytes {
		o.ReadBlockMaxBytes = o.BlockMaxBytes
	}
	if readBufferDisabled {
		o.ReadBufferMaxBytes = 0
	} else if o.ReadBufferMaxBytes == 0 {
		o.ReadBufferMaxBytes = o.ReadBlockMaxBytes
	}
	if o.Compression == "" {
		o.Compression = CompressionSnappy
	}
	if o.Checksum == "" {
		o.Checksum = ChecksumCRC32C
	}
	if o.PrefetchBlocks < 0 {
		o.PrefetchBlocks = 0
	}
	if o.PrefetchBytes < 0 {
		o.PrefetchBytes = 0
	}
	if o.PrefetchBudgetBlocks < 0 {
		o.PrefetchBudgetBlocks = 0
	}
	if o.PrefetchBudgetBytes < 0 {
		o.PrefetchBudgetBytes = 0
	}
	if o.PrefetchQueueDepth <= 0 {
		o.PrefetchQueueDepth = 64
	}
	if o.PrefetchWorkers <= 0 {
		o.PrefetchWorkers = 1
	}
	if o.CorruptionPolicy == "" {
		o.CorruptionPolicy = CorruptionFailFast
	}
	if indexCacheDisabled {
		o.IndexCacheBytes = 0
	}
	if filterCacheDisabled {
		o.FilterCacheBytes = 0
	}
	if o.BlockCacheBytes > 0 {
		if o.IndexCacheBytes == 0 && !indexCacheDisabled {
			o.IndexCacheBytes = o.BlockCacheBytes / 8
		}
		if o.FilterCacheBytes == 0 && !filterCacheDisabled {
			o.FilterCacheBytes = o.BlockCacheBytes / 8
		}
	}
}

func (o *Options) Validate() error {
	switch o.Compression {
	case CompressionNone, CompressionSnappy:
	default:
		return fmt.Errorf("unsupported compression %q", o.Compression)
	}
	switch o.Checksum {
	case ChecksumCRC32C:
	default:
		return fmt.Errorf("unsupported checksum %q", o.Checksum)
	}
	switch o.CorruptionPolicy {
	case CorruptionFailFast, CorruptionSkipBlock, CorruptionDropTable:
	default:
		return fmt.Errorf("unsupported corruption policy %q", o.CorruptionPolicy)
	}
	return nil
}

// PolicySnapshot is a read-only set of tunables the controller can issue to the
// pipeline without touching data path logic.
type PolicySnapshot struct {
	UsePrefetch          bool
	PrefetchBudgetBytes  int
	PrefetchBudgetBlocks int
	PrefetchAsync        bool
	PrefetchQueueDepth   int
	PrefetchWorkers      int
	UsePartitionedIndex  bool
	UsePartitionedFilter bool
	UseMmap              bool
	ReadBufferMaxBytes   int
	ReadBlockMaxBytes    int
	CorruptionPolicy     CorruptionPolicy
}

func SnapshotFromOptions(opts Options, partitionedIndex bool, partitionedFilter bool) PolicySnapshot {
	budgetBytes := opts.PrefetchBudgetBytes
	budgetBlocks := opts.PrefetchBudgetBlocks
	// Backward compat: fold lookahead knobs into the budget so prefetch has a single source of truth.
	if budgetBytes == 0 && opts.PrefetchBytes > 0 {
		budgetBytes = opts.PrefetchBytes
	}
	if budgetBlocks == 0 && opts.PrefetchBlocks > 0 {
		budgetBlocks = opts.PrefetchBlocks
	}
	base := PolicySnapshot{
		UsePrefetch:          budgetBytes > 0 || budgetBlocks > 0 || opts.PrefetchAsync,
		PrefetchBudgetBytes:  budgetBytes,
		PrefetchBudgetBlocks: budgetBlocks,
		PrefetchAsync:        opts.PrefetchAsync,
		PrefetchQueueDepth:   opts.PrefetchQueueDepth,
		PrefetchWorkers:      opts.PrefetchWorkers,
		UsePartitionedIndex:  partitionedIndex,
		UsePartitionedFilter: partitionedFilter,
		UseMmap:              opts.UseMmap,
		ReadBufferMaxBytes:   opts.ReadBufferMaxBytes,
		ReadBlockMaxBytes:    opts.ReadBlockMaxBytes,
		CorruptionPolicy:     opts.CorruptionPolicy,
	}
	if opts.PolicyOverride != nil {
		return *opts.PolicyOverride
	}
	return base
}

// FlowEvent is emitted to observers; it is a copy of the pipeline state.
type FlowEvent struct {
	Key      []byte
	Node     string
	CacheHit bool
	Mmapped  bool
	Err      error
}

// FlowObserver receives events from the pipeline; observers should be fast.
type FlowObserver interface {
	OnNode(event FlowEvent, node string)
	OnError(event FlowEvent, node string, err error)
}

// FlowMetrics aggregates lightweight counters from FlowEvents.
type FlowMetrics struct {
	cacheHit   atomic.Uint64
	cacheMiss  atomic.Uint64
	filterPass atomic.Uint64
	filterSkip atomic.Uint64
	errors     atomic.Uint64
}

func (m *FlowMetrics) Record(event FlowEvent, isFilter bool) {
	if event.Err != nil {
		m.errors.Add(1)
		return
	}
	if isFilter {
		m.filterPass.Add(1)
		return
	}
	if event.CacheHit {
		m.cacheHit.Add(1)
	} else {
		m.cacheMiss.Add(1)
	}
}

type MetricsSnapshot struct {
	CacheHit   uint64
	CacheMiss  uint64
	FilterPass uint64
	FilterSkip uint64
	Errors     uint64
}

func (m *FlowMetrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		CacheHit:   m.cacheHit.Load(),
		CacheMiss:  m.cacheMiss.Load(),
		FilterPass: m.filterPass.Load(),
		FilterSkip: m.filterSkip.Load(),
		Errors:     m.errors.Load(),
	}
}

// MetricsObserver is a FlowObserver that accumulates metrics.
type MetricsObserver struct {
	metrics *FlowMetrics
}

func NewMetricsObserver(target *FlowMetrics) *MetricsObserver {
	if target == nil {
		target = &FlowMetrics{}
	}
	return &MetricsObserver{metrics: target}
}

func (o *MetricsObserver) OnNode(event FlowEvent, node string) {
	if o == nil || o.metrics == nil {
		return
	}
	o.metrics.Record(event, node == "filter")
}

func (o *MetricsObserver) OnError(event FlowEvent, node string, err error) {
	if o == nil || o.metrics == nil {
		return
	}
	o.metrics.Record(FlowEvent{Err: err}, false)
}
