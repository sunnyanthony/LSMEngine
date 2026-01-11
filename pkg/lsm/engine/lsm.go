package engine

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"lsmengine/internal/lsm/compaction"
	"lsmengine/internal/lsm/dispatch"
	"lsmengine/internal/lsm/logging"
	"lsmengine/internal/lsm/manifest"
	"lsmengine/internal/lsm/memtable"
	"lsmengine/internal/lsm/sstable"
	"lsmengine/internal/lsm/tableset"
	"lsmengine/internal/lsm/wal"
	"lsmengine/pkg/lsm/bus"
	"lsmengine/pkg/lsm/transport"
)

type Options struct {
	DataDir                  string
	MemtableLimit            int
	MemtableConcurrency      int
	MemtableShards           int
	MemtableKind             string
	MemtableFactory          memtable.Factory
	MemtableArenaBlockSize   int
	SSTable                  *SSTableOptions
	SSTablePolicyOverride    *sstable.PolicySnapshot
	NodeID                   string
	NodeTerm                 uint64
	TermProvider             TermProvider
	Transport                transport.Transport
	ReplicationQueueDepth    int
	ReplicationBatchMax      int
	ReplicationFlushInterval time.Duration
	WALSync                  bool
	WALMaxRecord             uint64
	WALBlockSize             uint32
	WALAsync                 bool
	WALQueueDepth            int
	WALBatchMax              int
	WALAutoRepair            *bool
	WALMissingSegmentPolicy  *MissingSegmentPolicy
	ReplayBatchSize          int
	FlushQueueSize           int
	CompactionL0Threshold    int
	CompactionDropTombstones bool
	BusBuffer                int
	LogDir                   string
	Logger                   logging.Logger

	// SSTableFlowObserver, if set, is propagated to the SSTable read pipeline to
	// collect per-node events/metrics.
	SSTableFlowObserver sstable.FlowObserver
}

type SSTableOptions struct {
	BlockTargetBytes        *int
	BlockMaxBytes           *int
	RestartInterval         *int
	IndexPartitionEntries   *int
	FilterPartitioned       *bool
	ReadBlockMaxBytes       *int
	ReadBufferMaxBytes      *int
	UseMmap                 *bool
	Compression             *string
	BloomBitsPerKey         *int
	BlockCacheBytes         *int64
	IndexCacheBytes         *int64
	FilterCacheBytes        *int64
	PrefetchBlocks          *int
	PrefetchBytes           *int
	PrefetchBudgetBlocks    *int
	PrefetchBudgetBytes     *int
	PrefetchAsync           *bool
	PrefetchQueueDepth      *int
	PrefetchWorkers         *int
	Checksum                *string
	CorruptionPolicy        *string
	RestartIntervalAdaptive *bool
	RestartIntervalMin      *int
	RestartIntervalMax      *int
	FlowObserver            sstable.FlowObserver
	PolicyOverride          *sstable.PolicySnapshot
}

const (
	SSTableCompressionNone     = "none"
	SSTableCompressionSnappy   = "snappy"
	SSTableChecksumCRC32C      = "crc32c"
	SSTableCorruptionFailFast  = "fail-fast"
	SSTableCorruptionSkipBlock = "skip-block"
	SSTableCorruptionDropTable = "drop-table"
)

type MissingSegmentPolicy int

const (
	MissingSegmentError MissingSegmentPolicy = iota
	MissingSegmentIgnore
)

type LSM struct {
	mem                  memtable.Table
	mtFactory            memtable.Factory
	immutables           []memtable.Table
	flushQueue           []memtable.Table
	pinned               map[memtable.Table]int
	memMu                sync.RWMutex
	mtPool               *sync.Pool
	wal                  *wal.WAL
	flusher              sstable.Flusher
	manifest             manifest.Store
	dispatch             *dispatch.Dispatcher
	bus                  *bus.Bus
	logger               logging.Logger
	logCloser            io.Closer
	tables               *tableset.Set
	sstableOpts          sstable.Options
	flowMetrics          *sstable.FlowMetrics
	mtLimit              int
	autoRepair           bool
	ctx                  context.Context
	cancel               context.CancelFunc
	startOnce            sync.Once
	lastFlush            uint64
	seq                  uint64
	missingSegmentPolicy MissingSegmentPolicy
	replayBatchSize      int
	compactionPlanner    compaction.Planner
	compactionRunner     compaction.Runner
	compactionApplier    compaction.Applier
	compactionCh         chan struct{}
	flushBlocked         atomic.Bool
	transport            transport.Transport
	nodeID               string
	nodeTerm             uint64
	termProvider         TermProvider
	replicationCh        chan replicationItem
	replicationBatchMax  int
	replicationFlush     time.Duration
	replicationMu        sync.Mutex
	replicationState     map[string]manifest.ReplicationState
}

// FlowMetrics returns a snapshot of SSTable read-path metrics if enabled.
func (l *LSM) FlowMetrics() sstable.MetricsSnapshot {
	if l == nil || l.flowMetrics == nil {
		return sstable.MetricsSnapshot{}
	}
	return l.flowMetrics.Snapshot()
}
