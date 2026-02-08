// LSM engine options and core runtime state.

package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	compactionruntime "lsmengine/internal/lsm/compaction/runtime"
	"lsmengine/internal/lsm/dispatch"
	"lsmengine/internal/lsm/iofs"
	"lsmengine/internal/lsm/logging"
	"lsmengine/internal/lsm/manifest"
	memtable "lsmengine/internal/lsm/memtable"
	"lsmengine/internal/lsm/sstable"
	sstableconfig "lsmengine/internal/lsm/sstable/config"
	"lsmengine/internal/lsm/tableedit"
	"lsmengine/internal/lsm/tableset"
	wal "lsmengine/internal/lsm/wal"
	"lsmengine/pkg/lsm/bus"
)

type Options struct {
	DataDir                   string
	NodeID                    string
	ClusterID                 string
	StorageMode               string
	Raft                      *RaftOptions
	ShardMap                  []ShardConfig
	MemtableLimit             int
	MemtableConcurrency       int
	MemtableShards            int
	MemtableKind              string
	MemtableFactory           memtable.Factory
	MemtableArenaBlockSize    int
	SSTable                   *SSTableOptions
	SSTablePolicyOverride     *sstableconfig.PolicySnapshot
	ManifestCheckpointEvery   int
	WALSync                   bool
	WALMaxRecord              uint64
	WALBlockSize              uint32
	WALAsync                  bool
	WALQueueDepth             int
	WALBatchMax               int
	WALAutoRepair             *bool
	WALMissingSegmentPolicy   *MissingSegmentPolicy
	ReplayBatchSize           int
	FlushQueueSize            int
	CompactionL0Threshold     int
	CompactionDropTombstones  bool
	CompactionLevelBaseBytes  uint64
	CompactionLevelMultiplier int
	BusBuffer                 int
	LogDir                    string
	Logger                    logging.Logger
	TrashDir                  string
	TrashMaxBytes             int64
	TrashMaxFiles             int
	CloseTimeout              time.Duration
	WebhookURL                string
	WebhookQueueDepth         int
	WebhookTimeout            time.Duration
	WebhookResolver           WebhookResolver
	WriteEventQueueDepth      int
	WriteEventSink            WriteEventSink
	UDSWriteEventPath         string
	UDSWriteEventTimeout      time.Duration
	IOFS                      iofs.FS
	IOAsyncMaxInFlight        int
	IOBackend                 string
	IOBackendStrict           bool

	// SSTableFlowObserver, if set, is propagated to the SSTable read pipeline to
	// collect per-node events/metrics.
	SSTableFlowObserver sstableconfig.FlowObserver
}

const (
	MemtableKindMap             = "map"
	MemtableKindSkipList        = "skiplist"
	MemtableKindShardedSkipList = "sharded-skiplist"
)

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
	FlowObserver            sstableconfig.FlowObserver
	PolicyOverride          *sstableconfig.PolicySnapshot
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
	sstableOpts          sstableconfig.Options
	flowMetrics          *sstableconfig.FlowMetrics
	mtLimit              int
	autoRepair           bool
	ctx                  context.Context
	cancel               context.CancelFunc
	lastFlush            uint64
	seq                  uint64
	missingSegmentPolicy MissingSegmentPolicy
	replayBatchSize      int
	flushBlocked         atomic.Bool
	writer               *writeService
	reader               *readService
	flushSvc             *flushService
	compactionSvc        *compactionruntime.Runtime
	tableEdits           tableedit.Editor
	remover              tableedit.Remover
	ioFS                 iofs.FS
	control              *controlPlane
	bg                   sync.WaitGroup
	closeOnce            sync.Once
	closeErr             error
	closeTimeout         time.Duration
	closing              atomic.Bool
	closed               atomic.Bool
	writeEvents          *writeEventDispatcher
}

// FlowMetrics returns a snapshot of SSTable read-path metrics if enabled.
func (l *LSM) FlowMetrics() sstableconfig.MetricsSnapshot {
	if l == nil || l.flowMetrics == nil {
		return sstableconfig.MetricsSnapshot{}
	}
	return l.flowMetrics.Snapshot()
}

func (l *LSM) notifyWriteEvent(op string, key []byte, seq uint64, status string, err error) {
	if l == nil || l.writeEvents == nil {
		return
	}
	l.writeEvents.notify(WriteEvent{
		Op:     op,
		Key:    key,
		Status: status,
		Seq:    seq,
		Err:    err,
	})
}

func (l *LSM) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		l.closing.Store(true)
		if err := l.flushOnClose(); err != nil {
			if l.logger != nil {
				l.logger.Printf("close flush: %v", err)
			}
			l.closeErr = errors.Join(l.closeErr, err)
		}
		if l.cancel != nil {
			l.cancel()
		}
		l.bg.Wait()
		errOut := l.closeErr
		if l.wal != nil {
			if err := l.wal.Close(); err != nil {
				if l.logger != nil {
					l.logger.Printf("wal close: %v", err)
				}
				errOut = errors.Join(errOut, err)
			}
		}
		if l.tables != nil {
			tables := l.tables.Tables()
			for _, table := range tables {
				if err := table.Close(); err != nil {
					if l.logger != nil {
						l.logger.Printf("table close: %v", err)
					}
					errOut = errors.Join(errOut, err)
				}
			}
			l.cleanupTables(l.tables.Pending())
		}
		if l.logCloser != nil {
			if err := l.logCloser.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "lsm: log close: %v\n", err)
				errOut = errors.Join(errOut, err)
			}
		}
		if c, ok := l.ioFS.(interface{ Close() error }); ok {
			if err := c.Close(); err != nil {
				if l.logger != nil {
					l.logger.Printf("iofs close: %v", err)
				}
				errOut = errors.Join(errOut, err)
			}
		}
		if l.writeEvents != nil {
			l.writeEvents.stop()
		}
		l.closeErr = errOut
		l.closed.Store(true)
	})
	return l.closeErr
}
