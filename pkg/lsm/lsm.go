package lsm

import (
	"context"
	"io"
	"sync"

	"lsmengine/internal/lsm/dispatch"
	"lsmengine/internal/lsm/logging"
	"lsmengine/internal/lsm/manifest"
	"lsmengine/internal/lsm/memtable"
	"lsmengine/internal/lsm/sstable"
	"lsmengine/internal/lsm/wal"
	"lsmengine/pkg/lsm/bus"
)

type Options struct {
	DataDir                 string
	MemtableLimit           int
	MemtableConcurrency     int
	MemtableShards          int
	MemtableKind            string
	MemtableFactory         memtable.Factory
	MemtableArenaBlockSize  int
	SSTable                 *SSTableOptions
	WALSync                 bool
	WALMaxRecord            uint64
	WALBlockSize            uint32
	WALAsync                bool
	WALQueueDepth           int
	WALBatchMax             int
	WALAutoRepair           *bool
	WALMissingSegmentPolicy *MissingSegmentPolicy
	ReplayBatchSize         int
	FlushQueueSize          int
	BusBuffer               int
	LogDir                  string
	Logger                  logging.Logger
}

type SSTableOptions struct {
	BlockTargetBytes *int
	BlockMaxBytes    *int
	Compression      *string
	BloomBitsPerKey  *int
	BlockCacheBytes  *int64
	PrefetchBlocks   *int
	Checksum         *string
}

const (
	SSTableCompressionNone   = "none"
	SSTableCompressionSnappy = "snappy"
	SSTableChecksumCRC32C    = "crc32c"
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
	tables               []sstable.SSTable
	sstableOpts          sstable.Options
	mtLimit              int
	autoRepair           bool
	ctx                  context.Context
	cancel               context.CancelFunc
	tablesMu             sync.RWMutex
	startOnce            sync.Once
	lastFlush            uint64
	seq                  uint64
	missingSegmentPolicy MissingSegmentPolicy
	replayBatchSize      int
}
