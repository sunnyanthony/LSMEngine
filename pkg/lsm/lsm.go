package lsm

import (
	"context"
	"io"
	"sync"

	"lsmengine/pkg/lsm/bus"
	"lsmengine/pkg/lsm/dispatch"
	"lsmengine/pkg/lsm/logging"
	"lsmengine/pkg/lsm/manifest"
	"lsmengine/pkg/lsm/memtable"
	"lsmengine/pkg/lsm/sstable"
	"lsmengine/pkg/lsm/wal"
)

type Options struct {
	DataDir                 string
	MemtableLimit           int
	MemtableConcurrency     int
	MemtableShards          int
	MemtableKind            string
	MemtableFactory         memtable.Factory
	MemtableArenaBlockSize  int
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
	memMu                sync.RWMutex
	wal                  *wal.WAL
	flusher              sstable.Flusher
	manifest             manifest.Store
	dispatch             *dispatch.Dispatcher
	bus                  *bus.Bus
	logger               logging.Logger
	logCloser            io.Closer
	tables               []sstable.SSTable
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
