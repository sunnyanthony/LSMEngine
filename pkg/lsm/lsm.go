package lsm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"

	"lsmengine/pkg/lsm/bus"
	"lsmengine/pkg/lsm/dispatch"
	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/logging"
	"lsmengine/pkg/lsm/manifest"
	"lsmengine/pkg/lsm/memtable"
	"lsmengine/pkg/lsm/sstable"
	"lsmengine/pkg/lsm/types"
	"lsmengine/pkg/lsm/wal"
)

type Options struct {
	DataDir        string
	MemtableLimit  int
	WALSync        bool
	WALMaxRecord   uint64
	WALBlockSize   uint32
	WALAutoRepair  *bool
	FlushQueueSize int
	BusBuffer      int
	LogDir         string
	Logger         logging.Logger
}

type LSM struct {
	mem        *memtable.MemTable
	wal        *wal.WAL
	flusher    sstable.Flusher
	manifest   manifest.Store
	dispatch   *dispatch.Dispatcher
	bus        *bus.Bus
	logger     logging.Logger
	logCloser  io.Closer
	tables     []sstable.SSTable
	mtLimit    int
	autoRepair bool
	ctx        context.Context
	cancel     context.CancelFunc
	tablesMu   sync.RWMutex
	startOnce  sync.Once
	lastFlush  uint64
}

func New(opts Options) (*LSM, error) {
	if opts.DataDir == "" {
		return nil, fmt.Errorf("data dir required")
	}
	if opts.MemtableLimit == 0 {
		opts.MemtableLimit = 1024
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
	if opts.FlushQueueSize == 0 {
		opts.FlushQueueSize = 4
	}
	if opts.BusBuffer == 0 {
		opts.BusBuffer = 16
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
	})
	if err != nil {
		return nil, err
	}
	sstableDir := filepath.Join(opts.DataDir, "sstables")
	flusher, err := sstable.NewSSTableWriter(sstableDir)
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
		mem:        memtable.NewMemTable(),
		wal:        w,
		flusher:    flusher,
		manifest:   manifestStore,
		bus:        eventBus,
		logger:     logger,
		logCloser:  logCloser,
		mtLimit:    opts.MemtableLimit,
		autoRepair: autoRepair,
		ctx:        ctx,
		cancel:     cancel,
	}

	lsm.dispatch = dispatch.NewDispatcher(opts.FlushQueueSize, eventBus, lsm.appendTable)

	m, err := lsm.loadManifest()
	if err != nil {
		return nil, err
	}
	lsm.lastFlush = m.WALSeq

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
		table, err := sstable.LoadSSTable(t.Path)
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
	defer l.tablesMu.Unlock()
	l.tables = append([]sstable.SSTable{t}, l.tables...)
}

func (l *LSM) Put(key string, value []byte) error {
	entry := l.mem.Put(key, value)
	if err := l.wal.Append(entry); err != nil {
		return err
	}
	if l.mem.Size() >= l.mtLimit {
		l.triggerFlush()
	}
	if l.bus != nil {
		l.bus.Publish(bus.Event{Type: bus.EventWalAppended, Sequence: entry.Seq})
	}
	return nil
}

func (l *LSM) Delete(key string) error {
	entry := l.mem.Delete(key)
	if err := l.wal.Append(entry); err != nil {
		return err
	}
	if l.mem.Size() >= l.mtLimit {
		l.triggerFlush()
	}
	if l.bus != nil {
		l.bus.Publish(bus.Event{Type: bus.EventWalAppended, Sequence: entry.Seq})
	}
	return nil
}

func (l *LSM) triggerFlush() {
	entries := l.mem.Drain()
	if len(entries) == 0 {
		return
	}
	if ok := l.dispatch.Enqueue(entries); !ok {
		// backpressure: flush synchronously
		table, err := l.flusher.Flush(entries)
		if err == nil {
			l.appendTable(table)
			_ = l.manifest.Save(manifest.Manifest{
				WALSeq: table.Seq,
				Tables: append([]manifest.Entry(nil), manifest.Entry{Path: table.Path, Seq: table.Seq}),
			})
			if l.logger != nil {
				l.logger.Printf("flush (sync) completed seq=%d", table.Seq)
			}
		} else if l.logger != nil {
			l.logger.Printf("flush (sync) failed: %v", err)
		}
	}
}

func (l *LSM) Get(key string) (types.Entry, bool) {
	if e, ok := l.mem.Get(key); ok {
		return e, true
	}
	l.tablesMu.RLock()
	defer l.tablesMu.RUnlock()
	for _, table := range l.tables {
		if e, ok := table.Get(key); ok {
			return e, !e.Tombstone
		}
	}
	return types.Entry{}, false
}

// replayWAL loads entries above the checkpoint sequence into the memtable.
func (l *LSM) replayWAL(checkpoint uint64) error {
	err := l.wal.Replay(func(e types.Entry) error {
		if e.Seq <= checkpoint {
			return nil
		}
		l.mem.Apply(e)
		return nil
	})
	if err != nil && l.autoRepair && (errors.Is(err, errs.ErrWALMissingSegment) || errors.Is(err, errs.ErrWALCorruptSegment)) {
		if l.logger != nil {
			l.logger.Printf("wal replay degraded: %v", err)
		}
		return nil
	}
	return err
}

func (l *LSM) Close() error {
	l.cancel()
	if l.wal != nil {
		_ = l.wal.Close()
	}
	if l.logCloser != nil {
		_ = l.logCloser.Close()
	}
	return nil
}
