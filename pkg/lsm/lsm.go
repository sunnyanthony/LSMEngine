package lsm

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"

	"lsmengine/pkg/lsm/bus"
	"lsmengine/pkg/lsm/dispatch"
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
	FlushQueueSize int
	BusBuffer      int
	LogDir         string
	Logger         logging.Logger
}

type LSM struct {
	mem       *memtable.MemTable
	wal       *wal.WAL
	flusher   sstable.Flusher
	manifest  manifest.Store
	dispatch  *dispatch.Dispatcher
	bus       *bus.Bus
	logger    logging.Logger
	logCloser io.Closer
	tables    []sstable.SSTable
	mtLimit   int
	ctx       context.Context
	cancel    context.CancelFunc
	tablesMu  sync.RWMutex
	startOnce sync.Once
}

func New(opts Options) (*LSM, error) {
	if opts.DataDir == "" {
		return nil, fmt.Errorf("data dir required")
	}
	if opts.MemtableLimit == 0 {
		opts.MemtableLimit = 1024
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
	w, err := wal.NewWAL(wal.Options{Path: walPath, Sync: opts.WALSync})
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
		mem:       memtable.NewMemTable(),
		wal:       w,
		flusher:   flusher,
		manifest:  manifestStore,
		bus:       eventBus,
		logger:    logger,
		logCloser: logCloser,
		mtLimit:   opts.MemtableLimit,
		ctx:       ctx,
		cancel:    cancel,
	}

	lsm.dispatch = dispatch.NewDispatcher(opts.FlushQueueSize, eventBus, lsm.appendTable)

	if err := lsm.loadManifest(); err != nil {
		return nil, err
	}

	go lsm.dispatch.Run(ctx, lsm.flusher, lsm.manifest)

	return lsm, nil
}

func (l *LSM) loadManifest() error {
	m, err := l.manifest.Load()
	if err != nil {
		return err
	}
	if len(m.Tables) == 0 {
		return nil
	}
	tables := make([]sstable.SSTable, 0, len(m.Tables))
	for _, t := range m.Tables {
		table, err := sstable.LoadSSTable(t.Path)
		if err != nil {
			return err
		}
		tables = append(tables, table)
	}
	l.tablesMu.Lock()
	l.tables = tables
	l.tablesMu.Unlock()
	return nil
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
