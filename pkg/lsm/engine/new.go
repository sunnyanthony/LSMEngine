// Engine construction and component wiring.

package engine

import (
	"context"
	"fmt"
	"io"
	"path/filepath"

	"lsmengine/internal/lsm/bootstrap"
	"lsmengine/internal/lsm/cleanup"
	"lsmengine/internal/lsm/dispatch"
	"lsmengine/internal/lsm/logging"
	"lsmengine/internal/lsm/manifest"
	"lsmengine/internal/lsm/sstable"
	"lsmengine/internal/lsm/tableedit"
	"lsmengine/internal/lsm/tableset"
	wal "lsmengine/internal/lsm/wal"
	"lsmengine/pkg/lsm/bus"
)

func New(opts Options) (*LSM, error) {
	var err error
	opts, err = normalizeOptions(opts)
	if err != nil {
		return nil, err
	}
	mtFactory, mtPool, err := prepareMemtableFactory(opts)
	if err != nil {
		return nil, err
	}
	control, err := newControlPlane(opts)
	if err != nil {
		return nil, err
	}
	autoRepair, missingPolicy := walRepairPolicy(opts)

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
		FS:             opts.IOFS,
	})
	if err != nil {
		return nil, err
	}
	sstOpts, flowMetrics := buildSSTableOptions(opts)
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
		FS:               opts.IOFS,
	})
	if err != nil {
		return nil, err
	}
	manifestStore := manifest.NewLockedStore(rawManifest)

	eventBus := bus.NewBus(opts.BusBuffer)
	var remover tableedit.Remover
	if opts.TrashDir != "" && (opts.TrashMaxBytes > 0 || opts.TrashMaxFiles > 0) {
		trash, err := cleanup.NewTrash(opts.TrashDir, opts.TrashMaxBytes, opts.TrashMaxFiles)
		if err != nil {
			return nil, err
		}
		remover = trash
	}
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
		closeTimeout:         opts.CloseTimeout,
		ctx:                  ctx,
		cancel:               cancel,
		remover:              remover,
		ioFS:                 opts.IOFS,
		control:              control,
		commitLog:            control.consensus,
		cdc:                  newCDCStreamStore(0),
	}
	lsm.writer = newWriteService(lsm)
	lsm.reader = newReadService(lsm)
	lsm.flushSvc = newFlushService(lsm)
	if opts.WriteEventSink != nil {
		lsm.writeEvents = newWriteEventDispatcher(
			opts.WriteEventSink,
			opts.WriteEventQueueDepth,
			lsm.logger.Printf,
		)
	} else if opts.UDSWriteEventPath != "" {
		handler := NewUDSWriteEventHandler(
			opts.UDSWriteEventPath,
			opts.UDSWriteEventTimeout,
			lsm.logger.Printf,
		)
		lsm.writeEvents = newWriteEventDispatcher(
			handler,
			opts.WriteEventQueueDepth,
			lsm.logger.Printf,
		)
	} else if opts.WebhookURL != "" || opts.WebhookResolver != nil {
		sink := newWebhookSink(
			opts.WebhookURL,
			opts.WebhookTimeout,
			opts.WebhookResolver,
			lsm.logger.Printf,
		)
		lsm.writeEvents = newWriteEventDispatcher(
			sink,
			opts.WriteEventQueueDepth,
			lsm.logger.Printf,
		)
	}

	lsm.dispatch = dispatch.NewDispatcher(opts.FlushQueueSize, eventBus, lsm.flushSvc.onFlush)

	m, tables, err := bootstrap.LoadManifestTables(lsm.manifest, lsm.sstableOpts)
	if err != nil {
		cancel()
		return nil, err
	}
	if len(tables) > 0 {
		lsm.tables = tableset.NewSet(tables)
	}
	lsm.lastFlush = m.WALSeq
	lsm.seq = m.WALSeq

	if err := lsm.replayWAL(m.WALSeq); err != nil {
		cancel()
		return nil, err
	}
	lsm.commitLogAppliedIndex = lsm.initialCommitLogAppliedIndex()
	if observer, ok := lsm.commitLog.(commitLogIndexObserver); ok {
		observer.ObserveCommittedIndex(lsm.seq)
	}
	if setter, ok := lsm.commitLog.(commitLogCommittedEntryObserverSetter); ok {
		if err := setter.SetCommittedEntryObserver(lsmCommittedEntryObserver{l: lsm}); err != nil {
			cancel()
			return nil, err
		}
	}

	lsm.bg.Add(1)
	go func() {
		defer lsm.bg.Done()
		if err := lsm.dispatch.Run(ctx, lsm.flusher); err != nil && lsm.logger != nil {
			lsm.logger.Printf("flush dispatcher: %v", err)
		}
	}()
	lsm.compactionSvc = newCompactionRuntime(lsm, opts)
	if lsm.compactionSvc != nil {
		lsm.bg.Add(1)
		go func() {
			defer lsm.bg.Done()
			lsm.compactionSvc.Run(ctx)
		}()
		if lsm.bus != nil {
			lsm.bg.Add(1)
			go func() {
				defer lsm.bg.Done()
				ch := lsm.bus.Subscribe(bus.EventFlushCompleted)
				for {
					select {
					case <-ctx.Done():
						return
					case _, ok := <-ch:
						if !ok {
							return
						}
						lsm.compactionSvc.Trigger()
					}
				}
			}()
		}
		lsm.compactionSvc.Trigger()
	}

	return lsm, nil
}
