package engine

import (
	"context"
	"os"
	"sort"

	"lsmengine/internal/lsm/compaction"
	"lsmengine/internal/lsm/metadata"
	"lsmengine/internal/lsm/sstable"
	"lsmengine/internal/lsm/tableset"
	"lsmengine/pkg/lsm/bus"
)

func (l *LSM) startCompaction(ctx context.Context, opts Options) {
	if opts.CompactionL0Threshold <= 0 {
		return
	}
	planner := &compaction.StrictLevelledPlanner{
		L0FileThreshold: opts.CompactionL0Threshold,
	}
	runner := &compaction.SimpleRunner{
		Flusher:        l.flusher,
		DropTombstones: opts.CompactionDropTombstones,
	}
	applier := &lsmCompactionApplier{lsm: l}
	controller := &compaction.Coordinator{
		Planner: planner,
		Runner:  runner,
		Applier: applier,
		Resolve: l.tables.Resolve,
		Logger:  l.logger,
		Metrics: l.flowMetrics,
	}
	service := compaction.NewService(controller, l.compactionState)
	service.OnError = func(err error) {
		if err != nil && l.logger != nil {
			l.logger.Printf("compaction: %v", err)
		}
	}
	l.compactionService = service
	go service.Run(ctx)
	if l.bus != nil {
		scheduler := compaction.NewScheduler(service, compaction.FlushTriggerPolicy{})
		go scheduler.Run(ctx, l.bus.Subscribe(bus.EventFlushCompleted))
	}
	service.Trigger()
}

func (l *LSM) compactionState() compaction.State {
	metas := l.tables.Snapshot()
	levels := make(map[int][]metadata.TableMeta)
	for _, meta := range metas {
		levels[meta.Level] = append(levels[meta.Level], meta)
	}
	levelList := make([]compaction.Level, 0, len(levels))
	for level, tables := range levels {
		levelList = append(levelList, compaction.Level{Level: level, Tables: tables})
	}
	sort.Slice(levelList, func(i, j int) bool {
		return levelList[i].Level < levelList[j].Level
	})
	return compaction.State{
		Levels: levelList,
	}
}

type lsmCompactionApplier struct {
	lsm *LSM
}

func (a *lsmCompactionApplier) Apply(result compaction.Result) error {
	if a == nil || a.lsm == nil {
		return nil
	}
	l := a.lsm
	outputs := append([]sstable.SSTable(nil), result.Output...)
	add := make([]tableset.Table, 0, len(outputs))
	for _, table := range outputs {
		meta := tableMetaFromSSTable(table, result.OutputLevel)
		add = append(add, tableset.Table{Meta: meta, Handle: table})
	}
	obsoleteHandles, _ := l.tables.Resolve(result.Obsolete)
	if err := l.applyTableEdit(add, result.Obsolete, 0); err != nil {
		return err
	}
	for _, table := range obsoleteHandles {
		_ = table.Close()
	}
	for _, meta := range result.Obsolete {
		if err := os.Remove(meta.Path); err != nil && !os.IsNotExist(err) {
			if l.logger != nil {
				l.logger.Printf("compaction: remove obsolete %s: %v", meta.Path, err)
			}
		}
	}
	return nil
}
