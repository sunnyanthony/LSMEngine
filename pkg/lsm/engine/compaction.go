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
	l.compactionPlanner = &compaction.StrictLevelledPlanner{
		L0FileThreshold: opts.CompactionL0Threshold,
	}
	l.compactionRunner = &compaction.SimpleRunner{
		Flusher:        l.flusher,
		DropTombstones: opts.CompactionDropTombstones,
	}
	l.compactionApplier = &lsmCompactionApplier{lsm: l}
	l.compactionCh = make(chan struct{}, 1)
	go l.compactionLoop(ctx)
	if l.bus != nil {
		go l.compactionEvents(ctx, l.bus.Subscribe(bus.EventFlushCompleted))
	}
	l.triggerCompaction()
}

func (l *LSM) compactionEvents(ctx context.Context, ch <-chan bus.Event) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ch:
			l.triggerCompaction()
		}
	}
}

func (l *LSM) triggerCompaction() {
	if l.compactionCh == nil {
		return
	}
	select {
	case l.compactionCh <- struct{}{}:
	default:
	}
}

func (l *LSM) compactionLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-l.compactionCh:
			for {
				ran, err := l.runCompactionOnce()
				if err != nil && l.logger != nil {
					l.logger.Printf("compaction: %v", err)
				}
				if !ran {
					break
				}
			}
		}
	}
}

func (l *LSM) runCompactionOnce() (bool, error) {
	if l.compactionPlanner == nil || l.compactionRunner == nil || l.compactionApplier == nil {
		return false, nil
	}
	state := l.compactionState()
	plan, ok, err := l.compactionPlanner.Next(state)
	if err != nil || !ok {
		return false, err
	}
	inputs, err := l.tables.Resolve(plan.Inputs)
	if err != nil {
		return false, err
	}
	result, err := l.compactionRunner.Run(plan, inputs)
	if err != nil {
		return false, err
	}
	if err := l.compactionApplier.Apply(result); err != nil {
		return false, err
	}
	return true, nil
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
