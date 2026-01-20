// WAL replay into memtables during startup.

package engine

import (
	"errors"

	"lsmengine/internal/lsm/bootstrap"
	"lsmengine/internal/lsm/memory"
	memtable "lsmengine/internal/lsm/memtable"
	"lsmengine/internal/lsm/tableedit"
	"lsmengine/internal/lsm/tableset"
	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/types"
)

// replayWAL loads entries above the checkpoint sequence into the memtable.
func (l *LSM) replayWAL(checkpoint uint64) error {
	mem := l.activeMem()
	builder := l.entryBuilder(mem)
	err := bootstrap.ReplayWAL(bootstrap.ReplayConfig{
		WAL:        l.wal,
		Checkpoint: checkpoint,
		BatchSize:  l.replayBatchSize,
		Build: func(view memory.EntryView) types.Entry {
			return builder.FromView(view)
		},
		Apply: func(entries []types.Entry) error {
			l.applyEntriesOwned(mem, entries)
			if l.mtLimit > 0 && mem.Size() >= l.mtLimit {
				frozen := l.freezeMemtableIfCurrent(mem)
				if frozen != nil {
					if err := l.flushMemtableForReplay(frozen); err != nil {
						return err
					}
					mem = l.activeMem()
					builder = l.entryBuilder(mem)
				}
			}
			return nil
		},
		BumpSeq: l.bumpSeq,
	})
	if err == nil {
		return nil
	}
	if errors.Is(err, errs.ErrWALMissingSegment) {
		if l.missingSegmentPolicy == MissingSegmentIgnore {
			if l.logger != nil {
				l.logger.Printf("wal replay missing segment: %v", err)
			}
			return nil
		}
		return err
	}
	if l.autoRepair && errors.Is(err, errs.ErrWALCorruptSegment) {
		if l.logger != nil {
			l.logger.Printf("wal replay corrupt segment: %v", err)
		}
		return nil
	}
	return err
}

func (l *LSM) flushMemtableForReplay(table memtable.Table) error {
	if l == nil || table == nil {
		return nil
	}
	if err := table.WaitWriters(l.ctx); err != nil {
		return err
	}
	entries := entriesFromTable(table)
	if len(entries) == 0 {
		l.removeImmutable(table)
		l.recycleMemtable(table)
		return nil
	}
	t, err := l.flusher.Flush(entries)
	if err != nil {
		return err
	}
	meta := tableedit.TableMetaFromSSTable(t, 0)
	add := []tableset.Table{{Meta: meta, Handle: t}}
	if err := l.tableEditor().Apply(add, nil, t.Seq); err != nil {
		return err
	}
	l.removeImmutable(table)
	l.recycleMemtable(table)
	return nil
}
