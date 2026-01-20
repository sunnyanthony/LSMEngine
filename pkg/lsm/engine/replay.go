// WAL replay into memtables during startup.

package engine

import (
	"errors"

	"lsmengine/internal/lsm/bootstrap"
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
		Build:      builder.FromView,
		Apply: func(entries []types.Entry) {
			l.applyEntriesOwned(mem, entries)
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
