package engine

import (
	"errors"

	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/types"
)

// replayWAL loads entries above the checkpoint sequence into the memtable.
func (l *LSM) replayWAL(checkpoint uint64) error {
	const defaultReplayBatchSize = 256
	batchSize := l.replayBatchSize
	if batchSize <= 0 {
		batchSize = defaultReplayBatchSize
	}
	var batch []types.Entry
	flushBatch := func() {
		if len(batch) == 0 {
			return
		}
		l.applyEntriesOwned(l.activeMem(), batch)
		for i := range batch {
			l.bumpSeq(batch[i].Seq)
		}
		batch = batch[:0]
	}
	err := l.wal.Replay(func(e types.Entry) error {
		if e.Seq <= checkpoint {
			return nil
		}
		batch = append(batch, e)
		if len(batch) >= batchSize {
			flushBatch()
		}
		return nil
	})
	flushBatch()
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
