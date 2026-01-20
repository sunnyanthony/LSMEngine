package bootstrap

import (
	"lsmengine/internal/lsm/memory"
	"lsmengine/pkg/lsm/types"
)

const defaultReplayBatchSize = 256

// WALReplayer exposes WAL replay using entry views.
type WALReplayer interface {
	ReplayViews(func(memory.EntryView) error) error
}

// ReplayConfig defines WAL replay dependencies.
type ReplayConfig struct {
	WAL        WALReplayer
	Checkpoint uint64
	BatchSize  int
	Build      func(memory.EntryView) types.Entry
	Apply      func([]types.Entry)
	BumpSeq    func(uint64)
}

// ReplayWAL replays WAL entries through the provided callbacks.
func ReplayWAL(cfg ReplayConfig) error {
	if cfg.WAL == nil || cfg.Build == nil {
		return nil
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultReplayBatchSize
	}
	var batch []types.Entry
	flushBatch := func() {
		if len(batch) == 0 {
			return
		}
		if cfg.Apply != nil {
			cfg.Apply(batch)
		}
		if cfg.BumpSeq != nil {
			for i := range batch {
				cfg.BumpSeq(batch[i].Seq)
			}
		}
		batch = batch[:0]
	}
	err := cfg.WAL.ReplayViews(func(view memory.EntryView) error {
		if view.Seq <= cfg.Checkpoint {
			return nil
		}
		owned := cfg.Build(view)
		batch = append(batch, owned)
		if len(batch) >= batchSize {
			flushBatch()
		}
		return nil
	})
	flushBatch()
	return err
}
