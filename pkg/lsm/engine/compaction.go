// Compaction wiring helpers.

package engine

import (
	"fmt"

	compactionruntime "lsmengine/internal/lsm/compaction/runtime"
	"lsmengine/pkg/lsm/errs"
)

func newCompactionRuntime(l *LSM, opts Options) *compactionruntime.Runtime {
	if l == nil {
		return nil
	}
	return compactionruntime.NewRuntimeFromTables(compactionruntime.RuntimeFromTablesOptions{
		L0FileThreshold: opts.CompactionL0Threshold,
		LevelBaseBytes:  opts.CompactionLevelBaseBytes,
		LevelMultiplier: opts.CompactionLevelMultiplier,
		DropTombstones:  opts.CompactionDropTombstones,
		Flusher:         l.flusher,
		Tables:          l.tables,
		Editor:          l.tableEditor(),
		Logger:          l.logger,
		Metrics:         l.flowMetrics,
		OnError: func(err error) {
			if err != nil && l.logger != nil {
				l.logger.Printf("compaction: %v", err)
			}
		},
	})
}

// TriggerCompaction requests a node-local background compaction pass.
//
// The request wakes the configured compaction runtime; the planner still
// decides whether any tables currently satisfy compaction policy.
func (l *LSM) TriggerCompaction() error {
	if l == nil || l.closed.Load() {
		return errs.ErrClosed
	}
	if l.compactionSvc == nil {
		return fmt.Errorf("lsm: compaction disabled")
	}
	l.compactionSvc.Trigger()
	return nil
}
