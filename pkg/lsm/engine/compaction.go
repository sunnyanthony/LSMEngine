// Compaction wiring helpers.

package engine

import (
	compactionruntime "lsmengine/internal/lsm/compaction/runtime"
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
