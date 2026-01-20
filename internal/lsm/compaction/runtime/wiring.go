package runtime

import (
	"lsmengine/internal/lsm/compaction"
	"lsmengine/internal/lsm/logging"
	"lsmengine/internal/lsm/sstable"
	sstableconfig "lsmengine/internal/lsm/sstable/config"
	"lsmengine/internal/lsm/tableedit"
	"lsmengine/internal/lsm/tableset"
)

// RuntimeFromTablesOptions wires a runtime using a tableset snapshot and editor.
type RuntimeFromTablesOptions struct {
	L0FileThreshold int
	LevelBaseBytes  uint64
	LevelMultiplier int
	DropTombstones  bool
	Flusher         sstable.Flusher
	Tables          *tableset.Set
	Editor          tableedit.Editor
	Logger          logging.Logger
	Metrics         *sstableconfig.FlowMetrics
	OnError         func(error)
}

// NewRuntimeFromTables builds a compaction runtime from a live tableset and editor.
func NewRuntimeFromTables(opts RuntimeFromTablesOptions) *Runtime {
	if opts.Tables == nil {
		return nil
	}
	return NewRuntime(RuntimeOptions{
		L0FileThreshold: opts.L0FileThreshold,
		LevelBaseBytes:  opts.LevelBaseBytes,
		LevelMultiplier: opts.LevelMultiplier,
		DropTombstones:  opts.DropTombstones,
		Flusher:         opts.Flusher,
		Resolve:         opts.Tables.Resolve,
		Apply:           applyFromEditor(opts.Editor),
		Logger:          opts.Logger,
		Metrics:         opts.Metrics,
		OnError:         opts.OnError,
	}, func() compaction.State {
		return compaction.StateFromMetas(opts.Tables.Snapshot())
	})
}

func applyFromEditor(editor tableedit.Editor) func(compaction.Result) error {
	return func(result compaction.Result) error {
		if editor == nil {
			return nil
		}
		if hook := currentHooks(); hook != nil && hook.BeforeApply != nil {
			hook.BeforeApply(result)
		}
		outputs := append([]sstable.SSTable(nil), result.Output...)
		add := make([]tableset.Table, 0, len(outputs))
		for _, table := range outputs {
			meta := tableedit.TableMetaFromSSTable(table, result.OutputLevel)
			add = append(add, tableset.Table{Meta: meta, Handle: table})
		}
		err := editor.Apply(add, result.Obsolete, 0)
		if hook := currentHooks(); hook != nil && hook.AfterApply != nil {
			hook.AfterApply(result, err)
		}
		return err
	}
}
