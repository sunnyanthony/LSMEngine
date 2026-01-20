// Compaction runtime service wiring.

package runtime

import (
	"context"

	"lsmengine/internal/lsm/compaction"
	"lsmengine/internal/lsm/compaction/controller"
	"lsmengine/internal/lsm/compaction/runner"
	"lsmengine/internal/lsm/compaction/strategy"
	"lsmengine/internal/lsm/logging"
	"lsmengine/internal/lsm/sstable"
	sstableconfig "lsmengine/internal/lsm/sstable/config"
)

// RuntimeOptions configures the compaction runtime wiring.
type RuntimeOptions struct {
	L0FileThreshold int
	LevelBaseBytes  uint64
	LevelMultiplier int
	DropTombstones  bool
	Flusher         sstable.Flusher
	Resolve         compaction.Resolver
	Apply           func(compaction.Result) error
	Logger          logging.Logger
	Metrics         *sstableconfig.FlowMetrics
	OnError         func(error)
}

// Runtime coordinates compaction planning, running, and applying.
type Runtime struct {
	service *controller.Service
}

// NewRuntime builds a compaction runtime using the default strict levelled planner.
func NewRuntime(opts RuntimeOptions, state controller.StateSource) *Runtime {
	if opts.L0FileThreshold <= 0 || opts.Flusher == nil || opts.Resolve == nil || opts.Apply == nil || state == nil {
		return nil
	}
	planner := &strategy.StrictLevelledPlanner{
		L0FileThreshold: opts.L0FileThreshold,
		LevelBaseBytes:  opts.LevelBaseBytes,
		LevelMultiplier: opts.LevelMultiplier,
	}
	runnerImpl := &runner.SimpleRunner{
		Flusher:        opts.Flusher,
		DropTombstones: opts.DropTombstones,
	}
	applier := applyFunc(opts.Apply)
	coord := &controller.Coordinator{
		Planner: planner,
		Runner:  runnerImpl,
		Applier: applier,
		Resolve: opts.Resolve,
		Logger:  opts.Logger,
		Metrics: opts.Metrics,
	}
	service := controller.NewService(coord, state)
	service.OnError = opts.OnError
	return &Runtime{service: service}
}

// Trigger requests a compaction run.
func (r *Runtime) Trigger() {
	if r == nil || r.service == nil {
		return
	}
	r.service.Trigger()
}

// Run executes compaction steps until ctx is canceled.
func (r *Runtime) Run(ctx context.Context) {
	if r == nil || r.service == nil {
		return
	}
	r.service.Run(ctx)
}

type applyFunc func(compaction.Result) error

func (fn applyFunc) Apply(result compaction.Result) error {
	if fn == nil {
		return nil
	}
	return fn(result)
}
