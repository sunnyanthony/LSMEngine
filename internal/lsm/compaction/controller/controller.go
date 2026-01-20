// Compaction coordinator that runs planner/runner/applier.

package controller

import (
	"lsmengine/internal/lsm/compaction"
	"lsmengine/internal/lsm/logging"
	sstableconfig "lsmengine/internal/lsm/sstable/config"
)

// Controller drives compaction by coordinating planner, runner, and applier.
type Controller interface {
	Step(state compaction.State) (bool, error)
}

// Coordinator is a simple in-process controller.
type Coordinator struct {
	Planner compaction.Planner
	Runner  compaction.Runner
	Applier compaction.Applier
	Resolve compaction.Resolver
	Logger  logging.Logger

	Metrics *sstableconfig.FlowMetrics
}

// Step performs one compaction cycle if a plan is available.
func (c *Coordinator) Step(state compaction.State) (bool, error) {
	if c == nil || c.Planner == nil || c.Runner == nil || c.Applier == nil || c.Resolve == nil {
		return false, nil
	}
	if c.Metrics == nil {
		c.Metrics = &sstableconfig.FlowMetrics{}
	}
	plan, ok, err := c.Planner.Next(state)
	if err != nil || !ok {
		return false, err
	}
	if c.Logger != nil {
		c.Logger.Printf("compaction: plan level=%d inputs=%d reason=%s", plan.OutputLevel, len(plan.Inputs), plan.Reason)
	}
	inputs, err := c.Resolve(plan.Inputs)
	if err != nil {
		return false, err
	}
	result, err := c.Runner.Run(plan, inputs)
	if err != nil {
		return false, err
	}
	if err := c.Applier.Apply(result); err != nil {
		return false, err
	}
	if c.Logger != nil {
		c.Logger.Printf("compaction: finished level=%d output=%d obsolete=%d", result.OutputLevel, len(result.Output), len(result.Obsolete))
	}
	return true, nil
}

// MetricsSnapshot exposes accumulated flow metrics (if any).
func (c *Coordinator) MetricsSnapshot() sstableconfig.MetricsSnapshot {
	if c == nil || c.Metrics == nil {
		return sstableconfig.MetricsSnapshot{}
	}
	return c.Metrics.Snapshot()
}
