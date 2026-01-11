package controller

import (
	"lsmengine/internal/lsm/compaction/data"
	"lsmengine/internal/lsm/compaction/model"
	"lsmengine/internal/lsm/logging"
	"lsmengine/internal/lsm/sstable"
)

// Controller drives compaction by coordinating planner, runner, and applier.
type Controller interface {
	Step(state model.State) (bool, error)
}

// Coordinator is a simple in-process controller.
type Coordinator struct {
	Planner model.Planner
	Runner  data.Runner
	Applier data.Applier
	Resolve data.Resolver
	Logger  logging.Logger

	Metrics *sstable.FlowMetrics
}

// Step performs one compaction cycle if a plan is available.
func (c *Coordinator) Step(state model.State) (bool, error) {
	if c == nil || c.Planner == nil || c.Runner == nil || c.Applier == nil || c.Resolve == nil {
		return false, nil
	}
	if c.Metrics == nil {
		c.Metrics = &sstable.FlowMetrics{}
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
func (c *Coordinator) MetricsSnapshot() sstable.MetricsSnapshot {
	if c == nil || c.Metrics == nil {
		return sstable.MetricsSnapshot{}
	}
	return c.Metrics.Snapshot()
}
