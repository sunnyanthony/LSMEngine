package compaction

import "lsmengine/internal/lsm/sstable"

// State describes the current SSTable layout by level.
type State struct {
	Levels []Level
}

// Level groups SSTables for a level.
type Level struct {
	Level  int
	Tables []sstable.SSTable
}

// Plan is the output of a compaction policy.
type Plan struct {
	Inputs      []sstable.SSTable
	OutputLevel int
	Reason      string
}

// Result is the output of a compaction run.
type Result struct {
	Output      []sstable.SSTable
	Obsolete    []sstable.SSTable
	OutputLevel int
}

// Planner chooses compaction inputs based on a policy.
type Planner interface {
	Next(state State) (Plan, bool, error)
}

// Runner executes a compaction plan and returns new tables.
type Runner interface {
	Run(plan Plan) (Result, error)
}

// Applier installs the result and deletes obsolete tables.
type Applier interface {
	Apply(result Result) error
}

// Engine orchestrates planning, running, and applying compactions.
type Engine struct {
	Planner Planner
	Runner  Runner
	Applier Applier
}

// RunOnce performs a single compaction cycle if a plan is available.
func (e *Engine) RunOnce(state State) (bool, error) {
	if e == nil || e.Planner == nil || e.Runner == nil || e.Applier == nil {
		return false, nil
	}
	plan, ok, err := e.Planner.Next(state)
	if err != nil || !ok {
		return false, err
	}
	result, err := e.Runner.Run(plan)
	if err != nil {
		return false, err
	}
	if err := e.Applier.Apply(result); err != nil {
		return false, err
	}
	return true, nil
}
