// Compaction state, plan, and interface definitions.

package compaction

import (
	"lsmengine/internal/lsm/metadata"
	"lsmengine/internal/lsm/sstable"
)

// State describes the current SSTable layout by level.
type State struct {
	Levels []Level
}

// Level groups SSTables for a level.
type Level struct {
	Level  int
	Tables []metadata.TableMeta
}

// Plan is the output of a compaction policy.
type Plan struct {
	Inputs      []metadata.TableMeta
	OutputLevel int
	Reason      string
}

// Planner chooses compaction inputs based on a policy.
type Planner interface {
	Next(state State) (Plan, bool, error)
}

// Result is the output of a compaction run.
type Result struct {
	Output      []sstable.SSTable
	Obsolete    []metadata.TableMeta
	OutputLevel int
}

// Runner executes a compaction plan and returns new tables.
type Runner interface {
	Run(plan Plan, inputs []sstable.SSTable) (Result, error)
}

// Applier installs the result and deletes obsolete tables.
type Applier interface {
	Apply(result Result) error
}

// Resolver maps table metadata to loaded SSTable handles.
type Resolver func([]metadata.TableMeta) ([]sstable.SSTable, error)
