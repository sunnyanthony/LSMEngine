package model

import "lsmengine/internal/lsm/metadata"

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
