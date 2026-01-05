package compaction

import "lsmengine/internal/lsm/sstable"

// StrictLevelledPlanner selects compactions using levelled invariants.
type StrictLevelledPlanner struct {
	// L0FileThreshold triggers compaction when L0 reaches this file count.
	L0FileThreshold int
}

// Next returns a plan when L0 exceeds the configured threshold.
func (p *StrictLevelledPlanner) Next(state State) (Plan, bool, error) {
	if p == nil {
		return Plan{}, false, nil
	}
	var l0 Level
	found := false
	for _, level := range state.Levels {
		if level.Level == 0 {
			l0 = level
			found = true
			break
		}
	}
	if !found {
		return Plan{}, false, nil
	}
	if p.L0FileThreshold > 0 && len(l0.Tables) >= p.L0FileThreshold {
		return Plan{
			Inputs:      append([]sstable.SSTable(nil), l0.Tables...),
			OutputLevel: 1,
			Reason:      "l0 file threshold exceeded",
		}, true, nil
	}
	return Plan{}, false, nil
}
