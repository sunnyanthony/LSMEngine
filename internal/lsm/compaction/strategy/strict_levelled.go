package strategy

import (
	"lsmengine/internal/lsm/compaction/model"
	"lsmengine/internal/lsm/metadata"
)

// StrictLevelledPlanner selects compactions using levelled invariants.
type StrictLevelledPlanner struct {
	// L0FileThreshold triggers compaction when L0 reaches this file count.
	L0FileThreshold int
}

// Next returns a plan when L0 exceeds the configured threshold.
func (p *StrictLevelledPlanner) Next(state model.State) (model.Plan, bool, error) {
	if p == nil {
		return model.Plan{}, false, nil
	}
	var l0 model.Level
	found := false
	for _, level := range state.Levels {
		if level.Level == 0 {
			l0 = level
			found = true
			break
		}
	}
	if !found {
		return model.Plan{}, false, nil
	}
	if p.L0FileThreshold > 0 && len(l0.Tables) >= p.L0FileThreshold {
		return model.Plan{
			Inputs:      append([]metadata.TableMeta(nil), l0.Tables...),
			OutputLevel: 1,
			Reason:      "l0 file threshold exceeded",
		}, true, nil
	}
	return model.Plan{}, false, nil
}
