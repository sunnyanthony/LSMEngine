// Strict levelled compaction planner.

package strategy

import (
	"bytes"

	"lsmengine/internal/lsm/compaction"
	"lsmengine/internal/lsm/metadata"
)

// StrictLevelledPlanner selects compactions using levelled invariants.
type StrictLevelledPlanner struct {
	// L0FileThreshold triggers compaction when L0 reaches this file count.
	L0FileThreshold int
	// LevelBaseBytes sets the size target for L1. When zero, size-based planning is disabled.
	LevelBaseBytes uint64
	// LevelMultiplier grows level size targets for L2+ (defaults to 10 when LevelBaseBytes > 0).
	LevelMultiplier int
}

// Next returns a plan when L0 exceeds the configured threshold.
func (p *StrictLevelledPlanner) Next(state compaction.State) (compaction.Plan, bool, error) {
	if p == nil {
		return compaction.Plan{}, false, nil
	}
	var l0 compaction.Level
	found := false
	for _, level := range state.Levels {
		if level.Level == 0 {
			l0 = level
			found = true
			break
		}
	}
	if found && p.L0FileThreshold > 0 && len(l0.Tables) >= p.L0FileThreshold {
		inputs := append([]metadata.TableMeta(nil), l0.Tables...)
		inputs = append(inputs, overlapTables(levelByID(state.Levels, 1), l0.Tables)...)
		inputs = dedupe(inputs)
		return compaction.Plan{
			Inputs:      inputs,
			OutputLevel: 1,
			Reason:      "l0 file threshold exceeded",
		}, true, nil
	}

	if p.LevelBaseBytes == 0 {
		return compaction.Plan{}, false, nil
	}
	multiplier := p.LevelMultiplier
	if multiplier <= 0 {
		multiplier = 10
	}
	for level := 1; ; level++ {
		lvl := levelByID(state.Levels, level)
		if lvl == nil {
			break
		}
		limit := levelTargetBytes(p.LevelBaseBytes, multiplier, level)
		if limit == 0 {
			continue
		}
		if levelSizeBytes(lvl.Tables) > limit {
			next := levelByID(state.Levels, level+1)
			inputs := append([]metadata.TableMeta(nil), lvl.Tables...)
			inputs = append(inputs, overlapTables(next, lvl.Tables)...)
			inputs = dedupe(inputs)
			return compaction.Plan{
				Inputs:      inputs,
				OutputLevel: level + 1,
				Reason:      "level size exceeded",
			}, true, nil
		}
	}
	return compaction.Plan{}, false, nil
}

func levelByID(levels []compaction.Level, id int) *compaction.Level {
	for i := range levels {
		if levels[i].Level == id {
			return &levels[i]
		}
	}
	return nil
}

func levelSizeBytes(tables []metadata.TableMeta) uint64 {
	var total uint64
	for _, t := range tables {
		total += t.SizeBytes
	}
	return total
}

func levelTargetBytes(base uint64, multiplier int, level int) uint64 {
	if base == 0 || level <= 0 {
		return 0
	}
	target := base
	for i := 1; i < level; i++ {
		target = target * uint64(multiplier)
	}
	return target
}

func overlapTables(level *compaction.Level, inputs []metadata.TableMeta) []metadata.TableMeta {
	if level == nil || len(level.Tables) == 0 {
		return nil
	}
	out := make([]metadata.TableMeta, 0, len(level.Tables))
	for _, table := range level.Tables {
		for _, in := range inputs {
			if rangesOverlap(in.MinKey, in.MaxKey, table.MinKey, table.MaxKey) {
				out = append(out, table)
				break
			}
		}
	}
	return out
}

func rangesOverlap(aMin, aMax, bMin, bMax []byte) bool {
	if len(aMin) == 0 || len(aMax) == 0 || len(bMin) == 0 || len(bMax) == 0 {
		return true
	}
	if bytes.Compare(aMax, bMin) < 0 {
		return false
	}
	if bytes.Compare(bMax, aMin) < 0 {
		return false
	}
	return true
}

func dedupe(in []metadata.TableMeta) []metadata.TableMeta {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]metadata.TableMeta, 0, len(in))
	for _, t := range in {
		if _, ok := seen[t.Path]; ok {
			continue
		}
		seen[t.Path] = struct{}{}
		out = append(out, t)
	}
	return out
}
