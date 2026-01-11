package strategy

import (
	"testing"

	"lsmengine/internal/lsm/compaction/model"
	"lsmengine/internal/lsm/metadata"
)

func TestStrictLevelledPlannerNil(t *testing.T) {
	var planner *StrictLevelledPlanner
	plan, ok, err := planner.Next(model.State{})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false, got true with plan=%+v", plan)
	}
}

func TestStrictLevelledPlannerNoL0(t *testing.T) {
	planner := &StrictLevelledPlanner{L0FileThreshold: 2}
	plan, ok, err := planner.Next(model.State{
		Levels: []model.Level{
			{Level: 1},
		},
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false, got true with plan=%+v", plan)
	}
}

func TestStrictLevelledPlannerBelowThreshold(t *testing.T) {
	planner := &StrictLevelledPlanner{L0FileThreshold: 3}
	plan, ok, err := planner.Next(model.State{
		Levels: []model.Level{
			{Level: 0, Tables: []metadata.TableMeta{{Path: "a", SeqMax: 1}}},
		},
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false, got true with plan=%+v", plan)
	}
}

func TestStrictLevelledPlannerThresholdMet(t *testing.T) {
	tables := []metadata.TableMeta{
		{Path: "a", SeqMax: 1},
		{Path: "b", SeqMax: 2},
	}
	planner := &StrictLevelledPlanner{L0FileThreshold: 2}
	plan, ok, err := planner.Next(model.State{
		Levels: []model.Level{
			{Level: 0, Tables: tables},
		},
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if plan.OutputLevel != 1 {
		t.Fatalf("expected output level 1, got %d", plan.OutputLevel)
	}
	if len(plan.Inputs) != len(tables) {
		t.Fatalf("expected %d inputs, got %d", len(tables), len(plan.Inputs))
	}
	if plan.Inputs[0].Path != tables[0].Path {
		t.Fatalf("expected first input path %q, got %q", tables[0].Path, plan.Inputs[0].Path)
	}
	tables[0].Path = "mutated"
	if plan.Inputs[0].Path == "mutated" {
		t.Fatalf("expected inputs to be copied, got shared slice")
	}
}
