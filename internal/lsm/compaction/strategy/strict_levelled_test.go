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

func TestStrictLevelledPlannerL0IncludesOverlap(t *testing.T) {
	l0 := []metadata.TableMeta{
		{Path: "l0a", SeqMax: 2, MinKey: []byte("a"), MaxKey: []byte("c")},
	}
	l1 := []metadata.TableMeta{
		{Path: "l1a", SeqMax: 1, MinKey: []byte("b"), MaxKey: []byte("d")},
		{Path: "l1b", SeqMax: 1, MinKey: []byte("e"), MaxKey: []byte("f")},
	}
	planner := &StrictLevelledPlanner{L0FileThreshold: 1}
	plan, ok, err := planner.Next(model.State{
		Levels: []model.Level{
			{Level: 0, Tables: l0},
			{Level: 1, Tables: l1},
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
	paths := make(map[string]struct{}, len(plan.Inputs))
	for _, in := range plan.Inputs {
		paths[in.Path] = struct{}{}
	}
	if _, ok := paths["l0a"]; !ok {
		t.Fatalf("expected l0 input included")
	}
	if _, ok := paths["l1a"]; !ok {
		t.Fatalf("expected overlapping l1 input included")
	}
	if _, ok := paths["l1b"]; ok {
		t.Fatalf("expected non-overlapping l1 input excluded")
	}
}

func TestStrictLevelledPlannerLevelSizeExceeded(t *testing.T) {
	l1 := []metadata.TableMeta{
		{Path: "l1a", SeqMax: 10, SizeBytes: 8, MinKey: []byte("a"), MaxKey: []byte("b")},
		{Path: "l1b", SeqMax: 9, SizeBytes: 8, MinKey: []byte("c"), MaxKey: []byte("d")},
	}
	l2 := []metadata.TableMeta{
		{Path: "l2a", SeqMax: 7, MinKey: []byte("b"), MaxKey: []byte("c")},
	}
	planner := &StrictLevelledPlanner{
		LevelBaseBytes: 10,
		LevelMultiplier: 10,
	}
	plan, ok, err := planner.Next(model.State{
		Levels: []model.Level{
			{Level: 1, Tables: l1},
			{Level: 2, Tables: l2},
		},
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if plan.OutputLevel != 2 {
		t.Fatalf("expected output level 2, got %d", plan.OutputLevel)
	}
	paths := make(map[string]struct{}, len(plan.Inputs))
	for _, in := range plan.Inputs {
		paths[in.Path] = struct{}{}
	}
	if _, ok := paths["l1a"]; !ok {
		t.Fatalf("expected l1 input included")
	}
	if _, ok := paths["l1b"]; !ok {
		t.Fatalf("expected l1 input included")
	}
	if _, ok := paths["l2a"]; !ok {
		t.Fatalf("expected overlapping l2 input included")
	}
}
