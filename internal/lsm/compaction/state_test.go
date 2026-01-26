package compaction

import "testing"

import "lsmengine/internal/lsm/metadata"

func TestStateFromMetasGroupsAndSorts(t *testing.T) {
	metas := []metadata.TableMeta{
		{Path: "l1.sst", Level: 1},
		{Path: "l0.sst", Level: 0},
		{Path: "l1b.sst", Level: 1},
	}
	state := StateFromMetas(metas)
	if len(state.Levels) != 2 {
		t.Fatalf("expected 2 levels, got %d", len(state.Levels))
	}
	if state.Levels[0].Level != 0 || state.Levels[1].Level != 1 {
		t.Fatalf("unexpected level order: %+v", state.Levels)
	}
	if len(state.Levels[1].Tables) != 2 {
		t.Fatalf("expected two tables at level 1, got %d", len(state.Levels[1].Tables))
	}
}
