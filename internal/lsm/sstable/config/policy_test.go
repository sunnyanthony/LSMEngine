package config

import "testing"

func TestSnapshotFromOptionsPrefetchBudget(t *testing.T) {
	opts := DefaultOptions("dir")
	opts.PrefetchBlocks = 4
	opts.PrefetchBudgetBlocks = 0
	policy := SnapshotFromOptions(opts, false, false)
	if policy.PrefetchBudgetBlocks != 4 {
		t.Fatalf("expected budget blocks from prefetch blocks, got %d", policy.PrefetchBudgetBlocks)
	}
	if !policy.UsePrefetch {
		t.Fatalf("expected UsePrefetch true")
	}
}

func TestSnapshotFromOptionsPrefetchBudgetBytes(t *testing.T) {
	opts := DefaultOptions("dir")
	opts.PrefetchBlocks = 0
	opts.PrefetchBytes = 512
	opts.PrefetchBudgetBytes = 0
	policy := SnapshotFromOptions(opts, false, false)
	if policy.PrefetchBudgetBytes != 512 {
		t.Fatalf("expected budget bytes from prefetch bytes, got %d", policy.PrefetchBudgetBytes)
	}
}

func TestSnapshotFromOptionsOverride(t *testing.T) {
	override := &PolicySnapshot{
		UsePrefetch:         false,
		PrefetchBudgetBytes: 123,
		ReadBlockMaxBytes:   99,
	}
	opts := DefaultOptions("dir")
	opts.PolicyOverride = override
	policy := SnapshotFromOptions(opts, true, true)
	if policy.PrefetchBudgetBytes != 123 || policy.ReadBlockMaxBytes != 99 {
		t.Fatalf("expected override to be used, got %+v", policy)
	}
}
