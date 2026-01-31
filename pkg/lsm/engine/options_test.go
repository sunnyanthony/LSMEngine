package engine

import (
	"path/filepath"
	"testing"
)

func TestNormalizeOptionsDefaults(t *testing.T) {
	opts, err := normalizeOptions(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if opts.MemtableLimit == 0 {
		t.Fatalf("expected memtable limit default")
	}
	if opts.TrashDir != filepath.Join(opts.DataDir, "trash") {
		t.Fatalf("expected trash dir default, got %q", opts.TrashDir)
	}
	if opts.ManifestCheckpointEvery == 0 || opts.ReplayBatchSize == 0 {
		t.Fatalf("expected manifest/replay defaults")
	}
	if opts.CloseTimeout <= 0 {
		t.Fatalf("expected close timeout default")
	}
}

func TestNormalizeOptionsRequiresDataDir(t *testing.T) {
	if _, err := normalizeOptions(Options{}); err == nil {
		t.Fatalf("expected error for missing data dir")
	}
}

func TestWalRepairPolicyDefaults(t *testing.T) {
	autoRepair, missing := walRepairPolicy(Options{})
	if !autoRepair {
		t.Fatalf("expected autoRepair default true")
	}
	if missing != MissingSegmentIgnore {
		t.Fatalf("expected missing policy ignore when autoRepair, got %v", missing)
	}
}
