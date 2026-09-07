package segment

import (
	"os"
	"path/filepath"
	"testing"
)

func TestListSegmentsDetectsMissing(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "wal.log")
	if err := os.WriteFile(base+".1", []byte("a"), 0o644); err != nil {
		t.Fatalf("write seg1: %v", err)
	}
	if err := os.WriteFile(base+".3", []byte("c"), 0o644); err != nil {
		t.Fatalf("write seg3: %v", err)
	}

	segs, missing, err := ListSegments(base)
	if err != nil {
		t.Fatalf("list segments: %v", err)
	}
	if !missing {
		t.Fatalf("expected missing segments")
	}
	if len(segs) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(segs))
	}
}

func TestListSegmentsAllowsMarkedPrunedPrefix(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "wal.log")
	if err := WritePrunedThrough(base, 2); err != nil {
		t.Fatalf("write pruned marker: %v", err)
	}
	if err := os.WriteFile(base+".3", []byte("c"), 0o644); err != nil {
		t.Fatalf("write seg3: %v", err)
	}
	if err := os.WriteFile(base+".4", []byte("d"), 0o644); err != nil {
		t.Fatalf("write seg4: %v", err)
	}

	segs, missing, err := ListSegments(base)
	if err != nil {
		t.Fatalf("list segments: %v", err)
	}
	if missing {
		t.Fatalf("expected pruned prefix to be accepted")
	}
	if len(segs) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(segs))
	}
}

func TestListSegmentsStillDetectsInternalGapAfterPrunedPrefix(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "wal.log")
	if err := WritePrunedThrough(base, 2); err != nil {
		t.Fatalf("write pruned marker: %v", err)
	}
	if err := os.WriteFile(base+".3", []byte("c"), 0o644); err != nil {
		t.Fatalf("write seg3: %v", err)
	}
	if err := os.WriteFile(base+".5", []byte("e"), 0o644); err != nil {
		t.Fatalf("write seg5: %v", err)
	}

	_, missing, err := ListSegments(base)
	if err != nil {
		t.Fatalf("list segments: %v", err)
	}
	if !missing {
		t.Fatalf("expected internal gap to remain missing")
	}
}

func TestSegmentID(t *testing.T) {
	id, ok := SegmentID(filepath.Join(t.TempDir(), "wal.log.42"))
	if !ok || id != 42 {
		t.Fatalf("expected segment id 42, got id=%d ok=%v", id, ok)
	}
	if _, ok := SegmentID("wal.log.pruned"); ok {
		t.Fatalf("expected non-numeric suffix to be rejected")
	}
}

func TestListSegmentsAcceptsPartialDeletionWithinMarkedPrefix(t *testing.T) {
	base := filepath.Join(t.TempDir(), "wal.log")
	if err := WritePrunedThrough(base, 4); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{".1", ".3", ".5"} {
		if err := os.WriteFile(base+suffix, []byte("segment"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	paths, missing, err := ListSegments(base)
	if err != nil || missing || len(paths) != 3 {
		t.Fatalf("authorized partial deletion: paths=%v missing=%v err=%v", paths, missing, err)
	}
	if err := WritePrunedThrough(base, 2); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadPrunedThrough(base); err != nil || got != 4 {
		t.Fatalf("marker regressed: %d %v", got, err)
	}
	if err := os.WriteFile(base+".7", []byte("segment"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, missing, err := ListSegments(base); err != nil || !missing {
		t.Fatalf("unmarked gap must fail: missing=%v err=%v", missing, err)
	}
}

func TestNextSegmentID(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "wal.log")
	if got := NextSegmentID(base); got != 1 {
		t.Fatalf("expected 1 for empty dir, got %d", got)
	}
	if err := os.WriteFile(base+".2", []byte("b"), 0o644); err != nil {
		t.Fatalf("write seg2: %v", err)
	}
	if got := NextSegmentID(base); got != 3 {
		t.Fatalf("expected 3, got %d", got)
	}
}
