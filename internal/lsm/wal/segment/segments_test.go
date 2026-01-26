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
