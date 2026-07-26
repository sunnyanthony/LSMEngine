package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"lsmengine/internal/lsm/iofs"
)

type statsFailureFS struct{ iofs.FS }

func (fs statsFailureFS) Stat(path string) (os.FileInfo, error) {
	if strings.HasSuffix(path, ".2") {
		return nil, fmt.Errorf("injected segment stat failure")
	}
	return fs.FS.Stat(path)
}

func TestWALStatsPartialScanUsesIOBackend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := NewWAL(Options{Path: path, FS: statsFailureFS{iofs.OSFS{}}})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for i := 0; i < 2; i++ {
		if err := w.rotate(); err != nil {
			t.Fatal(err)
		}
	}
	got := w.Stats()
	if !strings.Contains(got.SegmentScanError, "injected") {
		t.Fatalf("expected backend stat failure: %+v", got)
	}
	if got.ArchivedSegmentCount != 1 || got.SegmentCount != 2 || got.ArchivedSegmentBytes == 0 || got.TotalBytes != got.ActiveSegmentBytes+got.ArchivedSegmentBytes {
		t.Fatalf("inconsistent partial scan: %+v", got)
	}
}

func TestWALStatsExcludesSegmentsAtOrAfterSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := NewWAL(Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	// Model files appearing after the active segment state was sampled.
	for _, id := range []int{1, 2} {
		if err := os.WriteFile(fmt.Sprintf("%s.%d", path, id), []byte("later segment"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	got := w.Stats()
	if got.SegmentCount != 1 || got.ArchivedSegmentCount != 0 || got.TotalBytes != got.ActiveSegmentBytes {
		t.Fatalf("included segment beyond snapshot: %+v", got)
	}
}
