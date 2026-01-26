package cleanup

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTrashRemovePrunesByCount(t *testing.T) {
	trashDir := t.TempDir()
	trash, err := NewTrash(trashDir, 0, 1)
	if err != nil {
		t.Fatalf("new trash: %v", err)
	}

	dataDir := t.TempDir()
	first := filepath.Join(dataDir, "a.sst")
	second := filepath.Join(dataDir, "b.sst")
	if err := os.WriteFile(first, []byte("one"), 0o644); err != nil {
		t.Fatalf("write first: %v", err)
	}
	if err := os.WriteFile(second, []byte("two"), 0o644); err != nil {
		t.Fatalf("write second: %v", err)
	}

	oldTime := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(first, oldTime, oldTime); err != nil {
		t.Fatalf("chtimes first: %v", err)
	}

	if err := trash.Remove(first); err != nil {
		t.Fatalf("remove first: %v", err)
	}
	if err := trash.Remove(second); err != nil {
		t.Fatalf("remove second: %v", err)
	}

	entries, err := os.ReadDir(trashDir)
	if err != nil {
		t.Fatalf("read trash: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 trashed file, got %d", len(entries))
	}
}

func TestTrashRemovePrunesByBytes(t *testing.T) {
	trashDir := t.TempDir()
	trash, err := NewTrash(trashDir, 1, 0)
	if err != nil {
		t.Fatalf("new trash: %v", err)
	}

	dataDir := t.TempDir()
	path := filepath.Join(dataDir, "big.sst")
	if err := os.WriteFile(path, []byte("abcdef"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	if err := trash.Remove(path); err != nil {
		t.Fatalf("remove: %v", err)
	}
	entries, err := os.ReadDir(trashDir)
	if err != nil {
		t.Fatalf("read trash: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected trash to be pruned, got %d files", len(entries))
	}
}
