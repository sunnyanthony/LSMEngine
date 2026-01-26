//go:build test

package helpers

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func WaitForSSTableFiles(t *testing.T, dir string, want int) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		matches, err := filepath.Glob(filepath.Join(dir, "sstables", "sstable-*.sst"))
		if err != nil {
			t.Fatalf("glob sstables: %v", err)
		}
		if len(matches) >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected %d sstable files within timeout", want)
}

func RemoveWALFiles(t *testing.T, dir string) {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(dir, "wal.log*"))
	if err != nil {
		t.Fatalf("glob wal: %v", err)
	}
	for _, path := range matches {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			t.Fatalf("remove wal %s: %v", path, err)
		}
	}
}
