package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotPublicationDoesNotFollowSymlink(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(map[bool]string{false: "exclusive", true: "replace"}[force], func(t *testing.T) {
			dir := t.TempDir()
			target := filepath.Join(dir, "original")
			path := filepath.Join(dir, "snapshot")
			if err := os.WriteFile(target, []byte("original"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
			err := writeStateSnapshotFile(path, []byte("snapshot"), force)
			if (err == nil) != force {
				t.Fatalf("force=%v: %v", force, err)
			}
			data, err := os.ReadFile(target)
			if err != nil || string(data) != "original" {
				t.Fatalf("symlink target changed: %q, %v", data, err)
			}
			if force {
				info, err := os.Lstat(path)
				if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
					t.Fatalf("snapshot must be a private regular file: %v, %v", info, err)
				}
				data, err := os.ReadFile(path)
				if err != nil || string(data) != "snapshot" {
					t.Fatalf("snapshot content: %q, %v", data, err)
				}
			}
			matches, err := filepath.Glob(filepath.Join(dir, ".lsm-snapshot-*"))
			if err != nil || len(matches) != 0 {
				t.Fatalf("temporary files remain: %v, %v", matches, err)
			}
		})
	}
}

func TestSnapshotPublicationFailurePreservesDestination(t *testing.T) {
	dir := t.TempDir()
	if err := writeStateSnapshotFile(dir, []byte("snapshot"), true); err == nil {
		t.Fatal("replacing a directory should fail")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("destination changed: %v, %v", entries, err)
	}
}
