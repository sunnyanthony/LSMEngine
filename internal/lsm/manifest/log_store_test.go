package manifest

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLogStoreUpdateAndReload(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "manifest.log")
	cpPath := filepath.Join(dir, "manifest.json")
	store, err := NewLogStore(LogOptions{
		LogPath:          logPath,
		CheckpointPath:   cpPath,
		CheckpointEveryN: 100,
	})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	if err := store.Update(func(m Manifest) Manifest {
		m.WALSeq = 42
		m.Tables = append(m.Tables, Entry{Path: "t1", Level: 0, SeqMax: 42})
		return m
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	reopen, err := NewLogStore(LogOptions{
		LogPath:          logPath,
		CheckpointPath:   cpPath,
		CheckpointEveryN: 100,
	})
	if err != nil {
		t.Fatalf("new log store reopen: %v", err)
	}
	got, err := reopen.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.WALSeq != 42 || len(got.Tables) != 1 || got.Tables[0].Path != "t1" {
		t.Fatalf("unexpected manifest: %+v", got)
	}
}

func TestLogStoreIgnoresCorruptTail(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "manifest.log")
	cpPath := filepath.Join(dir, "manifest.json")
	store, err := NewLogStore(LogOptions{
		LogPath:          logPath,
		CheckpointPath:   cpPath,
		CheckpointEveryN: 100,
	})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	if err := store.Update(func(m Manifest) Manifest {
		m.WALSeq = 7
		return m
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	if _, err := f.Write([]byte("{bad json\n")); err != nil {
		_ = f.Close()
		t.Fatalf("append corrupt: %v", err)
	}
	_ = f.Close()

	reopen, err := NewLogStore(LogOptions{
		LogPath:          logPath,
		CheckpointPath:   cpPath,
		CheckpointEveryN: 100,
	})
	if err != nil {
		t.Fatalf("new log store reopen: %v", err)
	}
	got, err := reopen.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.WALSeq != 7 {
		t.Fatalf("expected WALSeq=7, got %d", got.WALSeq)
	}
}

func TestLogStoreIgnoresCorruptCheckpointUsesLog(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "manifest.log")
	cpPath := filepath.Join(dir, "manifest.json")
	store, err := NewLogStore(LogOptions{
		LogPath:          logPath,
		CheckpointPath:   cpPath,
		CheckpointEveryN: 100,
	})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	if err := store.Update(func(m Manifest) Manifest {
		m.WALSeq = 99
		m.Tables = append(m.Tables, Entry{Path: "t1", Level: 0, SeqMax: 99})
		return m
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := os.WriteFile(cpPath, []byte("{bad json"), 0o644); err != nil {
		t.Fatalf("write corrupt checkpoint: %v", err)
	}

	reopen, err := NewLogStore(LogOptions{
		LogPath:          logPath,
		CheckpointPath:   cpPath,
		CheckpointEveryN: 100,
	})
	if err != nil {
		t.Fatalf("new log store reopen: %v", err)
	}
	got, err := reopen.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.WALSeq != 99 || len(got.Tables) != 1 || got.Tables[0].Path != "t1" {
		t.Fatalf("unexpected manifest: %+v", got)
	}
}

func TestLogStoreCheckpointTruncatesLog(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "manifest.log")
	cpPath := filepath.Join(dir, "manifest.json")
	store, err := NewLogStore(LogOptions{
		LogPath:          logPath,
		CheckpointPath:   cpPath,
		CheckpointEveryN: 1,
	})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	if err := store.Update(func(m Manifest) Manifest {
		m.WALSeq = 1
		return m
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("stat log: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("expected log truncated, size=%d", info.Size())
	}
}
