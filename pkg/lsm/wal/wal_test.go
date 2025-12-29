package wal

import (
	"lsmengine/pkg/lsm/types"
	"path/filepath"
	"testing"
)

func TestWALAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := NewWAL(Options{Path: path, Sync: true})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	defer w.Close()

	entries := []types.Entry{
		{Key: "a", Value: []byte("1"), Seq: 1},
		{Key: "b", Value: []byte("2"), Seq: 2},
	}
	for _, e := range entries {
		if err := w.Append(e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	wal := &WAL{path: path}
	var replayed []types.Entry
	if err := wal.Replay(func(e types.Entry) error {
		replayed = append(replayed, e)
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(replayed) != 2 {
		t.Fatalf("expected 2 entries replayed, got %d", len(replayed))
	}
	if replayed[0].Key != "a" || string(replayed[0].Value) != "1" {
		t.Fatalf("bad replay entry: %+v", replayed[0])
	}
}
