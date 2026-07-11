package commitlog

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestRaftPersistentStoragePersistsSegmentedLayout(t *testing.T) {
	dataDir := t.TempDir()
	nodeID := uint64(1)
	storage, loaded, err := newRaftPersistentStorage(dataDir, nodeID)
	if err != nil {
		t.Fatalf("new storage: %v", err)
	}
	if loaded {
		t.Fatalf("expected fresh storage")
	}
	entries := testRaftEntries(1, raftEntriesPerSegment+2)
	if err := storage.Append(entries); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := storage.SetHardState(raftpb.HardState{Term: 2, Commit: entries[len(entries)-1].Index}); err != nil {
		t.Fatalf("set hard state: %v", err)
	}
	if err := storage.Persist(); err != nil {
		t.Fatalf("persist: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dataDir, "raft", "commitlog-0000000000000001", "hard_state.json")); err != nil {
		t.Fatalf("expected hard state file: %v", err)
	}
	segments := readRaftSegmentNames(t, filepath.Join(dataDir, "raft", "commitlog-0000000000000001", "segments"))
	if len(segments) != 2 {
		t.Fatalf("expected 2 segments, got %d (%v)", len(segments), segments)
	}

	restarted, loaded, err := newRaftPersistentStorage(dataDir, nodeID)
	if err != nil {
		t.Fatalf("restart storage: %v", err)
	}
	if !loaded {
		t.Fatalf("expected restarted storage to load log")
	}
	if got := restarted.entries; len(got) != len(entries) {
		t.Fatalf("expected %d restored entries, got %d", len(entries), len(got))
	}
	hardState, _, err := restarted.InitialState()
	if err != nil {
		t.Fatalf("initial state: %v", err)
	}
	if hardState.Commit != entries[len(entries)-1].Index || hardState.Term != 2 {
		t.Fatalf("unexpected restored hard state: %+v", hardState)
	}
}

func TestRaftPersistentStorageLoadsLegacyJSON(t *testing.T) {
	dataDir := t.TempDir()
	nodeID := uint64(2)
	entries := testRaftEntries(1, 3)
	legacy := raftDiskState{
		HardState: raftpb.HardState{Term: 3, Commit: 3},
		Entries:   entries,
	}
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatalf("marshal legacy: %v", err)
	}
	legacyPath := filepath.Join(dataDir, "raft", "commitlog-0000000000000002.json")
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(legacyPath, data, 0o644); err != nil {
		t.Fatalf("write legacy: %v", err)
	}

	storage, loaded, err := newRaftPersistentStorage(dataDir, nodeID)
	if err != nil {
		t.Fatalf("load legacy storage: %v", err)
	}
	if !loaded {
		t.Fatalf("expected legacy log to load")
	}
	if len(storage.entries) != len(entries) {
		t.Fatalf("expected %d legacy entries, got %d", len(entries), len(storage.entries))
	}
	if err := storage.Persist(); err != nil {
		t.Fatalf("persist migrated layout: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "raft", "commitlog-0000000000000002", "hard_state.json")); err != nil {
		t.Fatalf("expected migrated hard state: %v", err)
	}
}

func TestRaftPersistentStorageRemovesStaleSegmentsOnOverwrite(t *testing.T) {
	dataDir := t.TempDir()
	nodeID := uint64(3)
	storage, _, err := newRaftPersistentStorage(dataDir, nodeID)
	if err != nil {
		t.Fatalf("new storage: %v", err)
	}
	if err := storage.Append(testRaftEntries(1, raftEntriesPerSegment*2+1)); err != nil {
		t.Fatalf("append initial: %v", err)
	}
	if err := storage.Persist(); err != nil {
		t.Fatalf("persist initial: %v", err)
	}
	segmentsDir := filepath.Join(dataDir, "raft", "commitlog-0000000000000003", "segments")
	if got := len(readRaftSegmentNames(t, segmentsDir)); got != 3 {
		t.Fatalf("expected 3 initial segments, got %d", got)
	}

	if err := storage.Append(testRaftEntries(uint64(raftEntriesPerSegment+1), 2)); err != nil {
		t.Fatalf("append overwrite: %v", err)
	}
	if err := storage.Persist(); err != nil {
		t.Fatalf("persist overwrite: %v", err)
	}
	if got := len(readRaftSegmentNames(t, segmentsDir)); got != 2 {
		t.Fatalf("expected stale segment cleanup to leave 2 segments, got %d", got)
	}
}

func testRaftEntries(first uint64, count int) []raftpb.Entry {
	entries := make([]raftpb.Entry, count)
	for i := range entries {
		index := first + uint64(i)
		entries[i] = raftpb.Entry{
			Index: index,
			Term:  1,
			Data:  []byte{byte(index)},
		}
	}
	return entries
}

func readRaftSegmentNames(t *testing.T, dir string) []string {
	t.Helper()
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read segments: %v", err)
	}
	var names []string
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		names = append(names, file.Name())
	}
	return names
}
