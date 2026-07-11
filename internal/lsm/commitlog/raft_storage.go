package commitlog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type raftDiskState struct {
	HardState raftpb.HardState `json:"hard_state"`
	Entries   []raftpb.Entry   `json:"entries"`
	Snapshot  raftpb.Snapshot  `json:"snapshot,omitempty"`
}

type raftHardStateDisk struct {
	HardState raftpb.HardState `json:"hard_state"`
}

type raftSnapshotDisk struct {
	Snapshot raftpb.Snapshot `json:"snapshot"`
}

type raftEntrySegmentDisk struct {
	FirstIndex uint64         `json:"first_index"`
	LastIndex  uint64         `json:"last_index"`
	Entries    []raftpb.Entry `json:"entries"`
}

type raftPersistentStorage struct {
	*raft.MemoryStorage
	dir           string
	hardStatePath string
	snapshotPath  string
	segmentsDir   string
	legacyPath    string
	entries       []raftpb.Entry
	entriesDirty  bool
	snapshot      raftpb.Snapshot
	snapshotDirty bool
}

const raftEntriesPerSegment = 64

func newRaftPersistentStorage(dataDir string, nodeID uint64) (*raftPersistentStorage, bool, error) {
	raftDir := filepath.Join(dataDir, "raft")
	dir := filepath.Join(raftDir, fmt.Sprintf("commitlog-%016x", nodeID))
	storage := &raftPersistentStorage{
		MemoryStorage: raft.NewMemoryStorage(),
		dir:           dir,
		hardStatePath: filepath.Join(dir, "hard_state.json"),
		snapshotPath:  filepath.Join(dir, "snapshot.json"),
		segmentsDir:   filepath.Join(dir, "segments"),
		legacyPath:    filepath.Join(raftDir, fmt.Sprintf("commitlog-%016x.json", nodeID)),
	}
	state, loaded, err := storage.loadRaftDiskState()
	if err != nil {
		return nil, false, err
	}
	if !loaded {
		return storage, false, nil
	}
	if !raft.IsEmptySnap(state.Snapshot) {
		snapshot := cloneRaftSnapshot(state.Snapshot)
		if err := storage.MemoryStorage.ApplySnapshot(snapshot); err != nil {
			return nil, false, fmt.Errorf("restore raft snapshot: %w", err)
		}
		storage.snapshot = snapshot
	}
	if !raft.IsEmptyHardState(state.HardState) {
		if err := storage.MemoryStorage.SetHardState(state.HardState); err != nil {
			return nil, false, fmt.Errorf("restore raft hard state: %w", err)
		}
	}
	if len(state.Entries) > 0 {
		entries := entriesAfterSnapshot(state.Entries, state.Snapshot.Metadata.Index)
		if err := storage.MemoryStorage.Append(entries); err != nil {
			return nil, false, fmt.Errorf("restore raft entries: %w", err)
		}
		storage.entries = entries
	}
	return storage, !raft.IsEmptyHardState(state.HardState) || !raft.IsEmptySnap(state.Snapshot) || len(state.Entries) > 0, nil
}

func (s *raftPersistentStorage) loadRaftDiskState() (raftDiskState, bool, error) {
	state, loaded, err := loadSegmentedRaftDiskState(s.hardStatePath, s.segmentsDir)
	if err != nil {
		return raftDiskState{}, false, err
	}
	if loaded {
		return state, true, nil
	}
	state, loaded, err = loadLegacyRaftDiskState(s.legacyPath)
	if loaded {
		s.entriesDirty = true
		if !raft.IsEmptySnap(state.Snapshot) {
			s.snapshotDirty = true
		}
	}
	return state, loaded, err
}

func loadSegmentedRaftDiskState(hardStatePath string, segmentsDir string) (raftDiskState, bool, error) {
	var state raftDiskState
	loaded := false
	data, err := os.ReadFile(hardStatePath)
	if err == nil {
		var hard raftHardStateDisk
		if err := json.Unmarshal(data, &hard); err != nil {
			return raftDiskState{}, false, fmt.Errorf("decode raft hard state: %w", err)
		}
		state.HardState = hard.HardState
		loaded = true
	} else if !os.IsNotExist(err) {
		return raftDiskState{}, false, fmt.Errorf("read raft hard state: %w", err)
	}
	snapshotPath := filepath.Join(filepath.Dir(hardStatePath), "snapshot.json")
	snapshot, snapshotLoaded, err := loadRaftSnapshot(snapshotPath)
	if err != nil {
		return raftDiskState{}, false, err
	}
	if snapshotLoaded {
		state.Snapshot = snapshot
		loaded = true
	}
	segments, err := loadRaftEntrySegments(segmentsDir)
	if err != nil {
		return raftDiskState{}, false, err
	}
	if len(segments) > 0 {
		state.Entries = segments
		loaded = true
	}
	return state, loaded, nil
}

func loadRaftSnapshot(path string) (raftpb.Snapshot, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return raftpb.Snapshot{}, false, nil
		}
		return raftpb.Snapshot{}, false, fmt.Errorf("read raft snapshot: %w", err)
	}
	var snapshot raftSnapshotDisk
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return raftpb.Snapshot{}, false, fmt.Errorf("decode raft snapshot: %w", err)
	}
	return cloneRaftSnapshot(snapshot.Snapshot), true, nil
}

func loadRaftEntrySegments(segmentsDir string) ([]raftpb.Entry, error) {
	files, err := os.ReadDir(segmentsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read raft log segments: %w", err)
	}
	names := make([]string, 0, len(files))
	for _, file := range files {
		if file.IsDir() || !strings.HasPrefix(file.Name(), "segment-") || !strings.HasSuffix(file.Name(), ".json") {
			continue
		}
		names = append(names, file.Name())
	}
	sort.Strings(names)
	var entries []raftpb.Entry
	for _, name := range names {
		path := filepath.Join(segmentsDir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read raft log segment %q: %w", name, err)
		}
		var segment raftEntrySegmentDisk
		if err := json.Unmarshal(data, &segment); err != nil {
			return nil, fmt.Errorf("decode raft log segment %q: %w", name, err)
		}
		if len(segment.Entries) == 0 {
			continue
		}
		if segment.FirstIndex != segment.Entries[0].Index {
			return nil, fmt.Errorf("raft log segment %q first index mismatch", name)
		}
		if segment.LastIndex != segment.Entries[len(segment.Entries)-1].Index {
			return nil, fmt.Errorf("raft log segment %q last index mismatch", name)
		}
		entries = appendRaftEntries(entries, segment.Entries)
	}
	return entries, nil
}

func loadLegacyRaftDiskState(path string) (raftDiskState, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return raftDiskState{}, false, nil
		}
		return raftDiskState{}, false, fmt.Errorf("read raft state: %w", err)
	}
	var state raftDiskState
	if err := json.Unmarshal(data, &state); err != nil {
		return raftDiskState{}, false, fmt.Errorf("decode raft state: %w", err)
	}
	return state, true, nil
}

func (s *raftPersistentStorage) SetHardState(st raftpb.HardState) error {
	return s.MemoryStorage.SetHardState(st)
}

func (s *raftPersistentStorage) ApplySnapshot(snapshot raftpb.Snapshot) error {
	cloned := cloneRaftSnapshot(snapshot)
	if err := s.MemoryStorage.ApplySnapshot(cloned); err != nil {
		return err
	}
	s.snapshot = cloned
	s.snapshotDirty = true
	s.entries = nil
	s.entriesDirty = true
	return nil
}

func (s *raftPersistentStorage) CreateSnapshot(index uint64, confState *raftpb.ConfState, data []byte) (raftpb.Snapshot, error) {
	snapshot, err := s.MemoryStorage.CreateSnapshot(index, confState, append([]byte(nil), data...))
	if err != nil {
		return raftpb.Snapshot{}, err
	}
	s.snapshot = cloneRaftSnapshot(snapshot)
	s.snapshotDirty = true
	return cloneRaftSnapshot(snapshot), nil
}

func (s *raftPersistentStorage) Compact(index uint64) error {
	if err := s.MemoryStorage.Compact(index); err != nil {
		return err
	}
	s.entries = compactRaftEntries(s.entries, index)
	s.entriesDirty = true
	return nil
}

func (s *raftPersistentStorage) Append(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	cloned := cloneRaftEntries(entries)
	if err := s.MemoryStorage.Append(cloned); err != nil {
		return err
	}
	s.entries = appendRaftEntries(s.entries, cloned)
	s.entriesDirty = true
	return nil
}

func (s *raftPersistentStorage) Persist() error {
	if s == nil {
		return nil
	}
	hardState, _, err := s.InitialState()
	if err != nil {
		return fmt.Errorf("read raft initial state: %w", err)
	}
	if s.snapshotDirty {
		snapshot := raftSnapshotDisk{Snapshot: cloneRaftSnapshot(s.snapshot)}
		data, err := json.MarshalIndent(snapshot, "", "  ")
		if err != nil {
			return fmt.Errorf("encode raft snapshot: %w", err)
		}
		if err := writeAtomicSyncedFile(s.snapshotPath, data, 0o644); err != nil {
			return fmt.Errorf("write raft snapshot: %w", err)
		}
	}
	if s.entriesDirty {
		if err := os.MkdirAll(s.segmentsDir, 0o755); err != nil {
			return fmt.Errorf("create raft state dir: %w", err)
		}
		writtenSegments, err := s.persistEntrySegments()
		if err != nil {
			return err
		}
		if err := s.removeStaleSegments(writtenSegments); err != nil {
			return err
		}
	}
	hard := raftHardStateDisk{HardState: hardState}
	data, err := json.MarshalIndent(hard, "", "  ")
	if err != nil {
		return fmt.Errorf("encode raft hard state: %w", err)
	}
	if err := writeAtomicSyncedFile(s.hardStatePath, data, 0o644); err != nil {
		return fmt.Errorf("write raft hard state: %w", err)
	}
	if s.entriesDirty {
		if err := syncDir(s.segmentsDir); err != nil {
			return fmt.Errorf("sync raft log segments dir: %w", err)
		}
	}
	if err := syncDir(s.dir); err != nil {
		return fmt.Errorf("sync raft state dir: %w", err)
	}
	s.entriesDirty = false
	s.snapshotDirty = false
	return nil
}

func (s *raftPersistentStorage) persistEntrySegments() (map[string]struct{}, error) {
	written := make(map[string]struct{})
	entries := cloneRaftEntries(s.entries)
	for start := 0; start < len(entries); start += raftEntriesPerSegment {
		end := start + raftEntriesPerSegment
		if end > len(entries) {
			end = len(entries)
		}
		chunk := cloneRaftEntries(entries[start:end])
		if len(chunk) == 0 {
			continue
		}
		segment := raftEntrySegmentDisk{
			FirstIndex: chunk[0].Index,
			LastIndex:  chunk[len(chunk)-1].Index,
			Entries:    chunk,
		}
		data, err := json.MarshalIndent(segment, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("encode raft log segment: %w", err)
		}
		name := raftSegmentFileName(segment.FirstIndex)
		path := filepath.Join(s.segmentsDir, name)
		if err := writeAtomicSyncedFile(path, data, 0o644); err != nil {
			return nil, fmt.Errorf("write raft log segment %q: %w", name, err)
		}
		written[name] = struct{}{}
	}
	return written, nil
}

func (s *raftPersistentStorage) removeStaleSegments(written map[string]struct{}) error {
	files, err := os.ReadDir(s.segmentsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read raft log segments for cleanup: %w", err)
	}
	for _, file := range files {
		if file.IsDir() || !strings.HasPrefix(file.Name(), "segment-") || !strings.HasSuffix(file.Name(), ".json") {
			continue
		}
		if _, ok := written[file.Name()]; ok {
			continue
		}
		if err := os.Remove(filepath.Join(s.segmentsDir, file.Name())); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove stale raft log segment %q: %w", file.Name(), err)
		}
	}
	return nil
}

func raftSegmentFileName(firstIndex uint64) string {
	return fmt.Sprintf("segment-%020d.json", firstIndex)
}

func writeAtomicSyncedFile(path string, data []byte, perm os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := writeSyncedFile(tmp, data, perm); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func writeSyncedFile(path string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = f.Close()
		}
	}()
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	closed = true
	return f.Close()
}

func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func cloneRaftEntries(entries []raftpb.Entry) []raftpb.Entry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]raftpb.Entry, len(entries))
	for i := range entries {
		out[i] = entries[i]
		out[i].Data = append([]byte(nil), entries[i].Data...)
	}
	return out
}

func cloneRaftSnapshot(snapshot raftpb.Snapshot) raftpb.Snapshot {
	out := snapshot
	out.Data = append([]byte(nil), snapshot.Data...)
	out.Metadata.ConfState.Voters = append([]uint64(nil), snapshot.Metadata.ConfState.Voters...)
	out.Metadata.ConfState.Learners = append([]uint64(nil), snapshot.Metadata.ConfState.Learners...)
	out.Metadata.ConfState.VotersOutgoing = append([]uint64(nil), snapshot.Metadata.ConfState.VotersOutgoing...)
	out.Metadata.ConfState.LearnersNext = append([]uint64(nil), snapshot.Metadata.ConfState.LearnersNext...)
	return out
}

func entriesAfterSnapshot(entries []raftpb.Entry, snapshotIndex uint64) []raftpb.Entry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]raftpb.Entry, 0, len(entries))
	for _, entry := range entries {
		if entry.Index <= snapshotIndex {
			continue
		}
		out = append(out, entry)
	}
	return cloneRaftEntries(out)
}

func compactRaftEntries(entries []raftpb.Entry, compactIndex uint64) []raftpb.Entry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]raftpb.Entry, 0, len(entries))
	for _, entry := range entries {
		if entry.Index <= compactIndex {
			continue
		}
		out = append(out, entry)
	}
	return cloneRaftEntries(out)
}

func appendRaftEntries(existing []raftpb.Entry, incoming []raftpb.Entry) []raftpb.Entry {
	if len(incoming) == 0 {
		return existing
	}
	out := cloneRaftEntries(existing)
	firstIncoming := incoming[0].Index
	cut := len(out)
	for i, entry := range out {
		if entry.Index >= firstIncoming {
			cut = i
			break
		}
	}
	out = out[:cut]
	out = append(out, cloneRaftEntries(incoming)...)
	return out
}
