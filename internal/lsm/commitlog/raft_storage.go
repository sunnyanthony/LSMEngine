package commitlog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type raftDiskState struct {
	HardState raftpb.HardState `json:"hard_state"`
	Entries   []raftpb.Entry   `json:"entries"`
}

type raftPersistentStorage struct {
	*raft.MemoryStorage
	path    string
	entries []raftpb.Entry
}

func newRaftPersistentStorage(dataDir string, nodeID uint64) (*raftPersistentStorage, bool, error) {
	path := filepath.Join(dataDir, "raft", fmt.Sprintf("commitlog-%016x.json", nodeID))
	storage := &raftPersistentStorage{
		MemoryStorage: raft.NewMemoryStorage(),
		path:          path,
	}
	state, loaded, err := loadRaftDiskState(path)
	if err != nil {
		return nil, false, err
	}
	if !loaded {
		return storage, false, nil
	}
	if !raft.IsEmptyHardState(state.HardState) {
		if err := storage.MemoryStorage.SetHardState(state.HardState); err != nil {
			return nil, false, fmt.Errorf("restore raft hard state: %w", err)
		}
	}
	if len(state.Entries) > 0 {
		entries := cloneRaftEntries(state.Entries)
		if err := storage.MemoryStorage.Append(entries); err != nil {
			return nil, false, fmt.Errorf("restore raft entries: %w", err)
		}
		storage.entries = entries
	}
	return storage, len(state.Entries) > 0, nil
}

func loadRaftDiskState(path string) (raftDiskState, bool, error) {
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

func (s *raftPersistentStorage) Append(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	cloned := cloneRaftEntries(entries)
	if err := s.MemoryStorage.Append(cloned); err != nil {
		return err
	}
	s.entries = appendRaftEntries(s.entries, cloned)
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
	state := raftDiskState{
		HardState: hardState,
		Entries:   cloneRaftEntries(s.entries),
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode raft state: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create raft state dir: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := writeSyncedFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write raft state: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replace raft state: %w", err)
	}
	if err := syncDir(filepath.Dir(s.path)); err != nil {
		return fmt.Errorf("sync raft state dir: %w", err)
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
