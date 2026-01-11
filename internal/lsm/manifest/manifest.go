package manifest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Entry struct {
	Path      string `json:"path"`
	Level     int    `json:"level"`
	MinKey    []byte `json:"min_key,omitempty"`
	MaxKey    []byte `json:"max_key,omitempty"`
	SeqMin    uint64 `json:"seq_min"`
	SeqMax    uint64 `json:"seq_max"`
	SizeBytes uint64 `json:"size_bytes"`
}

type ReplicationState struct {
	Term uint64 `json:"term"`
	Seq  uint64 `json:"seq"`
}

func (r *ReplicationState) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if data[0] != '{' {
		var seq uint64
		if err := json.Unmarshal(data, &seq); err != nil {
			return err
		}
		r.Term = 1
		r.Seq = seq
		return nil
	}
	var aux struct {
		Term uint64 `json:"term"`
		Seq  uint64 `json:"seq"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if aux.Term == 0 {
		aux.Term = 1
	}
	r.Term = aux.Term
	r.Seq = aux.Seq
	return nil
}

type Manifest struct {
	WALSeq      uint64                      `json:"wal_seq"`
	Tables      []Entry                     `json:"tables"`
	Replication map[string]ReplicationState `json:"replication,omitempty"`
}

type Store interface {
	Load() (Manifest, error)
	Save(Manifest) error
}

// FileManifestStore persists manifest as JSON on disk.
type FileManifestStore struct {
	path string
}

func NewFileStore(path string) (*FileManifestStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir manifest dir: %w", err)
	}
	return &FileManifestStore{path: path}, nil
}

func (s *FileManifestStore) Load() (Manifest, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return Manifest{}, nil
		}
		return Manifest{}, fmt.Errorf("read manifest: %w", err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return Manifest{}, fmt.Errorf("unmarshal manifest: %w", err)
	}
	return m, nil
}

func (s *FileManifestStore) Save(m Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write manifest tmp: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("rename manifest: %w", err)
	}
	return nil
}
