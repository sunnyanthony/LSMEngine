package manifest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Entry struct {
	Path string `json:"path"`
	Seq  uint64 `json:"seq"`
}

type Manifest struct {
	WALSeq uint64  `json:"wal_seq"`
	Tables []Entry `json:"tables"`
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
