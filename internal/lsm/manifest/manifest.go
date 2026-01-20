// Manifest types and JSON store implementation.

package manifest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
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

type Manifest struct {
	WALSeq uint64  `json:"wal_seq"`
	Tables []Entry `json:"tables"`
}

type Store interface {
	Load() (Manifest, error)
	Save(Manifest) error
}

// UpdateStore extends Store with an atomic read-modify-write helper.
type UpdateStore interface {
	Store
	Update(func(Manifest) Manifest) error
}

// LockedStore serializes manifest updates to avoid lost writes.
type LockedStore struct {
	store Store
	mu    sync.Mutex
}

// NewLockedStore wraps a Store with a process-local lock.
func NewLockedStore(store Store) *LockedStore {
	return &LockedStore{store: store}
}

func (s *LockedStore) Load() (Manifest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store.Load()
}

func (s *LockedStore) Save(m Manifest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store.Save(m)
}

func (s *LockedStore) Update(fn func(Manifest) Manifest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.store.Load()
	if err != nil {
		return err
	}
	return s.store.Save(fn(m))
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
