package manifest

import "sync"

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
