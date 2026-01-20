// Read path service implementation.

package engine

import "lsmengine/pkg/lsm/types"

type readService struct {
	l *LSM
}

func newReadService(l *LSM) *readService {
	return &readService{l: l}
}

func (s *readService) Get(key []byte) (types.Entry, bool) {
	mem, immutables := s.l.memSnapshot()
	if mem != nil {
		if e, ok := mem.Get(key); ok {
			return copyEntry(e), !e.Tombstone
		}
	}
	for _, table := range immutables {
		if e, ok := table.Get(key); ok {
			return copyEntry(e), !e.Tombstone
		}
	}
	for _, table := range s.l.tables.Tables() {
		if view, ok := table.GetView(key); ok {
			entry := types.Entry{
				Key:       view.Key,
				Value:     view.Value,
				Tombstone: view.Tombstone,
				Seq:       view.Seq,
			}
			return copyEntry(entry), !view.Tombstone
		}
	}
	return types.Entry{}, false
}

func copyEntry(entry types.Entry) types.Entry {
	entry.Key = append([]byte(nil), entry.Key...)
	entry.Value = append([]byte(nil), entry.Value...)
	return entry
}
