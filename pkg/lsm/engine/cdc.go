package engine

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"lsmengine/pkg/lsm/errs"
)

const (
	defaultCDCReadLimit         = 100
	maxCDCReadLimit             = 1000
	defaultCDCMaxEventsPerShard = 4096
)

// CDCEvent is a committed data-change event in a node-local retained stream.
type CDCEvent struct {
	Offset      uint64    `json:"offset"`
	ShardID     string    `json:"shard_id"`
	Operation   string    `json:"operation"`
	Key         []byte    `json:"key,omitempty"`
	Value       []byte    `json:"value,omitempty"`
	Tombstone   bool      `json:"tombstone,omitempty"`
	CommittedAt time.Time `json:"committed_at"`
}

// CDCReadResult is one page of retained in-memory events for a shard.
type CDCReadResult struct {
	ShardID       string     `json:"shard_id"`
	FromOffset    uint64     `json:"from_offset"`
	NextOffset    uint64     `json:"next_offset"`
	OldestOffset  uint64     `json:"oldest_offset"`
	DroppedBefore bool       `json:"dropped_before"`
	Events        []CDCEvent `json:"events"`
}

type cdcStreamStore struct {
	mu       sync.RWMutex
	capacity int
	shards   map[string][]CDCEvent
}

func newCDCStreamStore(capacity int) *cdcStreamStore {
	if capacity <= 0 {
		capacity = defaultCDCMaxEventsPerShard
	}
	return &cdcStreamStore{
		capacity: capacity,
		shards:   make(map[string][]CDCEvent),
	}
}

func (s *cdcStreamStore) append(event CDCEvent) {
	if s == nil {
		return
	}
	event.ShardID = strings.TrimSpace(event.ShardID)
	if event.ShardID == "" || event.Offset == 0 {
		return
	}
	event.Key = append([]byte(nil), event.Key...)
	event.Value = append([]byte(nil), event.Value...)
	s.mu.Lock()
	defer s.mu.Unlock()
	stream := append(s.shards[event.ShardID], event)
	if len(stream) > s.capacity {
		drop := len(stream) - s.capacity
		stream = append([]CDCEvent(nil), stream[drop:]...)
	}
	s.shards[event.ShardID] = stream
}

func (s *cdcStreamStore) read(shardID string, offset uint64, limit int) (CDCReadResult, error) {
	if s == nil {
		return CDCReadResult{}, errs.ErrShardNotFound
	}
	shardID = strings.TrimSpace(shardID)
	if shardID == "" {
		return CDCReadResult{}, fmt.Errorf("cdc shard id required")
	}
	limit = normalizeCDCReadLimit(limit)

	s.mu.RLock()
	stream, ok := s.shards[shardID]
	if !ok {
		s.mu.RUnlock()
		return CDCReadResult{}, errs.ErrShardNotFound
	}
	copied := append([]CDCEvent(nil), stream...)
	s.mu.RUnlock()

	out := emptyCDCReadResult(shardID, offset, limit)
	if len(copied) == 0 {
		return out, nil
	}
	out.OldestOffset = copied[0].Offset
	if out.OldestOffset > 0 && offset+1 < out.OldestOffset {
		out.DroppedBefore = true
	}

	for _, event := range copied {
		if event.Offset <= offset {
			continue
		}
		out.Events = append(out.Events, cloneCDCEvent(event))
		out.NextOffset = event.Offset
		if len(out.Events) >= limit {
			break
		}
	}
	return out, nil
}

func normalizeCDCReadLimit(limit int) int {
	if limit <= 0 {
		return defaultCDCReadLimit
	}
	if limit > maxCDCReadLimit {
		return maxCDCReadLimit
	}
	return limit
}

func emptyCDCReadResult(shardID string, offset uint64, limit int) CDCReadResult {
	limit = normalizeCDCReadLimit(limit)
	return CDCReadResult{
		ShardID:    strings.TrimSpace(shardID),
		FromOffset: offset,
		NextOffset: offset,
		Events:     make([]CDCEvent, 0, limit),
	}
}

func cloneCDCEvent(in CDCEvent) CDCEvent {
	out := in
	out.Key = append([]byte(nil), in.Key...)
	out.Value = append([]byte(nil), in.Value...)
	return out
}

func (l *LSM) recordCDCEvent(op string, key []byte, value []byte, seq uint64, tombstone bool) {
	if l == nil || l.cdc == nil || seq == 0 {
		return
	}
	shardID := "default"
	if l.control != nil {
		if routed, ok := l.control.shardIDForKey(key); ok && strings.TrimSpace(routed) != "" {
			shardID = routed
		}
	}
	l.cdc.append(CDCEvent{
		Offset:      seq,
		ShardID:     shardID,
		Operation:   op,
		Key:         key,
		Value:       value,
		Tombstone:   tombstone,
		CommittedAt: time.Now().UTC(),
	})
}

// ReadCDCEvents returns node-local retained per-shard change events after the given offset.
func (l *LSM) ReadCDCEvents(shardID string, offset uint64, limit int) (CDCReadResult, error) {
	if l == nil || l.cdc == nil {
		return CDCReadResult{}, errs.ErrShardNotFound
	}
	shardID = strings.TrimSpace(shardID)
	knownShard := false
	if l.control != nil {
		shards := l.control.shardsSnapshot()
		if shardID == "" && len(shards) == 1 {
			shardID = shards[0].ID
		}
		for _, shard := range shards {
			if shard.ID == shardID {
				knownShard = true
				break
			}
		}
		if shardID != "" && !knownShard {
			return CDCReadResult{}, errs.ErrShardNotFound
		}
	}
	result, err := l.cdc.read(shardID, offset, limit)
	if err == nil {
		return result, nil
	}
	if knownShard && errors.Is(err, errs.ErrShardNotFound) {
		return emptyCDCReadResult(shardID, offset, limit), nil
	}
	return CDCReadResult{}, err
}
