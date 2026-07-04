package engine

import (
	"testing"
	"time"
)

func TestCDCStreamReadByOffsetAndLimit(t *testing.T) {
	store := newCDCStreamStore(10)
	store.append(CDCEvent{Offset: 1, ShardID: "users", Operation: "put", Key: []byte("a"), Value: []byte("1"), CommittedAt: time.Now().UTC()})
	store.append(CDCEvent{Offset: 2, ShardID: "users", Operation: "put", Key: []byte("b"), Value: []byte("2"), CommittedAt: time.Now().UTC()})
	store.append(CDCEvent{Offset: 3, ShardID: "users", Operation: "delete", Key: []byte("b"), Tombstone: true, CommittedAt: time.Now().UTC()})

	result, err := store.read("users", 1, 1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if result.FromOffset != 1 {
		t.Fatalf("expected from offset 1, got %d", result.FromOffset)
	}
	if len(result.Events) != 1 {
		t.Fatalf("expected one event, got %d", len(result.Events))
	}
	if result.Events[0].Offset != 2 {
		t.Fatalf("expected offset 2, got %d", result.Events[0].Offset)
	}
	if result.NextOffset != 2 {
		t.Fatalf("expected next offset 2, got %d", result.NextOffset)
	}
}

func TestCDCStreamRetentionSetsDroppedBefore(t *testing.T) {
	store := newCDCStreamStore(1)
	store.append(CDCEvent{Offset: 10, ShardID: "users", Operation: "put", Key: []byte("a"), Value: []byte("1"), CommittedAt: time.Now().UTC()})
	store.append(CDCEvent{Offset: 11, ShardID: "users", Operation: "put", Key: []byte("b"), Value: []byte("2"), CommittedAt: time.Now().UTC()})

	result, err := store.read("users", 0, 10)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !result.DroppedBefore {
		t.Fatalf("expected dropped_before=true")
	}
	if result.OldestOffset != 11 {
		t.Fatalf("expected oldest offset 11, got %d", result.OldestOffset)
	}
	if len(result.Events) != 1 || result.Events[0].Offset != 11 {
		t.Fatalf("unexpected retained events: %+v", result.Events)
	}
}

func TestReadCDCEventsReturnsEmptyForKnownShardWithNoEvents(t *testing.T) {
	store, err := New(Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer store.Close()

	result, err := store.ReadCDCEvents("default", 12, 2)
	if err != nil {
		t.Fatalf("read cdc events: %v", err)
	}
	if result.ShardID != "default" {
		t.Fatalf("expected default shard, got %q", result.ShardID)
	}
	if result.FromOffset != 12 || result.NextOffset != 12 {
		t.Fatalf("expected empty result to preserve offset 12, got from=%d next=%d", result.FromOffset, result.NextOffset)
	}
	if len(result.Events) != 0 {
		t.Fatalf("expected no events, got %+v", result.Events)
	}
}
