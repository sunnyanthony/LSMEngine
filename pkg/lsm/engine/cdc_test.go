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

func TestCDCStreamStartOffsetSignalsDroppedBefore(t *testing.T) {
	store := newCDCStreamStore(10)
	store.setStartOffset(7)

	result, err := store.read("users", 3, 10)
	if err != nil {
		t.Fatalf("read gap: %v", err)
	}
	if !result.DroppedBefore || result.StartOffset != 7 || result.OldestOffset != 8 {
		t.Fatalf("expected start offset gap, got %+v", result)
	}
	if result.FromOffset != 3 || result.NextOffset != 3 {
		t.Fatalf("expected empty gap result to preserve requested offset, got %+v", result)
	}
	if len(result.Events) != 0 {
		t.Fatalf("expected no retained events, got %+v", result.Events)
	}

	store.append(CDCEvent{Offset: 7, ShardID: "users", Operation: "put", Key: []byte("old"), Value: []byte("old"), CommittedAt: time.Now().UTC()})
	store.append(CDCEvent{Offset: 8, ShardID: "users", Operation: "put", Key: []byte("new"), Value: []byte("new"), CommittedAt: time.Now().UTC()})

	result, err = store.read("users", 7, 10)
	if err != nil {
		t.Fatalf("read retained: %v", err)
	}
	if result.DroppedBefore {
		t.Fatalf("did not expect dropped_before at baseline offset: %+v", result)
	}
	if result.StartOffset != 7 || result.OldestOffset != 8 {
		t.Fatalf("unexpected retained window metadata: %+v", result)
	}
	if len(result.Events) != 1 || result.Events[0].Offset != 8 {
		t.Fatalf("expected only post-baseline event, got %+v", result.Events)
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

func TestReadCDCEventsSignalsRestartGap(t *testing.T) {
	dir := t.TempDir()
	store, err := New(Options{DataDir: dir})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	seq, err := store.PutWithSeq([]byte("a"), []byte("1"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	restarted, err := New(Options{DataDir: dir})
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	defer restarted.Close()

	result, err := restarted.ReadCDCEvents("default", 0, 10)
	if err != nil {
		t.Fatalf("read cdc events: %v", err)
	}
	if !result.DroppedBefore {
		t.Fatalf("expected restart gap signal, got %+v", result)
	}
	if result.StartOffset != seq || result.OldestOffset != seq+1 {
		t.Fatalf("expected start offset %d and oldest %d, got %+v", seq, seq+1, result)
	}
	if len(result.Events) != 0 {
		t.Fatalf("expected no in-memory events after restart, got %+v", result.Events)
	}
}

func TestCDCStatusReportsRetainedWindows(t *testing.T) {
	store := newCDCStreamStore(2)
	store.append(CDCEvent{Offset: 1, ShardID: "users", Operation: "put", Key: []byte("a"), Value: []byte("1"), CommittedAt: time.Now().UTC()})
	store.append(CDCEvent{Offset: 2, ShardID: "users", Operation: "put", Key: []byte("b"), Value: []byte("2"), CommittedAt: time.Now().UTC()})
	store.append(CDCEvent{Offset: 3, ShardID: "users", Operation: "delete", Key: []byte("b"), Tombstone: true, CommittedAt: time.Now().UTC()})

	status := store.status([]string{"orders", "users"})
	if status.Durable || status.ReplayOnRestart || status.Source != CDCSourceMemory {
		t.Fatalf("expected in-memory non-durable status, got %+v", status)
	}
	if status.MaxEventsPerShard != 2 {
		t.Fatalf("expected max events 2, got %+v", status)
	}
	if status.StartOffset != 0 {
		t.Fatalf("expected start offset 0, got %+v", status)
	}
	if len(status.Shards) != 2 {
		t.Fatalf("expected users and orders shard status, got %+v", status.Shards)
	}
	if status.Shards[0].ShardID != "orders" || status.Shards[0].StartOffset != 0 || status.Shards[0].RetainedEvents != 0 {
		t.Fatalf("expected empty orders status first, got %+v", status.Shards)
	}
	users := status.Shards[1]
	if users.ShardID != "users" || users.StartOffset != 0 || users.OldestOffset != 2 || users.NextOffset != 3 || users.RetainedEvents != 2 {
		t.Fatalf("unexpected users cdc status: %+v", users)
	}
}
