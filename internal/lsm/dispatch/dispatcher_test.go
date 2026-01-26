package dispatch

import (
	"context"
	"testing"
	"time"

	"lsmengine/internal/lsm/sstable"
	"lsmengine/pkg/lsm/bus"
	"lsmengine/pkg/lsm/types"
)

type fakeFlusher struct {
	lastSeq uint64
}

func (f *fakeFlusher) Flush(entries []types.Entry) (sstable.SSTable, error) {
	f.lastSeq = entries[len(entries)-1].Seq
	return sstable.SSTable{Seq: f.lastSeq}, nil
}

func TestDispatcherPublishesEvents(t *testing.T) {
	eventBus := bus.NewBus(4)
	scheduled := eventBus.Subscribe(bus.EventFlushScheduled)
	completed := eventBus.Subscribe(bus.EventFlushCompleted)
	backpressureOff := eventBus.Subscribe(bus.EventBackpressureOff)

	dispatcher := NewDispatcher(1, eventBus, nil)
	entries := []types.Entry{{Key: []byte("a"), Seq: 42}}
	if ok := dispatcher.Enqueue(entries); !ok {
		t.Fatalf("expected enqueue to succeed")
	}
	select {
	case ev := <-scheduled:
		if ev.Sequence != 42 {
			t.Fatalf("expected scheduled seq=42, got %d", ev.Sequence)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for scheduled event")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	flusher := &fakeFlusher{}
	done := make(chan error, 1)
	go func() {
		done <- dispatcher.Run(ctx, flusher)
	}()

	select {
	case ev := <-completed:
		if ev.Sequence != 42 {
			t.Fatalf("expected completed seq=42, got %d", ev.Sequence)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for completed event")
	}
	select {
	case <-backpressureOff:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for backpressure off")
	}
	cancel()
	<-done
}
