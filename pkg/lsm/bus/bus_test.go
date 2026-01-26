package bus

import (
	"testing"
	"time"
)

func TestBusPublishSubscribe(t *testing.T) {
	b := NewBus(1)
	ch := b.Subscribe(EventFlushScheduled)

	b.Publish(Event{Type: EventFlushScheduled, Sequence: 7})

	select {
	case ev := <-ch:
		if ev.Sequence != 7 {
			t.Fatalf("expected seq=7, got %d", ev.Sequence)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for event")
	}
}

func TestBusDropOnOverflow(t *testing.T) {
	b := NewBus(1)
	ch := b.Subscribe(EventFlushScheduled)

	b.Publish(Event{Type: EventFlushScheduled, Sequence: 1})
	b.Publish(Event{Type: EventFlushScheduled, Sequence: 2})

	<-ch
	select {
	case <-ch:
		t.Fatalf("expected overflow event to be dropped")
	default:
	}
}
