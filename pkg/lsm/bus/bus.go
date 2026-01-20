// Event bus implementation for engine notifications.

package bus

import "sync"

type EventType string

const (
	EventWalAppended     EventType = "wal_appended"
	EventFlushScheduled  EventType = "flush_scheduled"
	EventFlushCompleted  EventType = "flush_completed"
	EventBackpressureOn  EventType = "backpressure_on"
	EventBackpressureOff EventType = "backpressure_off"
)

type Event struct {
	Type     EventType
	Sequence uint64
	Payload  any
}

// Bus is a lightweight, non-blocking pub/sub.
type Bus struct {
	mu    sync.RWMutex
	subs  map[EventType][]chan Event
	bufSz int
}

func NewBus(bufSz int) *Bus {
	return &Bus{
		subs:  make(map[EventType][]chan Event),
		bufSz: bufSz,
	}
}

func (b *Bus) Subscribe(t EventType) <-chan Event {
	ch := make(chan Event, b.bufSz)
	b.mu.Lock()
	b.subs[t] = append(b.subs[t], ch)
	b.mu.Unlock()
	return ch
}

func (b *Bus) Publish(ev Event) {
	b.mu.RLock()
	subs := append([]chan Event(nil), b.subs[ev.Type]...)
	b.mu.RUnlock()
	for _, ch := range subs {
		select {
		case ch <- ev:
		default:
			// drop on overflow; a metric hook could be added later
		}
	}
}
