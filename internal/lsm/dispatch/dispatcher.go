package dispatch

import (
	"context"
	"fmt"
	"lsmengine/internal/lsm/manifest"
	"lsmengine/internal/lsm/sstable"
	"lsmengine/pkg/lsm/bus"
	"lsmengine/pkg/lsm/types"
)

// Dispatcher sends drained memtables to a flusher asynchronously.
type Dispatcher struct {
	queue   chan []types.Entry
	bus     *bus.Bus
	onFlush func(sstable.SSTable)
}

func NewDispatcher(size int, b *bus.Bus, onFlush func(sstable.SSTable)) *Dispatcher {
	return &Dispatcher{
		queue:   make(chan []types.Entry, size),
		bus:     b,
		onFlush: onFlush,
	}
}

func (d *Dispatcher) Enqueue(entries []types.Entry) bool {
	select {
	case d.queue <- entries:
		if d.bus != nil {
			d.bus.Publish(bus.Event{Type: bus.EventFlushScheduled, Sequence: entries[len(entries)-1].Seq})
		}
		return true
	default:
		if d.bus != nil {
			d.bus.Publish(bus.Event{Type: bus.EventBackpressureOn})
		}
		return false
	}
}

// Run starts a worker that consumes the queue and flushes. It blocks until ctx is done.
func (d *Dispatcher) Run(ctx context.Context, flusher sstable.Flusher, mstore manifest.Store) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case entries := <-d.queue:
			table, err := flusher.Flush(entries)
			if err != nil {
				return fmt.Errorf("flush: %w", err)
			}
			if d.onFlush != nil {
				d.onFlush(table)
			}
			if mstore != nil {
				m, err := mstore.Load()
				if err != nil {
					return fmt.Errorf("manifest load: %w", err)
				}
				m.Tables = append([]manifest.Entry{{Path: table.Path, Seq: table.Seq}}, m.Tables...)
				m.WALSeq = table.Seq
				if err := mstore.Save(m); err != nil {
					return fmt.Errorf("manifest save: %w", err)
				}
			}
			if d.bus != nil {
				d.bus.Publish(bus.Event{Type: bus.EventFlushCompleted, Sequence: table.Seq, Payload: table})
				d.bus.Publish(bus.Event{Type: bus.EventBackpressureOff})
			}
		}
	}
}
