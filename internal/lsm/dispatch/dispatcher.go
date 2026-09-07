// Flush dispatcher for immutable memtables.

package dispatch

import (
	"context"
	"fmt"

	"lsmengine/internal/lsm/sstable"
	"lsmengine/pkg/lsm/bus"
	"lsmengine/pkg/lsm/types"
)

// Dispatcher sends drained memtables to a flusher asynchronously.
type Dispatcher struct {
	queue   chan flushJob
	bus     *bus.Bus
	onFlush func(sstable.SSTable)
}

type flushJob struct {
	entries []types.Entry
	onFlush func(sstable.SSTable)
}

func NewDispatcher(size int, b *bus.Bus, onFlush func(sstable.SSTable)) *Dispatcher {
	return &Dispatcher{
		queue:   make(chan flushJob, size),
		bus:     b,
		onFlush: onFlush,
	}
}

func (d *Dispatcher) Enqueue(entries []types.Entry) bool {
	return d.EnqueueWithCallback(entries, nil)
}

// EnqueueWithCallback associates completion with this specific flush job.
func (d *Dispatcher) EnqueueWithCallback(entries []types.Entry, onFlush func(sstable.SSTable)) bool {
	select {
	case d.queue <- flushJob{entries: entries, onFlush: onFlush}:
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

func (d *Dispatcher) CanEnqueue() bool {
	if d == nil || d.queue == nil {
		return false
	}
	return len(d.queue) < cap(d.queue)
}

// EnqueueBlocking waits until the queue has capacity or ctx is canceled.
func (d *Dispatcher) EnqueueBlocking(ctx context.Context, entries []types.Entry) bool {
	return d.EnqueueBlockingWithCallback(ctx, entries, nil)
}

// EnqueueBlockingWithCallback retains job identity while waiting for capacity.
func (d *Dispatcher) EnqueueBlockingWithCallback(ctx context.Context, entries []types.Entry, onFlush func(sstable.SSTable)) bool {
	if d == nil || d.queue == nil {
		return false
	}
	select {
	case <-ctx.Done():
		return false
	case d.queue <- flushJob{entries: entries, onFlush: onFlush}:
		if d.bus != nil {
			d.bus.Publish(bus.Event{Type: bus.EventFlushScheduled, Sequence: entries[len(entries)-1].Seq})
		}
		return true
	}
}

// Run starts a worker that consumes the queue and flushes. It blocks until ctx is done.
func (d *Dispatcher) Run(ctx context.Context, flusher sstable.Flusher) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case job := <-d.queue:
			table, err := flusher.Flush(job.entries)
			if err != nil {
				return fmt.Errorf("flush: %w", err)
			}
			if job.onFlush != nil {
				job.onFlush(table)
			} else if d.onFlush != nil {
				d.onFlush(table)
			}
			if d.bus != nil {
				d.bus.Publish(bus.Event{Type: bus.EventFlushCompleted, Sequence: table.Seq, Payload: table})
				d.bus.Publish(bus.Event{Type: bus.EventBackpressureOff})
			}
		}
	}
}
