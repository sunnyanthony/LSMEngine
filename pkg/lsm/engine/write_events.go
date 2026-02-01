// Write event dispatch for async notifications.

package engine

import (
	"context"
	"sync"
)

// WriteEvent captures an outcome for a write operation.
type WriteEvent struct {
	Op     string
	Key    []byte
	Status string
	Seq    uint64
	Err    error
}

// WriteEventSink receives write events, potentially forwarding them elsewhere.
type WriteEventSink interface {
	HandleWrite(ctx context.Context, event WriteEvent)
}

type writeEventDispatcher struct {
	sink   WriteEventSink
	queue  chan WriteEvent
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	logger func(string, ...any)
}

func newWriteEventDispatcher(sink WriteEventSink, depth int, logger func(string, ...any)) *writeEventDispatcher {
	if sink == nil {
		return nil
	}
	if depth <= 0 {
		depth = 128
	}
	ctx, cancel := context.WithCancel(context.Background())
	d := &writeEventDispatcher{
		sink:   sink,
		queue:  make(chan WriteEvent, depth),
		ctx:    ctx,
		cancel: cancel,
		logger: logger,
	}
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.run()
	}()
	return d
}

func (d *writeEventDispatcher) notify(event WriteEvent) {
	if d == nil {
		return
	}
	select {
	case d.queue <- event:
	default:
		if d.logger != nil {
			d.logger("write event dropped: queue full")
		}
	}
}

func (d *writeEventDispatcher) stop() {
	if d == nil {
		return
	}
	d.cancel()
	d.wg.Wait()
}

func (d *writeEventDispatcher) run() {
	for {
		select {
		case <-d.ctx.Done():
			return
		case event := <-d.queue:
			d.sink.HandleWrite(d.ctx, event)
		}
	}
}
