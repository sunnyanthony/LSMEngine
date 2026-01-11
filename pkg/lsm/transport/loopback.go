package transport

import (
	"context"
	"sync"
	"sync/atomic"
)

// LoopbackOptions configures the in-process transport.
type LoopbackOptions struct {
	Buffer int
}

// Loopback is an in-memory transport for local testing and wiring validation.
type Loopback struct {
	mu     sync.RWMutex
	subs   map[uint64]chan Message
	nextID uint64
	buf    int
	closed uint32
}

func NewLoopback(opts LoopbackOptions) *Loopback {
	buf := opts.Buffer
	if buf <= 0 {
		buf = 256
	}
	return &Loopback{
		subs: make(map[uint64]chan Message),
		buf:  buf,
	}
}

func (l *Loopback) Publish(ctx context.Context, msg Message) error {
	if atomic.LoadUint32(&l.closed) == 1 {
		return ErrClosed
	}
	l.mu.RLock()
	subs := make([]chan Message, 0, len(l.subs))
	for _, ch := range l.subs {
		subs = append(subs, ch)
	}
	l.mu.RUnlock()
	for _, ch := range subs {
		select {
		case ch <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (l *Loopback) Subscribe(ctx context.Context, handler func(Message) error) error {
	if atomic.LoadUint32(&l.closed) == 1 {
		return ErrClosed
	}
	ch := make(chan Message, l.buf)
	id := atomic.AddUint64(&l.nextID, 1)
	l.mu.Lock()
	l.subs[id] = ch
	l.mu.Unlock()

	go func() {
		defer l.remove(id, ch)
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				_ = handler(msg)
			}
		}
	}()
	return nil
}

func (l *Loopback) Close() error {
	if !atomic.CompareAndSwapUint32(&l.closed, 0, 1) {
		return nil
	}
	l.mu.Lock()
	for id, ch := range l.subs {
		close(ch)
		delete(l.subs, id)
	}
	l.mu.Unlock()
	return nil
}

func (l *Loopback) remove(id uint64, ch chan Message) {
	l.mu.Lock()
	_, ok := l.subs[id]
	if ok {
		delete(l.subs, id)
	}
	l.mu.Unlock()
	if ok {
		close(ch)
	}
}
