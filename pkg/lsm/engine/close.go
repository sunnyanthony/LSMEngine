// Graceful shutdown helpers.

package engine

import (
	"context"
	"time"

	memtable "lsmengine/internal/lsm/memtable"
)

func (l *LSM) flushOnClose() error {
	if l == nil {
		return nil
	}
	ctx := l.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := l.closeTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if l.flushSvc == nil {
		l.flushSvc = newFlushService(l)
	}

	l.memMu.Lock()
	if l.mem != nil {
		l.freezeMemtableLocked(false)
	}
	immutables := append([]memtable.Table(nil), l.immutables...)
	l.memMu.Unlock()

	for _, table := range immutables {
		if l.flushQueued(table) {
			continue
		}
		l.flushSvc.enqueue(table)
	}

	return l.waitForFlushQueue(ctx)
}

func (l *LSM) isClosing() bool {
	if l == nil {
		return true
	}
	return l.closing.Load() || l.closed.Load()
}

func (l *LSM) flushQueued(table memtable.Table) bool {
	l.memMu.RLock()
	defer l.memMu.RUnlock()
	for _, queued := range l.flushQueue {
		if queued == table {
			return true
		}
	}
	return false
}

func (l *LSM) waitForFlushQueue(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		l.memMu.RLock()
		pending := len(l.flushQueue) > 0 || len(l.immutables) > 0
		l.memMu.RUnlock()
		if !pending {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
