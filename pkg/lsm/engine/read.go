// Point read API entry point.

package engine

import (
	"context"
	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/types"
	"sync"
	"time"
)

// GetContext reads this node's locally applied state, not a quorum snapshot.
// It preserves storage failures and waits safely against shutdown/compaction.
func (l *LSM) GetContext(ctx context.Context, key []byte) (types.Entry, bool, error) {
	if l == nil {
		return types.Entry{}, false, errs.ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return types.Entry{}, false, err
	}
	if l.isClosing() {
		return types.Entry{}, false, errs.ErrClosed
	}
	if err := readLockContext(ctx, &l.readMu); err != nil {
		return types.Entry{}, false, err
	}
	defer l.readMu.RUnlock()
	if l.isClosing() {
		return types.Entry{}, false, errs.ErrClosed
	}
	// Keep memtables from being recycled until their values have been copied.
	if err := readLockContext(ctx, &l.memMu); err != nil {
		return types.Entry{}, false, err
	}
	if l.mem != nil {
		if entry, ok := l.mem.Get(key); ok {
			entry = copyEntry(entry)
			l.memMu.RUnlock()
			if err := ctx.Err(); err != nil {
				return types.Entry{}, false, err
			}
			return entry, !entry.Tombstone, nil
		}
	}
	for i := len(l.immutables) - 1; i >= 0; i-- {
		if entry, ok := l.immutables[i].Get(key); ok {
			entry = copyEntry(entry)
			l.memMu.RUnlock()
			if err := ctx.Err(); err != nil {
				return types.Entry{}, false, err
			}
			return entry, !entry.Tombstone, nil
		}
	}
	tables := l.tables.SnapshotAndPin()
	l.memMu.RUnlock()
	defer func() {
		paths := make([]string, 0, len(tables))
		for _, table := range tables {
			paths = append(paths, table.Meta.Path)
		}
		l.cleanupTables(l.tables.Unpin(paths))
	}()
	for _, table := range tables {
		view, found, err := table.Handle.GetViewContext(ctx, key)
		if err != nil {
			return types.Entry{}, false, err
		}
		if found {
			entry := copyEntry(types.Entry{Key: view.Key, Value: view.Value, Seq: view.Seq, Tombstone: view.Tombstone})
			return entry, !entry.Tombstone, nil
		}
	}
	return types.Entry{}, false, ctx.Err()
}

// RWMutex has no cancellable acquisition. Try the uncontended path first, then
// retry on a bounded timer rather than leaking a goroutine blocked in RLock.
func readLockContext(ctx context.Context, mu *sync.RWMutex) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if mu.TryRLock() {
		if err := ctx.Err(); err != nil {
			mu.RUnlock()
			return err
		}
		return nil
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if mu.TryRLock() {
				if err := ctx.Err(); err != nil {
					mu.RUnlock()
					return err
				}
				return nil
			}
		}
	}
}

func (l *LSM) Get(key []byte) (types.Entry, bool) {
	if l == nil {
		return types.Entry{}, false
	}
	if l.reader == nil {
		l.reader = newReadService(l)
	}
	return l.reader.Get(key)
}
