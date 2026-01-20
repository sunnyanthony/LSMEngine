//go:build test

package runtime

import "sync"

// TestHooks configures compaction apply callbacks for tests.
type TestHooks = applyHooks

var (
	hooksMu sync.RWMutex
	hooks   *applyHooks
)

// SetTestHooks registers hooks for the compaction runtime (tests only).
func SetTestHooks(h *TestHooks) {
	hooksMu.Lock()
	defer hooksMu.Unlock()
	hooks = h
}

func currentHooks() *applyHooks {
	hooksMu.RLock()
	defer hooksMu.RUnlock()
	return hooks
}
