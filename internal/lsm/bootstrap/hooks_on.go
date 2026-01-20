//go:build test

package bootstrap

import "sync"

// TestHooks configures recovery hooks (tests only).
type TestHooks = recoveryHooks

var (
	hooksMu sync.RWMutex
	hooks   *recoveryHooks
)

// SetTestHooks registers hooks for bootstrap recovery (tests only).
func SetTestHooks(h *TestHooks) {
	hooksMu.Lock()
	defer hooksMu.Unlock()
	hooks = h
}

func currentHooks() *recoveryHooks {
	hooksMu.RLock()
	defer hooksMu.RUnlock()
	return hooks
}
