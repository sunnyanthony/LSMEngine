//go:build test

package integration_test

import (
	"testing"
	"time"

	compactionruntime "lsmengine/internal/lsm/compaction/runtime"
	"lsmengine/internal/lsm/compaction"
)

type compactionWaiter struct {
	ch chan compaction.Result
}

func startCompactionWait(t *testing.T) *compactionWaiter {
	t.Helper()

	waiter := &compactionWaiter{ch: make(chan compaction.Result, 1)}
	compactionruntime.SetTestHooks(&compactionruntime.TestHooks{
		AfterApply: func(result compaction.Result, err error) {
			if err != nil {
				return
			}
			select {
			case waiter.ch <- result:
			default:
			}
		},
	})
	return waiter
}

func (w *compactionWaiter) Wait(t *testing.T) compaction.Result {
	t.Helper()
	defer compactionruntime.SetTestHooks(nil)

	select {
	case result := <-w.ch:
		return result
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for compaction apply")
		return compaction.Result{}
	}
}
