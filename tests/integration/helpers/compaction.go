//go:build test

package helpers

import (
	"testing"
	"time"

	"lsmengine/internal/lsm/compaction"
	compactionruntime "lsmengine/internal/lsm/compaction/runtime"
)

type CompactionWaiter struct {
	ch chan compaction.Result
}

func StartCompactionWait(t *testing.T) *CompactionWaiter {
	t.Helper()

	waiter := &CompactionWaiter{ch: make(chan compaction.Result, 1)}
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
	t.Cleanup(func() {
		compactionruntime.SetTestHooks(nil)
	})
	return waiter
}

func (w *CompactionWaiter) Wait(t *testing.T) compaction.Result {
	t.Helper()

	select {
	case result := <-w.ch:
		return result
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for compaction apply")
		return compaction.Result{}
	}
}
