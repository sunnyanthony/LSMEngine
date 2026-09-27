package engine

import (
	"context"
	"errors"
	"time"

	internalcommitlog "lsmengine/internal/lsm/commitlog"
)

const commitLogTickInterval = 100 * time.Millisecond

func (l *LSM) startCommitLogTicks() {
	builtin, ok := l.commitLog.(*builtinCommitLogConsensus)
	if !ok {
		return
	}
	if builtin.RuntimeStatus().Replicas <= 1 {
		return
	}
	ticker, ok := builtin.inner.(internalcommitlog.Ticker)
	if !ok {
		return
	}
	ctx, cancel := context.WithCancel(l.ctx)
	l.tickCancel = cancel
	l.bg.Add(1)
	go func() {
		defer l.bg.Done()
		clock := time.NewTicker(commitLogTickInterval)
		defer clock.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-clock.C:
				if err := l.tickCommitLog(ctx, ticker); err != nil && !errors.Is(err, context.Canceled) && l.logger != nil {
					l.logger.Printf("commit log tick: %v", err)
				}
			}
		}
	}()
}

func (l *LSM) tickCommitLog(ctx context.Context, ticker internalcommitlog.Ticker) error {
	finish, err := l.beginCommitLogOperation()
	if err != nil {
		return err
	}
	defer finish()
	err = ticker.Tick(ctx)
	// A tick can make committed entries available even if peer delivery fails.
	if l.committedApply != nil {
		if applyErr := l.committedApply.drain(0); applyErr != nil {
			return applyErr
		}
	}
	return err
}
