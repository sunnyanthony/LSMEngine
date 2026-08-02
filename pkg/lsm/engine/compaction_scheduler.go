package engine

import (
	"context"
	"time"
)

func (l *LSM) runCompactionCheckLoop(ctx context.Context) {
	if l == nil || l.compactionSvc == nil || l.compactionCheckInterval <= 0 {
		return
	}
	timer := time.NewTimer(l.currentCompactionCheckInterval())
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			l.compactionSvc.Trigger()
			timer.Reset(l.currentCompactionCheckInterval())
		}
	}
}

func (l *LSM) currentCompactionCheckInterval() time.Duration {
	if l == nil {
		return 0
	}
	return compactionAdaptiveCheckDelay(
		l.compactionCheckInterval,
		l.compactionAdaptiveCheck,
		l.currentL0TableCount(),
		l.compactionL0Threshold,
	)
}

func (l *LSM) currentL0TableCount() int {
	if l == nil || l.tables == nil {
		return 0
	}
	count := 0
	for _, meta := range l.tables.Snapshot() {
		if meta.Level == 0 {
			count++
		}
	}
	return count
}

func compactionAdaptiveCheckDelay(base time.Duration, adaptive bool, l0Tables int, threshold int) time.Duration {
	if base <= 0 {
		return 0
	}
	if !adaptive || threshold <= 0 || l0Tables < threshold {
		return base
	}
	divisor := int64(4)
	if l0Tables >= threshold*2 {
		divisor = 8
	}
	next := base / time.Duration(divisor)
	minimum := time.Millisecond
	if base < minimum {
		minimum = base
	}
	if next < minimum {
		return minimum
	}
	return next
}
