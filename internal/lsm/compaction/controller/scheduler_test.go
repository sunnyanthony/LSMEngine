package controller

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"lsmengine/pkg/lsm/bus"
)

type triggerStub struct {
	count int32
}

func (t *triggerStub) Trigger() {
	atomic.AddInt32(&t.count, 1)
}

func TestSchedulerTriggersOnFlush(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	trigger := &triggerStub{}
	scheduler := NewScheduler(trigger, FlushTriggerPolicy{})
	events := make(chan bus.Event, 1)

	go scheduler.Run(ctx, events)
	events <- bus.Event{Type: bus.EventFlushCompleted}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&trigger.count) == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected trigger to be called")
}

func TestSchedulerIgnoresOtherEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	trigger := &triggerStub{}
	scheduler := NewScheduler(trigger, FlushTriggerPolicy{})
	events := make(chan bus.Event, 1)

	go scheduler.Run(ctx, events)
	events <- bus.Event{Type: bus.EventWalAppended}
	time.Sleep(20 * time.Millisecond)

	if atomic.LoadInt32(&trigger.count) != 0 {
		t.Fatalf("expected no trigger for non-flush event")
	}
}
