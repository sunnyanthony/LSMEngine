package controller

import (
	"context"

	"lsmengine/pkg/lsm/bus"
)

// TriggerPolicy decides whether an event should trigger compaction.
type TriggerPolicy interface {
	ShouldTrigger(event bus.Event) bool
}

// FlushTriggerPolicy triggers on flush completion events.
type FlushTriggerPolicy struct{}

func (FlushTriggerPolicy) ShouldTrigger(event bus.Event) bool {
	return event.Type == bus.EventFlushCompleted
}

// Scheduler listens for events and triggers compaction via a Triggerer.
type Scheduler struct {
	Trigger Triggerer
	Policy  TriggerPolicy
}

// NewScheduler creates a scheduler bound to a trigger and policy.
func NewScheduler(trigger Triggerer, policy TriggerPolicy) *Scheduler {
	return &Scheduler{Trigger: trigger, Policy: policy}
}

// Run consumes events until ctx is canceled or the channel closes.
func (s *Scheduler) Run(ctx context.Context, events <-chan bus.Event) {
	if s == nil || s.Trigger == nil || s.Policy == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			if s.Policy.ShouldTrigger(event) {
				s.Trigger.Trigger()
			}
		}
	}
}
