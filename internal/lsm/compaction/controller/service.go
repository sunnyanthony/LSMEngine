// Compaction service loop with trigger coalescing.

package controller

import (
	"context"

	"lsmengine/internal/lsm/compaction"
)

// Triggerer can request a compaction run.
type Triggerer interface {
	Trigger()
}

// StateSource builds a compaction state snapshot for the controller.
type StateSource func() compaction.State

// Service runs compaction steps in response to triggers.
type Service struct {
	Controller Controller
	Source     StateSource
	OnError    func(error)

	trigger chan struct{}
}

// NewService wires a controller with a state source and trigger channel.
func NewService(controller Controller, source StateSource) *Service {
	return &Service{
		Controller: controller,
		Source:     source,
		trigger:    make(chan struct{}, 1),
	}
}

// Trigger schedules a compaction run (coalesced if already pending).
func (s *Service) Trigger() {
	if s == nil || s.trigger == nil {
		return
	}
	select {
	case s.trigger <- struct{}{}:
	default:
	}
}

// Run blocks, executing compaction steps until ctx is canceled.
func (s *Service) Run(ctx context.Context) {
	if s == nil || s.Controller == nil || s.Source == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.trigger:
			for {
				ran, err := s.Controller.Step(s.Source())
				if err != nil {
					if s.OnError != nil {
						s.OnError(err)
					}
					break
				}
				if !ran {
					break
				}
			}
		}
	}
}
