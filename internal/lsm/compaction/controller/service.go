// Compaction service loop with trigger coalescing.

package controller

import (
	"context"
	"sync/atomic"

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
	stats   serviceStats
}

// Stats describes cumulative compaction service activity.
type Stats struct {
	Triggers          uint64
	CoalescedTriggers uint64
	Runs              uint64
	Steps             uint64
	SuccessfulSteps   uint64
	Errors            uint64
	Running           bool
}

type serviceStats struct {
	triggers          atomic.Uint64
	coalescedTriggers atomic.Uint64
	runs              atomic.Uint64
	steps             atomic.Uint64
	successfulSteps   atomic.Uint64
	errors            atomic.Uint64
	running           atomic.Bool
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
		s.stats.triggers.Add(1)
	default:
		s.stats.coalescedTriggers.Add(1)
	}
}

// Stats returns cumulative activity counters for this service.
func (s *Service) Stats() Stats {
	if s == nil {
		return Stats{}
	}
	return Stats{
		Triggers:          s.stats.triggers.Load(),
		CoalescedTriggers: s.stats.coalescedTriggers.Load(),
		Runs:              s.stats.runs.Load(),
		Steps:             s.stats.steps.Load(),
		SuccessfulSteps:   s.stats.successfulSteps.Load(),
		Errors:            s.stats.errors.Load(),
		Running:           s.stats.running.Load(),
	}
}

// Run blocks, executing compaction steps until ctx is canceled.
func (s *Service) Run(ctx context.Context) {
	if s == nil || s.Controller == nil || s.Source == nil {
		return
	}
	defer s.stats.running.Store(false)
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.trigger:
			if ctx.Err() != nil {
				return
			}
			s.stats.runs.Add(1)
			s.stats.running.Store(true)
			for {
				if ctx.Err() != nil {
					return
				}
				ran, err := s.Controller.Step(s.Source())
				s.stats.steps.Add(1)
				if err != nil {
					s.stats.errors.Add(1)
					if s.OnError != nil {
						s.OnError(err)
					}
					break
				}
				if !ran {
					break
				}
				s.stats.successfulSteps.Add(1)
			}
			s.stats.running.Store(false)
		}
	}
}
