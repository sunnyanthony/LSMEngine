package controller

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"lsmengine/internal/lsm/compaction"
)

type serviceControllerStub struct {
	sequence []bool
	err      error
	calls    int32
	callsCh  chan struct{}
}

type serviceStepFunc func(compaction.State) (bool, error)

func (f serviceStepFunc) Step(state compaction.State) (bool, error) { return f(state) }

func TestServiceCancellationStopsBetweenSteps(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var service *Service
	calls := 0
	service = NewService(serviceStepFunc(func(compaction.State) (bool, error) {
		calls++
		if !service.Stats().Running {
			t.Fatal("service should report running during a step")
		}
		cancel()
		return calls == 1, nil
	}), func() compaction.State { return compaction.State{} })
	service.Trigger()
	service.Run(ctx)
	stats := service.Stats()
	if calls != 1 || stats.Steps != 1 || stats.SuccessfulSteps != 1 || stats.Running || stats.Errors != 0 {
		t.Fatalf("canceled service did not stop after its current step: calls=%d stats=%+v", calls, stats)
	}
}

func (s *serviceControllerStub) Step(state compaction.State) (bool, error) {
	atomic.AddInt32(&s.calls, 1)
	if s.callsCh != nil {
		s.callsCh <- struct{}{}
	}
	if s.err != nil {
		return false, s.err
	}
	if len(s.sequence) == 0 {
		return false, nil
	}
	next := s.sequence[0]
	s.sequence = s.sequence[1:]
	return next, nil
}

func TestServiceRunsOnTrigger(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	callsCh := make(chan struct{}, 2)
	ctrl := &serviceControllerStub{
		sequence: []bool{true, false},
		callsCh:  callsCh,
	}
	service := NewService(ctrl, func() compaction.State { return compaction.State{} })

	done := make(chan struct{})
	go func() {
		defer close(done)
		service.Run(ctx)
	}()
	service.Trigger()

	for i := 0; i < 2; i++ {
		select {
		case <-callsCh:
		case <-time.After(2 * time.Second):
			t.Fatalf("expected controller step %d", i+1)
		}
	}
	if got := atomic.LoadInt32(&ctrl.calls); got != 2 {
		t.Fatalf("expected 2 steps, got %d", got)
	}
	cancel()
	<-done
	stats := service.Stats()
	if stats.Triggers != 1 || stats.Runs != 1 || stats.Steps != 2 || stats.SuccessfulSteps != 1 || stats.Errors != 0 {
		t.Fatalf("unexpected service stats: %+v", stats)
	}
}

func TestServiceStatsCountCoalescedTriggers(t *testing.T) {
	service := NewService(&serviceControllerStub{}, func() compaction.State { return compaction.State{} })
	service.Trigger()
	service.Trigger()

	stats := service.Stats()
	if stats.Triggers != 1 || stats.CoalescedTriggers != 1 {
		t.Fatalf("unexpected trigger stats: %+v", stats)
	}
}

func TestServiceOnErrorStopsLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan struct{}, 1)
	ctrl := &serviceControllerStub{
		err: errors.New("boom"),
	}
	service := NewService(ctrl, func() compaction.State { return compaction.State{} })
	service.OnError = func(err error) {
		if err != nil {
			errCh <- struct{}{}
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		service.Run(ctx)
	}()
	service.Trigger()

	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("expected error handler to fire")
	}
	if got := atomic.LoadInt32(&ctrl.calls); got != 1 {
		t.Fatalf("expected 1 step, got %d", got)
	}
	cancel()
	<-done
	stats := service.Stats()
	if stats.Triggers != 1 || stats.Runs != 1 || stats.Steps != 1 || stats.Errors != 1 {
		t.Fatalf("unexpected error stats: %+v", stats)
	}
}
