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

	go service.Run(ctx)
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

	go service.Run(ctx)
	service.Trigger()

	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("expected error handler to fire")
	}
	if got := atomic.LoadInt32(&ctrl.calls); got != 1 {
		t.Fatalf("expected 1 step, got %d", got)
	}
}
