package controller

import (
	"testing"

	"lsmengine/internal/lsm/compaction/data"
	"lsmengine/internal/lsm/compaction/model"
	"lsmengine/internal/lsm/metadata"
	"lsmengine/internal/lsm/sstable"
)

type plannerStub struct {
	plan   model.Plan
	ok     bool
	err    error
	called int
}

func (p *plannerStub) Next(state model.State) (model.Plan, bool, error) {
	p.called++
	return p.plan, p.ok, p.err
}

type runnerStub struct {
	result data.Result
	err    error
	called int
}

func (r *runnerStub) Run(plan model.Plan, inputs []sstable.SSTable) (data.Result, error) {
	r.called++
	return r.result, r.err
}

type applierStub struct {
	err    error
	called int
}

func (a *applierStub) Apply(result data.Result) error {
	a.called++
	return a.err
}

type resolverStub struct {
	handles []sstable.SSTable
	err     error
	called  int
}

func (r *resolverStub) Resolve(_ []metadata.TableMeta) ([]sstable.SSTable, error) {
	r.called++
	return r.handles, r.err
}

func TestCoordinatorStepMissingDependencies(t *testing.T) {
	var nilController *Coordinator
	ok, err := nilController.Step(model.State{})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false for nil controller")
	}

	controller := &Coordinator{}
	ok, err = controller.Step(model.State{})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false for missing deps")
	}
}

func TestCoordinatorStepNoPlan(t *testing.T) {
	planner := &plannerStub{ok: false}
	controller := &Coordinator{
		Planner: planner,
		Runner:  &runnerStub{},
		Applier: &applierStub{},
		Resolve: (&resolverStub{}).Resolve,
	}
	ok, err := controller.Step(model.State{})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false")
	}
}

func TestCoordinatorStepSuccessInitializesMetrics(t *testing.T) {
	planner := &plannerStub{ok: true}
	runner := &runnerStub{}
	applier := &applierStub{}
	controller := &Coordinator{
		Planner: planner,
		Runner:  runner,
		Applier: applier,
		Resolve: (&resolverStub{}).Resolve,
	}
	ok, err := controller.Step(model.State{})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if controller.Metrics == nil {
		t.Fatalf("expected metrics initialized")
	}
}

func TestCoordinatorMetricsSnapshotNil(t *testing.T) {
	controller := &Coordinator{}
	snap := controller.MetricsSnapshot()
	if snap.CacheHit != 0 || snap.CacheMiss != 0 || snap.Errors != 0 {
		t.Fatalf("expected zero snapshot, got %+v", snap)
	}
}
