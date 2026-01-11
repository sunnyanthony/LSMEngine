package compaction

import (
	"lsmengine/internal/lsm/compaction/controller"
	"lsmengine/internal/lsm/compaction/data"
	"lsmengine/internal/lsm/compaction/model"
	"lsmengine/internal/lsm/compaction/runner"
	"lsmengine/internal/lsm/compaction/strategy"
)

type State = model.State
type Level = model.Level
type Plan = model.Plan
type Planner = model.Planner

type Result = data.Result
type Runner = data.Runner
type Applier = data.Applier
type Engine = data.Engine
type Resolver = data.Resolver

type StrictLevelledPlanner = strategy.StrictLevelledPlanner
type SimpleRunner = runner.SimpleRunner
type Controller = controller.Controller
type Coordinator = controller.Coordinator
type Triggerer = controller.Triggerer
type StateSource = controller.StateSource
type Service = controller.Service
type Scheduler = controller.Scheduler
type TriggerPolicy = controller.TriggerPolicy
type FlushTriggerPolicy = controller.FlushTriggerPolicy

func NewService(ctrl Controller, source StateSource) *Service {
	return controller.NewService(ctrl, source)
}

func NewScheduler(trigger Triggerer, policy TriggerPolicy) *Scheduler {
	return controller.NewScheduler(trigger, policy)
}
