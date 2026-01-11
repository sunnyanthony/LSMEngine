package flow

import (
	"context"

	"lsmengine/internal/lsm/sstable/block"
	"lsmengine/internal/lsm/sstable/config"
	"lsmengine/internal/lsm/sstable/index"
)

type FlowObserver = config.FlowObserver
type FlowEvent = config.FlowEvent

// FlowItem is the message passed through the read DAG/state machine.
type FlowItem struct {
	Key   []byte
	Index index.Entry
	Block *block.Block
	Entry block.EntryView
	Found bool
	Done  bool
	Err   error
}

type NodeResult struct {
	Next Node
	Done bool
	Err  error
}

// Node processes a FlowItem and returns the next step.
type Node interface {
	Process(ctx context.Context, item *FlowItem) NodeResult
}

type nopObserver struct{}

func (nopObserver) OnNode(event FlowEvent, node string)             {}
func (nopObserver) OnError(event FlowEvent, node string, err error) {}
