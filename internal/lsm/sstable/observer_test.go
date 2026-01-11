package sstable

import (
	"sync"
	"testing"

	"lsmengine/pkg/lsm/types"
)

type countingObserver struct {
	mu     sync.Mutex
	nodes  map[string]int
	errors int
}

func (o *countingObserver) OnNode(_ FlowEvent, node string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.nodes == nil {
		o.nodes = make(map[string]int)
	}
	o.nodes[node]++
}

func (o *countingObserver) OnError(_ FlowEvent, _ string, _ error) {
	o.mu.Lock()
	o.errors++
	o.mu.Unlock()
}

func TestFlowObserverReceivesEvents(t *testing.T) {
	dir := t.TempDir()
	obs := &countingObserver{}
	opts := DefaultOptions(dir)
	opts.FlowObserver = obs

	writer, err := NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	table, err := writer.Flush([]types.Entry{
		{Key: []byte("k1"), Value: []byte("v1")},
	})
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	defer table.Close()

	if _, ok := table.Get([]byte("k1")); !ok {
		t.Fatalf("expected key present")
	}

	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.nodes) == 0 {
		t.Fatalf("expected observer to receive events")
	}
	if obs.errors != 0 {
		t.Fatalf("expected no errors, got %d", obs.errors)
	}
}
