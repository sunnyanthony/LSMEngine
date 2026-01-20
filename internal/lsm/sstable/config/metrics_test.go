package config

import "testing"

func TestFlowMetricsRecord(t *testing.T) {
	m := &FlowMetrics{}
	m.Record(FlowEvent{CacheHit: true}, false)
	m.Record(FlowEvent{CacheHit: false}, false)
	m.Record(FlowEvent{}, true)
	m.Record(FlowEvent{Err: errSentinel}, false)
	snap := m.Snapshot()
	if snap.CacheHit != 1 || snap.CacheMiss != 1 || snap.FilterPass != 1 || snap.Errors != 1 {
		t.Fatalf("unexpected metrics snapshot: %+v", snap)
	}
}

func TestMetricsObserver(t *testing.T) {
	m := &FlowMetrics{}
	obs := NewMetricsObserver(m)
	obs.OnNode(FlowEvent{CacheHit: true}, "data")
	obs.OnNode(FlowEvent{}, "filter")
	obs.OnError(FlowEvent{}, "data", errSentinel)
	snap := m.Snapshot()
	if snap.CacheHit != 1 || snap.FilterPass != 1 || snap.Errors != 1 {
		t.Fatalf("unexpected metrics snapshot: %+v", snap)
	}
}

var errSentinel = &stubError{"boom"}

type stubError struct {
	msg string
}

func (e *stubError) Error() string { return e.msg }
