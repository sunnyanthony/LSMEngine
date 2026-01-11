package config

import "sync/atomic"

// FlowEvent is emitted to observers; it is a copy of the pipeline state.
type FlowEvent struct {
	Key      []byte
	Node     string
	CacheHit bool
	Mmapped  bool
	Err      error
}

// FlowObserver receives events from the pipeline; observers should be fast.
type FlowObserver interface {
	OnNode(event FlowEvent, node string)
	OnError(event FlowEvent, node string, err error)
}

// FlowMetrics aggregates lightweight counters from FlowEvents.
type FlowMetrics struct {
	cacheHit   atomic.Uint64
	cacheMiss  atomic.Uint64
	filterPass atomic.Uint64
	filterSkip atomic.Uint64
	errors     atomic.Uint64
}

func (m *FlowMetrics) Record(event FlowEvent, isFilter bool) {
	if event.Err != nil {
		m.errors.Add(1)
		return
	}
	if isFilter {
		m.filterPass.Add(1)
		return
	}
	if event.CacheHit {
		m.cacheHit.Add(1)
	} else {
		m.cacheMiss.Add(1)
	}
}

type MetricsSnapshot struct {
	CacheHit   uint64
	CacheMiss  uint64
	FilterPass uint64
	FilterSkip uint64
	Errors     uint64
}

func (m *FlowMetrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		CacheHit:   m.cacheHit.Load(),
		CacheMiss:  m.cacheMiss.Load(),
		FilterPass: m.filterPass.Load(),
		FilterSkip: m.filterSkip.Load(),
		Errors:     m.errors.Load(),
	}
}

// MetricsObserver is a FlowObserver that accumulates metrics.
type MetricsObserver struct {
	metrics *FlowMetrics
}

func NewMetricsObserver(target *FlowMetrics) *MetricsObserver {
	if target == nil {
		target = &FlowMetrics{}
	}
	return &MetricsObserver{metrics: target}
}

func (o *MetricsObserver) OnNode(event FlowEvent, node string) {
	if o == nil || o.metrics == nil {
		return
	}
	o.metrics.Record(event, node == "filter")
}

func (o *MetricsObserver) OnError(event FlowEvent, node string, err error) {
	if o == nil || o.metrics == nil {
		return
	}
	o.metrics.Record(FlowEvent{Err: err}, false)
}
