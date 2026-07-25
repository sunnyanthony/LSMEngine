package engine

import "sync/atomic"

type pointReadMetrics struct {
	count            atomic.Uint64
	memtableHits     atomic.Uint64
	immutableHits    atomic.Uint64
	sstableHits      atomic.Uint64
	misses           atomic.Uint64
	sstableProbes    atomic.Uint64
	maxSSTableProbes atomic.Uint64
}

type pointReadMetricsSnapshot struct {
	count            uint64
	memtableHits     uint64
	immutableHits    uint64
	sstableHits      uint64
	misses           uint64
	sstableProbes    uint64
	maxSSTableProbes uint64
}

type pointReadSource int

const (
	pointReadMemtable pointReadSource = iota
	pointReadImmutable
	pointReadSSTable
	pointReadMiss
)

func (m *pointReadMetrics) record(source pointReadSource, sstableProbes uint64) {
	if m == nil {
		return
	}
	m.count.Add(1)
	m.sstableProbes.Add(sstableProbes)
	m.recordMaxSSTableProbes(sstableProbes)
	switch source {
	case pointReadMemtable:
		m.memtableHits.Add(1)
	case pointReadImmutable:
		m.immutableHits.Add(1)
	case pointReadSSTable:
		m.sstableHits.Add(1)
	case pointReadMiss:
		m.misses.Add(1)
	}
}

func (m *pointReadMetrics) snapshot() pointReadMetricsSnapshot {
	if m == nil {
		return pointReadMetricsSnapshot{}
	}
	return pointReadMetricsSnapshot{
		count:            m.count.Load(),
		memtableHits:     m.memtableHits.Load(),
		immutableHits:    m.immutableHits.Load(),
		sstableHits:      m.sstableHits.Load(),
		misses:           m.misses.Load(),
		sstableProbes:    m.sstableProbes.Load(),
		maxSSTableProbes: m.maxSSTableProbes.Load(),
	}
}

func (m *pointReadMetrics) recordMaxSSTableProbes(probes uint64) {
	for {
		current := m.maxSSTableProbes.Load()
		if probes <= current {
			return
		}
		if m.maxSSTableProbes.CompareAndSwap(current, probes) {
			return
		}
	}
}
