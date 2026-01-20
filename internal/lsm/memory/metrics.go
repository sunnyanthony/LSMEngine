// Memory and copy metrics counters.

package memory

import "sync/atomic"

// Metrics captures lightweight counters for copy and pool reuse.
type Metrics struct {
	EntryCopies uint64
	EntryBytes  uint64

	BufferGets uint64
	BufferHits uint64
	BufferMiss uint64
	BufferPuts uint64

	ReaderGets uint64
	ReaderHits uint64
	ReaderMiss uint64
	ReaderPuts uint64
}

var metricsEnabled atomic.Bool

var (
	metricEntryCopies atomic.Uint64
	metricEntryBytes  atomic.Uint64

	metricBufferGets atomic.Uint64
	metricBufferHits atomic.Uint64
	metricBufferMiss atomic.Uint64
	metricBufferPuts atomic.Uint64

	metricReaderGets atomic.Uint64
	metricReaderHits atomic.Uint64
	metricReaderMiss atomic.Uint64
	metricReaderPuts atomic.Uint64
)

// EnableMetrics turns on lightweight counters in the memory package.
func EnableMetrics() {
	metricsEnabled.Store(true)
}

// DisableMetrics turns off lightweight counters in the memory package.
func DisableMetrics() {
	metricsEnabled.Store(false)
}

// ResetMetrics clears all counters.
func ResetMetrics() {
	metricEntryCopies.Store(0)
	metricEntryBytes.Store(0)
	metricBufferGets.Store(0)
	metricBufferHits.Store(0)
	metricBufferMiss.Store(0)
	metricBufferPuts.Store(0)
	metricReaderGets.Store(0)
	metricReaderHits.Store(0)
	metricReaderMiss.Store(0)
	metricReaderPuts.Store(0)
}

// MetricsSnapshot returns a consistent snapshot of counters.
func MetricsSnapshot() Metrics {
	return Metrics{
		EntryCopies: metricEntryCopies.Load(),
		EntryBytes:  metricEntryBytes.Load(),
		BufferGets:  metricBufferGets.Load(),
		BufferHits:  metricBufferHits.Load(),
		BufferMiss:  metricBufferMiss.Load(),
		BufferPuts:  metricBufferPuts.Load(),
		ReaderGets:  metricReaderGets.Load(),
		ReaderHits:  metricReaderHits.Load(),
		ReaderMiss:  metricReaderMiss.Load(),
		ReaderPuts:  metricReaderPuts.Load(),
	}
}

func recordEntryCopy(n int) {
	if !metricsEnabled.Load() || n <= 0 {
		return
	}
	metricEntryCopies.Add(1)
	metricEntryBytes.Add(uint64(n))
}

func recordBufferGet(hit bool) {
	if !metricsEnabled.Load() {
		return
	}
	metricBufferGets.Add(1)
	if hit {
		metricBufferHits.Add(1)
		return
	}
	metricBufferMiss.Add(1)
}

func recordBufferPut() {
	if !metricsEnabled.Load() {
		return
	}
	metricBufferPuts.Add(1)
}

func recordReaderGet(hit bool) {
	if !metricsEnabled.Load() {
		return
	}
	metricReaderGets.Add(1)
	if hit {
		metricReaderHits.Add(1)
		return
	}
	metricReaderMiss.Add(1)
}

func recordReaderPut() {
	if !metricsEnabled.Load() {
		return
	}
	metricReaderPuts.Add(1)
}
