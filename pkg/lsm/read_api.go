package lsm

import "lsmengine/pkg/lsm/types"

// ReadProvider exposes point reads to external transports.
type ReadProvider interface {
	Get(key []byte) (types.Entry, bool)
}

// RangeProvider exposes bounded snapshot range scans to external transports.
type RangeProvider interface {
	Snapshot() *Snapshot
}
