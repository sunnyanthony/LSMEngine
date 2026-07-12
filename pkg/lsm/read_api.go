package lsm

import "lsmengine/pkg/lsm/types"

// ReadProvider exposes point reads to external transports.
type ReadProvider interface {
	Get(key []byte) (types.Entry, bool)
}
