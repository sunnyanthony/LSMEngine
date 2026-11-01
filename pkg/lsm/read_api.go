package lsm

import (
	"context"
	"lsmengine/pkg/lsm/types"
)

// ReadProvider exposes error-aware reads of a node's locally applied state.
// Reads do not establish a quorum barrier or linearizability.
type ReadProvider interface {
	GetContext(context.Context, []byte) (types.Entry, bool, error)
}
