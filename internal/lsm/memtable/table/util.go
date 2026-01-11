package table

import "lsmengine/pkg/lsm/types"

func entrySize(entry types.Entry) int {
	return len(entry.Key) + len(entry.Value)
}
