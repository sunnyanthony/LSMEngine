package engine

import "lsmengine/pkg/lsm/types"

func copyEntry(entry types.Entry) types.Entry {
	entry.Key = append([]byte(nil), entry.Key...)
	entry.Value = append([]byte(nil), entry.Value...)
	return entry
}
