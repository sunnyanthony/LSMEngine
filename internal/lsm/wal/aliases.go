package wal

import "lsmengine/internal/lsm/wal/core"

type WAL = core.WAL
type Options = core.Options

func NewWAL(opts Options) (*WAL, error) {
	return core.NewWAL(opts)
}

func OpenReplay(path string, repairOnReplay bool) *WAL {
	return core.OpenReplay(path, repairOnReplay)
}
