package wal

import "lsmengine/internal/lsm/wal/segment"

func listSegments(path string) ([]string, bool, error) {
	return segment.ListSegments(path)
}

func nextSegmentID(path string) uint64 {
	return segment.NextSegmentID(path)
}
