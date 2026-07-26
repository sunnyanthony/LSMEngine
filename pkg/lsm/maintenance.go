package lsm

// CompactionProvider exposes node-local compaction maintenance hooks.
type CompactionProvider interface {
	TriggerCompaction() error
}
