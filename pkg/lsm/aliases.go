// Public aliases for engine types and options.

package lsm

import "lsmengine/pkg/lsm/engine"

type LSM = engine.LSM
type Options = engine.Options
type SSTableOptions = engine.SSTableOptions
type MissingSegmentPolicy = engine.MissingSegmentPolicy
type Iterator = engine.Iterator
type Snapshot = engine.Snapshot
type Stats = engine.Stats
type Health = engine.Health

const (
	SSTableCompressionNone   = engine.SSTableCompressionNone
	SSTableCompressionSnappy = engine.SSTableCompressionSnappy
	SSTableChecksumCRC32C    = engine.SSTableChecksumCRC32C

	SSTableCorruptionFailFast  = engine.SSTableCorruptionFailFast
	SSTableCorruptionSkipBlock = engine.SSTableCorruptionSkipBlock
	SSTableCorruptionDropTable = engine.SSTableCorruptionDropTable

	MemtableKindMap             = engine.MemtableKindMap
	MemtableKindSkipList        = engine.MemtableKindSkipList
	MemtableKindShardedSkipList = engine.MemtableKindShardedSkipList
)

const (
	MissingSegmentError  = engine.MissingSegmentError
	MissingSegmentIgnore = engine.MissingSegmentIgnore
)

func New(opts Options) (*LSM, error) {
	return engine.New(opts)
}
