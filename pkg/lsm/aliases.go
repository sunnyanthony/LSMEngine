// Public aliases for engine types and options.

package lsm

import "lsmengine/pkg/lsm/engine"

type LSM = engine.LSM
type Options = engine.Options
type SSTableOptions = engine.SSTableOptions
type MissingSegmentPolicy = engine.MissingSegmentPolicy
type StorageMode = engine.StorageMode
type CommitLogProvider = engine.CommitLogProvider
type CommitLogOptions = engine.CommitLogOptions
type CommitLogConsensus = engine.CommitLogConsensus
type CommitLogFactory = engine.CommitLogFactory
type CommitLogControlMutation = engine.CommitLogControlMutation
type CommitLogDataMutation = engine.CommitLogDataMutation
type CommitLogCommit = engine.CommitLogCommit
type CommitLogControlCommittedEntry = engine.CommitLogControlCommittedEntry
type CommitLogDataCommittedEntry = engine.CommitLogDataCommittedEntry
type CommitLogRuntimeStatus = engine.CommitLogRuntimeStatus
type RaftPeerMessage = engine.RaftPeerMessage
type RaftMessageTransport = engine.RaftMessageTransport
type RaftOptions = engine.RaftOptions
type ShardConfig = engine.ShardConfig
type ReplicaStatus = engine.ReplicaStatus
type ShardStatus = engine.ShardStatus
type ClusterStatus = engine.ClusterStatus
type CDCEvent = engine.CDCEvent
type CDCReadResult = engine.CDCReadResult
type ControlWriteOptions = engine.ControlWriteOptions
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

const (
	StorageModeLocal = engine.StorageModeLocal
	StorageModePVC   = engine.StorageModePVC
)

const (
	CommitLogProviderLocal    = engine.CommitLogProviderLocal
	CommitLogProviderEtcdRaft = engine.CommitLogProviderEtcdRaft
)

func New(opts Options) (*LSM, error) {
	return engine.New(opts)
}

func RaftPeerID(nodeID string) uint64 {
	return engine.RaftPeerID(nodeID)
}
