package lsm

// ControlProvider exposes control-plane management APIs.
type ControlProvider interface {
	ClusterStatus() ClusterStatus
	Shards() []ShardStatus
	TransferLeader(shardID, target string) error
	AddReplica(shardID, target string) error
	RemoveReplica(shardID, target string) error
	TriggerSplit(shardID string, splitKey []byte) error
	TriggerRebalance(shardID, target string) error
	PrepareDrain(nodeID string) error
}

// ControlProviderWithOptions extends ControlProvider with optimistic-concurrency APIs.
type ControlProviderWithOptions interface {
	ControlProvider
	TransferLeaderWithOptions(shardID, target string, opts ControlWriteOptions) error
	AddReplicaWithOptions(shardID, target string, opts ControlWriteOptions) error
	RemoveReplicaWithOptions(shardID, target string, opts ControlWriteOptions) error
	TriggerSplitWithOptions(shardID string, splitKey []byte, opts ControlWriteOptions) error
	TriggerRebalanceWithOptions(shardID, target string, opts ControlWriteOptions) error
	PrepareDrainWithOptions(nodeID string, opts ControlWriteOptions) error
}
