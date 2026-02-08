package lsm

// ControlProvider exposes control-plane management APIs.
type ControlProvider interface {
	ClusterStatus() ClusterStatus
	Shards() []ShardStatus
	TransferLeader(shardID, target string) error
	TriggerSplit(shardID string, splitKey []byte) error
	TriggerRebalance(shardID, target string) error
	PrepareDrain(nodeID string) error
}
