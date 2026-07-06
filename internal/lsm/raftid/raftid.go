// Package raftid owns stable raft node id derivation.
package raftid

import "hash/fnv"

// StableNodeID returns the deterministic raft node id for a configured node name.
func StableNodeID(nodeID string) uint64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(nodeID))
	id := hasher.Sum64()
	if id == 0 {
		return 1
	}
	return id
}
