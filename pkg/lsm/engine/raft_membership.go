package engine

import (
	"context"
	"fmt"
	"strings"
)

// AddRaftPeer proposes a provider-owned raft voter add for nodeID.
//
// This updates the commit-log provider membership only. Shard replica metadata
// is still managed separately through AddReplica.
func (l *LSM) AddRaftPeer(nodeID string) error {
	return l.changeRaftPeer(CommitLogMembershipChange{
		Type:   CommitLogMembershipChangeAddNode,
		NodeID: nodeID,
	})
}

// RemoveRaftPeer proposes a provider-owned raft voter removal for nodeID.
//
// This updates the commit-log provider membership only. Shard replica metadata
// is still managed separately through RemoveReplica.
func (l *LSM) RemoveRaftPeer(nodeID string) error {
	return l.changeRaftPeer(CommitLogMembershipChange{
		Type:   CommitLogMembershipChangeRemoveNode,
		NodeID: nodeID,
	})
}

func (l *LSM) changeRaftPeer(change CommitLogMembershipChange) error {
	if l == nil || l.commitLog == nil {
		return fmt.Errorf("commit log consensus unavailable")
	}
	change.NodeID = strings.TrimSpace(change.NodeID)
	if change.NodeID == "" {
		return fmt.Errorf("raft peer node id is required")
	}
	changer, ok := l.commitLog.(commitLogMembershipChanger)
	if !ok {
		return fmt.Errorf("commit log provider %q does not support raft membership changes", l.commitLog.Provider())
	}
	return changer.ChangeMembership(context.Background(), change)
}
