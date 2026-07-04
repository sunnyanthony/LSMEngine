package engine

import (
	"context"
	"fmt"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// HandlePeerMessages routes inbound raft peer messages to the active
// commit-log provider implementation.
func (l *LSM) HandlePeerMessages(ctx context.Context, messages []raftpb.Message) error {
	if l == nil || l.commitLog == nil {
		return fmt.Errorf("commit log consensus unavailable")
	}
	if len(messages) == 0 {
		return nil
	}
	copied := append([]raftpb.Message(nil), messages...)
	return l.commitLog.HandlePeerMessages(ctx, copied)
}
