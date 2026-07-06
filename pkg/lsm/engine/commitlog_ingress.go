package engine

import (
	"context"
	"fmt"
)

// HandlePeerMessages routes inbound raft peer messages to the active
// commit-log provider implementation.
func (l *LSM) HandlePeerMessages(ctx context.Context, messages []RaftPeerMessage) error {
	if l == nil || l.commitLog == nil {
		return fmt.Errorf("commit log consensus unavailable")
	}
	if len(messages) == 0 {
		return nil
	}
	copied := cloneRaftPeerMessages(messages)
	return l.commitLog.HandlePeerMessages(ctx, copied)
}
