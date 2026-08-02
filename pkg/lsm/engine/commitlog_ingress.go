package engine

import (
	"context"
	"fmt"
)

// HandlePeerMessages routes inbound commit-log peer messages to the active
// commit-log provider implementation.
func (l *LSM) HandlePeerMessages(ctx context.Context, messages []CommitLogPeerMessage) error {
	if l == nil || l.commitLog == nil {
		return fmt.Errorf("commit log consensus unavailable")
	}
	if len(messages) == 0 {
		return nil
	}
	return l.commitLog.HandlePeerMessages(ctx, copyCommitLogPeerMessages(messages))
}
