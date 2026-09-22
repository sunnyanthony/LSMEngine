package engine

import (
	"context"
	"fmt"
	"lsmengine/pkg/lsm/errs"
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
	l.peerMu.Lock()
	if l.isClosing() {
		l.peerMu.Unlock()
		return errs.ErrClosed
	}
	l.peerWG.Add(1)
	l.peerMu.Unlock()
	defer l.peerWG.Done()
	err := l.commitLog.HandlePeerMessages(ctx, copyCommitLogPeerMessages(messages))
	if l.committedApply != nil {
		if applyErr := l.committedApply.drain(0); applyErr != nil {
			return applyErr
		}
	}
	return err
}
