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
	finish, err := l.beginCommitLogOperation()
	if err != nil {
		return err
	}
	defer finish()
	err = l.commitLog.HandlePeerMessages(ctx, copyCommitLogPeerMessages(messages))
	if l.committedApply != nil {
		if applyErr := l.committedApply.drain(0); applyErr != nil {
			return applyErr
		}
	}
	return err
}

// Admission covers both local proposals and inbound processing until their
// provider and application work completes. Close closes this gate before Wait.
func (l *LSM) beginCommitLogOperation() (func(), error) {
	l.peerMu.Lock()
	defer l.peerMu.Unlock()
	if l.peerClosing || l.isClosing() {
		return nil, errs.ErrClosed
	}
	l.peerWG.Add(1)
	return l.peerWG.Done, nil
}
