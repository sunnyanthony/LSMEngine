package engine

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// CommitLogProvider selects the commit-log backend.
type CommitLogProvider string

const (
	CommitLogProviderLocal    CommitLogProvider = "local"
	CommitLogProviderEtcdRaft CommitLogProvider = "etcd-raft"
)

// CommitLogOptions controls control-plane commit-log execution.
type CommitLogOptions struct {
	Provider CommitLogProvider `json:"provider" yaml:"provider"`
}

type controlMutation struct {
	Kind    string
	ShardID string
	Target  string
	Split   []byte
	NodeID  string
}

type controlCommit struct {
	Index uint64
	Term  uint64
}

type controlCommittedEntry struct {
	Commit   controlCommit
	Mutation controlMutation
}

type controlConsensus interface {
	CommitControl(ctx context.Context, mutation controlMutation) (controlCommittedEntry, error)
	Provider() CommitLogProvider
}

type localControlConsensus struct {
	mu    sync.Mutex
	index uint64
	term  uint64
}

func newLocalControlConsensus() *localControlConsensus {
	return &localControlConsensus{term: 1}
}

func (c *localControlConsensus) CommitControl(_ context.Context, mutation controlMutation) (controlCommittedEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.index++
	return controlCommittedEntry{
		Commit: controlCommit{
			Index: c.index,
			Term:  c.term,
		},
		Mutation: cloneControlMutation(mutation),
	}, nil
}

func (c *localControlConsensus) Provider() CommitLogProvider {
	return CommitLogProviderLocal
}

type etcdRaftControlConsensus struct{}

func (c *etcdRaftControlConsensus) CommitControl(_ context.Context, _ controlMutation) (controlCommittedEntry, error) {
	return controlCommittedEntry{}, fmt.Errorf("commit log provider %q not wired yet", CommitLogProviderEtcdRaft)
}

func (c *etcdRaftControlConsensus) Provider() CommitLogProvider {
	return CommitLogProviderEtcdRaft
}

func newControlConsensus(opts Options) (controlConsensus, error) {
	provider := CommitLogProviderLocal
	if opts.CommitLog != nil {
		if trimmed := strings.TrimSpace(string(opts.CommitLog.Provider)); trimmed != "" {
			provider = CommitLogProvider(trimmed)
		}
	}
	switch provider {
	case CommitLogProviderLocal:
		return newLocalControlConsensus(), nil
	case CommitLogProviderEtcdRaft:
		// Stage-1 skeleton: provider is selectable, wiring is deferred.
		return &etcdRaftControlConsensus{}, nil
	default:
		return nil, fmt.Errorf("unknown commit log provider %q", provider)
	}
}

func cloneControlMutation(mutation controlMutation) controlMutation {
	mutation.Split = append([]byte(nil), mutation.Split...)
	return mutation
}
