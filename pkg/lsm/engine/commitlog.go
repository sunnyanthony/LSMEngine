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

// CommitLogOptions controls commit-log execution.
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

type dataMutation struct {
	Kind  string
	Key   []byte
	Value []byte
}

type commitResult struct {
	Index uint64
	Term  uint64
}

type controlCommittedEntry struct {
	Commit   commitResult
	Mutation controlMutation
}

type dataCommittedEntry struct {
	Commit   commitResult
	Mutation dataMutation
	Seq      uint64
}

type commitLogConsensus interface {
	CommitControl(ctx context.Context, mutation controlMutation) (controlCommittedEntry, error)
	CommitData(ctx context.Context, mutation dataMutation) (dataCommittedEntry, error)
	Provider() CommitLogProvider
}

type commitLogIndexObserver interface {
	ObserveCommittedIndex(index uint64)
}

type localCommitLogConsensus struct {
	mu    sync.Mutex
	index uint64
	term  uint64
}

func newLocalCommitLogConsensus() *localCommitLogConsensus {
	return &localCommitLogConsensus{term: 1}
}

func (c *localCommitLogConsensus) CommitControl(_ context.Context, mutation controlMutation) (controlCommittedEntry, error) {
	commit := c.nextCommit()
	return controlCommittedEntry{
		Commit:   commit,
		Mutation: cloneControlMutation(mutation),
	}, nil
}

func (c *localCommitLogConsensus) CommitData(_ context.Context, mutation dataMutation) (dataCommittedEntry, error) {
	commit := c.nextCommit()
	return dataCommittedEntry{
		Commit:   commit,
		Mutation: cloneDataMutation(mutation),
		Seq:      commit.Index,
	}, nil
}

func (c *localCommitLogConsensus) nextCommit() commitResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.index++
	return commitResult{
		Index: c.index,
		Term:  c.term,
	}
}

func (c *localCommitLogConsensus) ObserveCommittedIndex(index uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if index > c.index {
		c.index = index
	}
}

func (c *localCommitLogConsensus) Provider() CommitLogProvider {
	return CommitLogProviderLocal
}

type etcdRaftCommitLogConsensus struct{}

func (c *etcdRaftCommitLogConsensus) CommitControl(_ context.Context, _ controlMutation) (controlCommittedEntry, error) {
	return controlCommittedEntry{}, fmt.Errorf("commit log provider %q not wired yet", CommitLogProviderEtcdRaft)
}

func (c *etcdRaftCommitLogConsensus) CommitData(_ context.Context, _ dataMutation) (dataCommittedEntry, error) {
	return dataCommittedEntry{}, fmt.Errorf("commit log provider %q not wired yet", CommitLogProviderEtcdRaft)
}

func (c *etcdRaftCommitLogConsensus) Provider() CommitLogProvider {
	return CommitLogProviderEtcdRaft
}

func newCommitLogConsensus(opts Options) (commitLogConsensus, error) {
	provider := CommitLogProviderLocal
	if opts.CommitLog != nil {
		if trimmed := strings.TrimSpace(string(opts.CommitLog.Provider)); trimmed != "" {
			provider = CommitLogProvider(trimmed)
		}
	}
	switch provider {
	case CommitLogProviderLocal:
		return newLocalCommitLogConsensus(), nil
	case CommitLogProviderEtcdRaft:
		// Stage-1 skeleton: provider is selectable, wiring is deferred.
		return &etcdRaftCommitLogConsensus{}, nil
	default:
		return nil, fmt.Errorf("unknown commit log provider %q", provider)
	}
}

func cloneControlMutation(mutation controlMutation) controlMutation {
	mutation.Split = append([]byte(nil), mutation.Split...)
	return mutation
}

func cloneDataMutation(mutation dataMutation) dataMutation {
	mutation.Key = append([]byte(nil), mutation.Key...)
	mutation.Value = append([]byte(nil), mutation.Value...)
	return mutation
}
