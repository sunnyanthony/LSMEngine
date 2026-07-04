package commitlog

import (
	"context"
	"sync"
)

type localConsensus struct {
	mu    sync.Mutex
	index uint64
	term  uint64
}

func newLocalConsensus() *localConsensus {
	return &localConsensus{term: 1}
}

func (c *localConsensus) CommitControl(_ context.Context, mutation ControlMutation) (ControlCommittedEntry, error) {
	commit := c.nextCommit()
	return ControlCommittedEntry{
		Commit:   commit,
		Mutation: cloneControlMutation(mutation),
	}, nil
}

func (c *localConsensus) CommitData(_ context.Context, mutation DataMutation) (DataCommittedEntry, error) {
	commit := c.nextCommit()
	return DataCommittedEntry{
		Commit:   commit,
		Mutation: cloneDataMutation(mutation),
		Seq:      commit.Index,
	}, nil
}

func (c *localConsensus) nextCommit() Commit {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.index++
	return Commit{
		Index: c.index,
		Term:  c.term,
	}
}

func (c *localConsensus) ObserveCommittedIndex(index uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if index > c.index {
		c.index = index
	}
}

func (c *localConsensus) Provider() Provider {
	return ProviderLocal
}

func (c *localConsensus) RuntimeStatus() RuntimeStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	return RuntimeStatus{
		Mode:     "local",
		Index:    c.index,
		Term:     c.term,
		Leader:   true,
		Replicas: 1,
	}
}
