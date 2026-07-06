package engine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

type testCommitLogFactory struct {
	mu        sync.Mutex
	calls     int
	consensus CommitLogConsensus
	err       error
}

func (f *testCommitLogFactory) New(_ Options) (CommitLogConsensus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.consensus, nil
}

func (f *testCommitLogFactory) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type testCommitLogConsensus struct {
	mu            sync.Mutex
	controlCalls  int
	dataCalls     int
	commits       []string
	peerCalls     int
	peerMsgCount  int
	provider      CommitLogProvider
	runtimeStatus CommitLogRuntimeStatus
}

func (c *testCommitLogConsensus) CommitControl(_ context.Context, mutation CommitLogControlMutation) (CommitLogControlCommittedEntry, error) {
	c.mu.Lock()
	c.controlCalls++
	c.commits = append(c.commits, "control:"+mutation.Kind)
	c.mu.Unlock()
	return CommitLogControlCommittedEntry{
		Commit:   CommitLogCommit{Index: 1, Term: 1},
		Mutation: mutation,
	}, nil
}

func (c *testCommitLogConsensus) CommitData(_ context.Context, mutation CommitLogDataMutation) (CommitLogDataCommittedEntry, error) {
	c.mu.Lock()
	c.dataCalls++
	c.commits = append(c.commits, "data:"+mutation.Kind)
	c.mu.Unlock()
	return CommitLogDataCommittedEntry{
		Commit:   CommitLogCommit{Index: 2, Term: 1},
		Mutation: mutation,
		Seq:      2,
	}, nil
}

func (c *testCommitLogConsensus) HandlePeerMessages(_ context.Context, messages []RaftPeerMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.peerCalls++
	c.peerMsgCount += len(messages)
	return nil
}

func (c *testCommitLogConsensus) Provider() CommitLogProvider {
	if c.provider == "" {
		return "custom"
	}
	return c.provider
}

func (c *testCommitLogConsensus) RuntimeStatus() CommitLogRuntimeStatus {
	if c.runtimeStatus.Term == 0 {
		return CommitLogRuntimeStatus{Mode: "custom", Index: 2, Term: 1, Leader: true, Replicas: 1}
	}
	return c.runtimeStatus
}

func (c *testCommitLogConsensus) DataCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dataCalls
}

func (c *testCommitLogConsensus) Calls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.commits...)
}

func (c *testCommitLogConsensus) PeerCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peerCalls
}

func (c *testCommitLogConsensus) PeerMessageCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peerMsgCount
}

func TestCommitLogFactoryOverridesProviderSelection(t *testing.T) {
	consensus := &testCommitLogConsensus{provider: "custom-raft"}
	factory := &testCommitLogFactory{consensus: consensus}

	store, err := New(Options{
		DataDir: t.TempDir(),
		CommitLog: &CommitLogOptions{
			Provider: CommitLogProviderLocal,
			Factory:  factory,
		},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer store.Close()

	if got := factory.Calls(); got != 1 {
		t.Fatalf("expected factory to be called once, got %d", got)
	}
	status := store.ClusterStatus()
	if status.CommitLog != "custom-raft" {
		t.Fatalf("expected commit log provider custom-raft, got %q", status.CommitLog)
	}
	if err := store.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if got := consensus.DataCalls(); got == 0 {
		t.Fatalf("expected custom consensus data apply to be invoked")
	}
	got, ok := store.Get([]byte("a"))
	if !ok {
		t.Fatalf("expected engine to apply provider committed entry")
	}
	if string(got.Value) != "1" {
		t.Fatalf("expected engine to apply provider committed entry, got %q", got.Value)
	}
	if calls := consensus.Calls(); len(calls) != 1 || calls[0] != "data:put" {
		t.Fatalf("expected provider commit before engine apply, got calls %v", calls)
	}
}

func TestCommitLogFactoryErrorFailsInitialization(t *testing.T) {
	factory := &testCommitLogFactory{err: errors.New("factory boom")}
	_, err := New(Options{
		DataDir: t.TempDir(),
		CommitLog: &CommitLogOptions{
			Factory: factory,
		},
	})
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "factory boom") {
		t.Fatalf("expected factory error to bubble up, got %v", err)
	}
}

func TestCommitLogFactoryNilConsensusFailsInitialization(t *testing.T) {
	factory := &testCommitLogFactory{}
	_, err := New(Options{
		DataDir: t.TempDir(),
		CommitLog: &CommitLogOptions{
			Factory: factory,
		},
	})
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "nil consensus") {
		t.Fatalf("expected nil consensus error, got %v", err)
	}
}

func TestCommitLogFactoryConsensusHandlesPeerMessages(t *testing.T) {
	consensus := &testCommitLogConsensus{provider: "custom-raft"}
	factory := &testCommitLogFactory{consensus: consensus}

	store, err := New(Options{
		DataDir: t.TempDir(),
		CommitLog: &CommitLogOptions{
			Factory: factory,
		},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer store.Close()

	if err := store.HandlePeerMessages(context.Background(), []RaftPeerMessage{
		{Type: "MsgHeartbeat", From: 2, To: 1, Term: 1},
		{Type: "MsgApp", From: 2, To: 1, Term: 1},
	}); err != nil {
		t.Fatalf("handle peer messages: %v", err)
	}
	if got := consensus.PeerCalls(); got != 1 {
		t.Fatalf("expected one peer ingress call, got %d", got)
	}
	if got := consensus.PeerMessageCount(); got != 2 {
		t.Fatalf("expected two peer messages, got %d", got)
	}
}

func TestLocalCommitLogPeerMessagesNoop(t *testing.T) {
	store, err := New(Options{
		DataDir: t.TempDir(),
		CommitLog: &CommitLogOptions{
			Provider: CommitLogProviderLocal,
		},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer store.Close()

	if err := store.HandlePeerMessages(context.Background(), []RaftPeerMessage{
		{Type: "not-a-real-raft-message", From: 2, To: 1},
	}); err != nil {
		t.Fatalf("expected local peer ingress to no-op, got %v", err)
	}
}
