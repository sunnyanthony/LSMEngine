package engine

import (
	"context"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// CommitLogProvider selects the commit-log backend.
type CommitLogProvider string

const (
	CommitLogProviderLocal    CommitLogProvider = "local"
	CommitLogProviderEtcdRaft CommitLogProvider = "etcd-raft"
)

// CommitLogOptions controls commit-log provider selection and injection.
type CommitLogOptions struct {
	Provider  CommitLogProvider    `json:"provider" yaml:"provider"`
	Transport RaftMessageTransport `json:"-" yaml:"-"`
	Factory   CommitLogFactory     `json:"-" yaml:"-"`
}

// RaftMessageTransport sends raft protocol messages to peer nodes.
//
// This transport is outbound from the local raft node. Inbound delivery is
// handled via CommitLogConsensus.HandlePeerMessages.
type RaftMessageTransport interface {
	Send(ctx context.Context, messages []raftpb.Message) error
}

// CommitLogControlMutation is a control-plane state mutation that must go
// through the commit-log correctness path.
type CommitLogControlMutation struct {
	Kind    string
	ShardID string
	Target  string
	Split   []byte
	NodeID  string
}

// CommitLogDataMutation is a data-plane mutation that must go through the
// commit-log correctness path.
type CommitLogDataMutation struct {
	Kind  string
	Key   []byte
	Value []byte
}

// CommitLogCommit identifies a mutation's durable ordered position.
type CommitLogCommit struct {
	Index uint64
	Term  uint64
}

// CommitLogControlCommittedEntry is the committed control mutation the engine
// must apply locally.
type CommitLogControlCommittedEntry struct {
	Commit   CommitLogCommit
	Mutation CommitLogControlMutation
}

// CommitLogDataCommittedEntry is the committed data mutation the engine must
// apply locally. Seq must be deterministic for a given committed entry.
type CommitLogDataCommittedEntry struct {
	Commit   CommitLogCommit
	Mutation CommitLogDataMutation
	Seq      uint64
}

// CommitLogRuntimeStatus exposes commit-log runtime progress and leadership state.
type CommitLogRuntimeStatus struct {
	Mode     string `json:"mode"`
	Index    uint64 `json:"index"`
	Term     uint64 `json:"term"`
	Leader   bool   `json:"leader"`
	Replicas int    `json:"replicas"`
}

// CommitLogConsensus is the provider contract for commit-log implementations.
type CommitLogConsensus interface {
	CommitControl(ctx context.Context, mutation CommitLogControlMutation) (CommitLogControlCommittedEntry, error)
	CommitData(ctx context.Context, mutation CommitLogDataMutation) (CommitLogDataCommittedEntry, error)
	HandlePeerMessages(ctx context.Context, messages []raftpb.Message) error
	Provider() CommitLogProvider
	RuntimeStatus() CommitLogRuntimeStatus
}

// CommitLogFactory builds a commit-log provider implementation.
//
// If CommitLogOptions.Factory is set, engine initialization uses it before
// built-in provider selection.
type CommitLogFactory interface {
	New(opts Options) (CommitLogConsensus, error)
}

// Internal aliases keep call sites concise while preserving the exported
// contract types above.
type controlMutation = CommitLogControlMutation
type dataMutation = CommitLogDataMutation
type controlCommittedEntry = CommitLogControlCommittedEntry
type dataCommittedEntry = CommitLogDataCommittedEntry
type commitLogConsensus = CommitLogConsensus

type commitLogIndexObserver interface {
	ObserveCommittedIndex(index uint64)
}
