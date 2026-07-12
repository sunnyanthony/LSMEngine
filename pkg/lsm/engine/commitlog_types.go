package engine

import (
	"context"
	"time"

	"lsmengine/internal/lsm/raftid"
)

// CommitLogProvider selects the commit-log backend.
type CommitLogProvider string

const (
	CommitLogProviderLocal    CommitLogProvider = "local"
	CommitLogProviderEtcdRaft CommitLogProvider = "etcd-raft"
)

// CommitLogOptions controls commit-log provider selection and injection.
type CommitLogOptions struct {
	Provider       CommitLogProvider       `json:"provider" yaml:"provider"`
	Transport      RaftMessageTransport    `json:"-" yaml:"-"`
	Factory        CommitLogFactory        `json:"-" yaml:"-"`
	SnapshotPolicy CommitLogSnapshotPolicy `json:"snapshot_policy" yaml:"snapshot_policy"`
}

// CommitLogSnapshotPolicy controls provider-owned raft log snapshot/compaction.
//
// AppliedEntries disables automatic provider snapshots when zero. RetainEntries
// keeps a tail of recent raft log entries after each snapshot. For the builtin
// etcd-raft provider, snapshot data is captured through the engine after the
// matching commit index has been applied locally.
type CommitLogSnapshotPolicy struct {
	AppliedEntries uint64 `json:"applied_entries" yaml:"applied_entries"`
	RetainEntries  uint64 `json:"retain_entries" yaml:"retain_entries"`
}

// RaftPeerMessage is an LSM-owned envelope for raft peer traffic.
//
// Payload is provider-specific encoded message data. Built-in providers keep
// library-specific protocol details behind their adapters instead of exposing
// those types through public APIs.
type RaftPeerMessage struct {
	From    uint64 `json:"from"`
	To      uint64 `json:"to"`
	Term    uint64 `json:"term,omitempty"`
	Type    string `json:"type,omitempty"`
	Payload []byte `json:"payload,omitempty"`
}

// RaftMessageTransport sends raft peer messages to other nodes.
//
// This transport is outbound from the local raft node. Inbound delivery is
// handled via CommitLogConsensus.HandlePeerMessages.
type RaftMessageTransport interface {
	Send(ctx context.Context, messages []RaftPeerMessage) error
}

// RaftPeerID returns the deterministic raft id used for a configured node name.
func RaftPeerID(nodeID string) uint64 {
	return raftid.StableNodeID(nodeID)
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
	Mode           string     `json:"mode"`
	Index          uint64     `json:"index"`
	Term           uint64     `json:"term"`
	SnapshotIndex  uint64     `json:"snapshot_index,omitempty"`
	Leader         bool       `json:"leader"`
	Replicas       int        `json:"replicas"`
	WriteAvailable bool       `json:"write_available"`
	LeaderKnown    bool       `json:"leader_known"`
	Health         string     `json:"health"`
	LastErrorCode  string     `json:"last_error_code,omitempty"`
	LastError      string     `json:"last_error,omitempty"`
	LastErrorAt    *time.Time `json:"last_error_at,omitempty"`
}

// CommitLogConsensus is the provider contract for commit-log implementations.
type CommitLogConsensus interface {
	CommitControl(ctx context.Context, mutation CommitLogControlMutation) (CommitLogControlCommittedEntry, error)
	CommitData(ctx context.Context, mutation CommitLogDataMutation) (CommitLogDataCommittedEntry, error)
	HandlePeerMessages(ctx context.Context, messages []RaftPeerMessage) error
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
