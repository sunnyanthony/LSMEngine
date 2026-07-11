package commitlog

import (
	"context"
	"time"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

type Provider string

const (
	ProviderLocal    Provider = "local"
	ProviderEtcdRaft Provider = "etcd-raft"
)

type Config struct {
	Provider  Provider
	DataDir   string
	NodeID    string
	Peers     []string
	Transport RaftMessageTransport
}

type RaftMessageTransport interface {
	Send(ctx context.Context, messages []raftpb.Message) error
}

type CommittedEntryObserver interface {
	ObserveCommittedControl(entry ControlCommittedEntry) error
	ObserveCommittedData(entry DataCommittedEntry) error
}

type CommittedEntryObserverSetter interface {
	SetCommittedEntryObserver(observer CommittedEntryObserver) error
}

type ControlMutation struct {
	Kind    string
	ShardID string
	Target  string
	Split   []byte
	NodeID  string
}

type DataMutation struct {
	Kind  string
	Key   []byte
	Value []byte
}

type Commit struct {
	Index uint64
	Term  uint64
}

type ControlCommittedEntry struct {
	Commit   Commit
	Mutation ControlMutation
}

type DataCommittedEntry struct {
	Commit   Commit
	Mutation DataMutation
	Seq      uint64
}

type RuntimeStatus struct {
	Mode           string
	Index          uint64
	Term           uint64
	Leader         bool
	Replicas       int
	WriteAvailable bool
	LeaderKnown    bool
	Health         string
	LastErrorCode  string
	LastError      string
	LastErrorAt    time.Time
}

type Consensus interface {
	CommitControl(ctx context.Context, mutation ControlMutation) (ControlCommittedEntry, error)
	CommitData(ctx context.Context, mutation DataMutation) (DataCommittedEntry, error)
	HandlePeerMessages(ctx context.Context, messages []raftpb.Message) error
	Provider() Provider
	RuntimeStatus() RuntimeStatus
}
