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
	Provider       Provider
	DataDir        string
	NodeID         string
	Peers          []string
	Transport      RaftMessageTransport
	SnapshotPolicy SnapshotPolicy
}

type SnapshotPolicy struct {
	AppliedEntries uint64
	RetainEntries  uint64
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

type StateSnapshotter interface {
	CaptureStateSnapshot(index uint64) ([]byte, error)
}

type StateSnapshotterSetter interface {
	SetStateSnapshotter(snapshotter StateSnapshotter) error
}

type MembershipChangeType string

const (
	MembershipChangeAddNode    MembershipChangeType = "add-node"
	MembershipChangeRemoveNode MembershipChangeType = "remove-node"
)

type MembershipChange struct {
	Type   MembershipChangeType
	NodeID string
}

type MembershipChanger interface {
	ChangeMembership(ctx context.Context, change MembershipChange) error
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
	SnapshotIndex  uint64
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
