package commitlog

import (
	"context"
	"time"
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
	Transport      PeerTransport
	SnapshotPolicy SnapshotPolicy
	Join           bool
}

type SnapshotPolicy struct {
	AppliedEntries uint64
	RetainEntries  uint64
}

type PeerMessage struct {
	From    uint64
	To      uint64
	Payload []byte
}

type PeerTransport interface {
	Send(ctx context.Context, messages []PeerMessage) error
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

// BoundaryStateSnapshotter captures state at a committed log boundary whose
// final mutation is stateMachineIndex. The provider must prove no intervening
// mutation is unapplied. A false ready result defers capture until engine apply;
// payloads must still encode index, not stateMachineIndex, as their boundary.
type BoundaryStateSnapshotter interface {
	CaptureStateSnapshotBoundary(index, stateMachineIndex uint64) (data []byte, ready bool, err error)
}

type StateSnapshotterSetter interface {
	SetStateSnapshotter(snapshotter StateSnapshotter) error
}

type StateSnapshotApplier interface {
	ApplyStateSnapshot(index uint64, data []byte) error
}

// StateSnapshotRestorer installs the persisted base before committed-tail
// replay, preserving independently durable engine state newer than that base.
type StateSnapshotRestorer interface {
	RestoreStateSnapshot(index uint64, data []byte) error
}

type StateSnapshotApplierSetter interface {
	SetStateSnapshotApplier(applier StateSnapshotApplier) error
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
	// StateMachineIndex excludes entries that require no state-machine apply.
	// Nil means the provider cannot report this boundary.
	StateMachineIndex *uint64
	Mode              string
	Index             uint64
	Term              uint64
	SnapshotIndex     uint64
	Leader            bool
	Replicas          int
	WriteAvailable    bool
	LeaderKnown       bool
	Health            string
	LastErrorCode     string
	LastError         string
	LastErrorAt       time.Time
}

type Consensus interface {
	CommitControl(ctx context.Context, mutation ControlMutation) (ControlCommittedEntry, error)
	CommitData(ctx context.Context, mutation DataMutation) (DataCommittedEntry, error)
	HandlePeerMessages(ctx context.Context, messages []PeerMessage) error
	Provider() Provider
	RuntimeStatus() RuntimeStatus
}
