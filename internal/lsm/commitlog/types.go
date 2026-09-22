package commitlog

import "context"

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
	Transport PeerTransport
}

type PeerMessage struct {
	From    uint64
	To      uint64
	Payload []byte
}

type PeerTransport interface {
	Send(ctx context.Context, messages []PeerMessage) error
}

type ControlMutation struct {
	ExpectedRevision *uint64
	OperationID      string
	Kind             string
	ShardID          string
	Target           string
	Split            []byte
	NodeID           string
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

// RecoveredEntry contains exactly one committed mutation, in log order.
type RecoveredEntry struct {
	Control *ControlCommittedEntry
	Data    *DataCommittedEntry
}

// RecoverySource exposes committed history without proposing new mutations.
type RecoverySource interface {
	RecoveredEntries() []RecoveredEntry
}

// CommittedEntrySource returns owned mutations in commit order, strictly after
// the supplied index. Reading does not acknowledge or discard entries: callers
// advance their cursor only after successful application.
type CommittedEntrySource interface {
	CommittedEntriesAfter(index uint64) []RecoveredEntry
}

type RuntimeStatus struct {
	Mode     string
	Index    uint64
	Term     uint64
	Leader   bool
	Replicas int
}

type Consensus interface {
	CommitControl(ctx context.Context, mutation ControlMutation) (ControlCommittedEntry, error)
	CommitData(ctx context.Context, mutation DataMutation) (DataCommittedEntry, error)
	HandlePeerMessages(ctx context.Context, messages []PeerMessage) error
	Provider() Provider
	RuntimeStatus() RuntimeStatus
}
