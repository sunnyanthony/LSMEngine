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
