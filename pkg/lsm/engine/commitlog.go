package engine

import (
	"context"
	"fmt"

	"go.etcd.io/etcd/raft/v3/raftpb"
	internalcommitlog "lsmengine/internal/lsm/commitlog"
)

type builtinCommitLogConsensus struct {
	inner internalcommitlog.Consensus
}

type internalCommitLogIndexObserver interface {
	ObserveCommittedIndex(index uint64)
}

func (c *builtinCommitLogConsensus) CommitControl(ctx context.Context, mutation controlMutation) (controlCommittedEntry, error) {
	entry, err := c.inner.CommitControl(ctx, toInternalControlMutation(mutation))
	if err != nil {
		return controlCommittedEntry{}, err
	}
	return fromInternalControlCommittedEntry(entry), nil
}

func (c *builtinCommitLogConsensus) CommitData(ctx context.Context, mutation dataMutation) (dataCommittedEntry, error) {
	entry, err := c.inner.CommitData(ctx, toInternalDataMutation(mutation))
	if err != nil {
		return dataCommittedEntry{}, err
	}
	return fromInternalDataCommittedEntry(entry), nil
}

func (c *builtinCommitLogConsensus) HandlePeerMessages(ctx context.Context, messages []RaftPeerMessage) error {
	if len(messages) == 0 {
		return nil
	}
	if c.inner.Provider() == internalcommitlog.ProviderLocal {
		return nil
	}
	converted := make([]raftpb.Message, 0, len(messages))
	for _, message := range messages {
		raftMessage, err := toRaftPBMessage(message)
		if err != nil {
			return err
		}
		converted = append(converted, raftMessage)
	}
	return c.inner.HandlePeerMessages(ctx, converted)
}

func (c *builtinCommitLogConsensus) Provider() CommitLogProvider {
	return CommitLogProvider(c.inner.Provider())
}

func (c *builtinCommitLogConsensus) RuntimeStatus() CommitLogRuntimeStatus {
	return fromInternalRuntimeStatus(c.inner.RuntimeStatus())
}

func (c *builtinCommitLogConsensus) ObserveCommittedIndex(index uint64) {
	observer, ok := c.inner.(internalCommitLogIndexObserver)
	if !ok {
		return
	}
	observer.ObserveCommittedIndex(index)
}

func newBuiltinCommitLogConsensus(opts Options, provider CommitLogProvider) (commitLogConsensus, error) {
	cfg := internalcommitlog.Config{
		Provider: internalcommitlog.Provider(provider),
		NodeID:   opts.NodeID,
	}
	if opts.Raft != nil {
		cfg.Peers = append([]string(nil), opts.Raft.Peers...)
	}
	if opts.CommitLog != nil {
		if opts.CommitLog.Transport != nil {
			cfg.Transport = raftPeerTransportAdapter{transport: opts.CommitLog.Transport}
		}
	}
	consensus, err := internalcommitlog.NewBuiltin(cfg)
	if err != nil {
		return nil, err
	}
	return &builtinCommitLogConsensus{inner: consensus}, nil
}

func newEtcdRaftCommitLogConsensus(opts Options) (commitLogConsensus, error) {
	return newBuiltinCommitLogConsensus(opts, CommitLogProviderEtcdRaft)
}

type raftPeerTransportAdapter struct {
	transport RaftMessageTransport
}

func (a raftPeerTransportAdapter) Send(ctx context.Context, messages []raftpb.Message) error {
	if a.transport == nil {
		return fmt.Errorf("raft peer transport unavailable")
	}
	converted := make([]RaftPeerMessage, 0, len(messages))
	for _, message := range messages {
		peerMessage, err := fromRaftPBMessage(message)
		if err != nil {
			return err
		}
		converted = append(converted, peerMessage)
	}
	return a.transport.Send(ctx, converted)
}

func cloneRaftPeerMessages(messages []RaftPeerMessage) []RaftPeerMessage {
	if len(messages) == 0 {
		return nil
	}
	out := make([]RaftPeerMessage, len(messages))
	for i := range messages {
		out[i] = messages[i]
		out[i].Payload = append([]byte(nil), messages[i].Payload...)
	}
	return out
}

func fromRaftPBMessage(message raftpb.Message) (RaftPeerMessage, error) {
	payload, err := message.Marshal()
	if err != nil {
		return RaftPeerMessage{}, fmt.Errorf("marshal raft peer message: %w", err)
	}
	return RaftPeerMessage{
		From:    message.From,
		To:      message.To,
		Term:    message.Term,
		Type:    message.Type.String(),
		Payload: payload,
	}, nil
}

func toRaftPBMessage(message RaftPeerMessage) (raftpb.Message, error) {
	if len(message.Payload) > 0 {
		var out raftpb.Message
		if err := out.Unmarshal(message.Payload); err != nil {
			return raftpb.Message{}, fmt.Errorf("unmarshal raft peer message: %w", err)
		}
		return out, nil
	}
	messageType, ok := raftpb.MessageType_value[message.Type]
	if !ok {
		return raftpb.Message{}, fmt.Errorf("unknown raft peer message type %q", message.Type)
	}
	return raftpb.Message{
		Type: raftpb.MessageType(messageType),
		From: message.From,
		To:   message.To,
		Term: message.Term,
	}, nil
}

func toInternalControlMutation(m CommitLogControlMutation) internalcommitlog.ControlMutation {
	return internalcommitlog.ControlMutation{
		Kind:    m.Kind,
		ShardID: m.ShardID,
		Target:  m.Target,
		Split:   append([]byte(nil), m.Split...),
		NodeID:  m.NodeID,
	}
}

func toInternalDataMutation(m CommitLogDataMutation) internalcommitlog.DataMutation {
	return internalcommitlog.DataMutation{
		Kind:  m.Kind,
		Key:   append([]byte(nil), m.Key...),
		Value: append([]byte(nil), m.Value...),
	}
}

func fromInternalCommit(c internalcommitlog.Commit) CommitLogCommit {
	return CommitLogCommit{
		Index: c.Index,
		Term:  c.Term,
	}
}

func fromInternalControlCommittedEntry(entry internalcommitlog.ControlCommittedEntry) controlCommittedEntry {
	return controlCommittedEntry{
		Commit:   fromInternalCommit(entry.Commit),
		Mutation: fromInternalControlMutation(entry.Mutation),
	}
}

func fromInternalDataCommittedEntry(entry internalcommitlog.DataCommittedEntry) dataCommittedEntry {
	return dataCommittedEntry{
		Commit:   fromInternalCommit(entry.Commit),
		Mutation: fromInternalDataMutation(entry.Mutation),
		Seq:      entry.Seq,
	}
}

func fromInternalRuntimeStatus(s internalcommitlog.RuntimeStatus) CommitLogRuntimeStatus {
	return CommitLogRuntimeStatus{
		Mode:     s.Mode,
		Index:    s.Index,
		Term:     s.Term,
		Leader:   s.Leader,
		Replicas: s.Replicas,
	}
}

func fromInternalControlMutation(m internalcommitlog.ControlMutation) controlMutation {
	return controlMutation{
		Kind:    m.Kind,
		ShardID: m.ShardID,
		Target:  m.Target,
		Split:   append([]byte(nil), m.Split...),
		NodeID:  m.NodeID,
	}
}

func fromInternalDataMutation(m internalcommitlog.DataMutation) dataMutation {
	return dataMutation{
		Kind:  m.Kind,
		Key:   append([]byte(nil), m.Key...),
		Value: append([]byte(nil), m.Value...),
	}
}
