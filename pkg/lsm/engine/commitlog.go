package engine

import (
	"context"
	"errors"
	"fmt"

	internalcommitlog "lsmengine/internal/lsm/commitlog"
	"lsmengine/pkg/lsm/errs"
)

type builtinCommitLogConsensus struct {
	inner internalcommitlog.Consensus
}

type internalCommitLogIndexObserver interface {
	ObserveCommittedIndex(index uint64)
}

type commitLogCommittedEntryObserver interface {
	ObserveCommittedControl(entry controlCommittedEntry) error
	ObserveCommittedData(entry dataCommittedEntry) error
}

type commitLogCommittedEntryObserverSetter interface {
	SetCommittedEntryObserver(observer commitLogCommittedEntryObserver) error
}

type commitLogStateSnapshotter interface {
	CaptureStateSnapshot(index uint64) ([]byte, error)
}

type commitLogStateSnapshotterSetter interface {
	SetStateSnapshotter(snapshotter commitLogStateSnapshotter) error
}

type commitLogStateSnapshotApplier interface {
	ApplyStateSnapshot(index uint64, data []byte) error
}

type commitLogStateSnapshotApplierSetter interface {
	SetStateSnapshotApplier(applier commitLogStateSnapshotApplier) error
}

func (c *builtinCommitLogConsensus) CommitControl(ctx context.Context, mutation controlMutation) (controlCommittedEntry, error) {
	entry, err := c.inner.CommitControl(ctx, toInternalControlMutation(mutation))
	if err != nil {
		return controlCommittedEntry{}, mapInternalCommitLogError(err)
	}
	return fromInternalControlCommittedEntry(entry), nil
}

func (c *builtinCommitLogConsensus) CommitData(ctx context.Context, mutation dataMutation) (dataCommittedEntry, error) {
	entry, err := c.inner.CommitData(ctx, toInternalDataMutation(mutation))
	if err != nil {
		return dataCommittedEntry{}, mapInternalCommitLogError(err)
	}
	return fromInternalDataCommittedEntry(entry), nil
}

func (c *builtinCommitLogConsensus) HandlePeerMessages(ctx context.Context, messages []CommitLogPeerMessage) error {
	if len(messages) == 0 {
		return nil
	}
	return c.inner.HandlePeerMessages(ctx, toInternalPeerMessages(messages))
}

func (c *builtinCommitLogConsensus) ChangeMembership(ctx context.Context, change commitLogMembershipChange) error {
	changer, ok := c.inner.(internalcommitlog.MembershipChanger)
	if !ok {
		return nil
	}
	return mapInternalCommitLogError(changer.ChangeMembership(ctx, toInternalMembershipChange(change)))
}

func mapInternalCommitLogError(err error) error {
	switch {
	case errors.Is(err, internalcommitlog.ErrNotLeader):
		return fmt.Errorf("%w: %v", errs.ErrNotLeader, err)
	case errors.Is(err, internalcommitlog.ErrUnavailable):
		return fmt.Errorf("%w: %v", errs.ErrCommitLogUnavailable, err)
	default:
		return err
	}
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

func (c *builtinCommitLogConsensus) SetCommittedEntryObserver(observer commitLogCommittedEntryObserver) error {
	setter, ok := c.inner.(internalcommitlog.CommittedEntryObserverSetter)
	if !ok {
		return nil
	}
	if observer == nil {
		return setter.SetCommittedEntryObserver(nil)
	}
	return setter.SetCommittedEntryObserver(internalCommittedEntryObserver{observer: observer})
}

func (c *builtinCommitLogConsensus) SetStateSnapshotter(snapshotter commitLogStateSnapshotter) error {
	setter, ok := c.inner.(internalcommitlog.StateSnapshotterSetter)
	if !ok {
		return nil
	}
	if snapshotter == nil {
		return setter.SetStateSnapshotter(nil)
	}
	return setter.SetStateSnapshotter(internalStateSnapshotter{snapshotter: snapshotter})
}

func (c *builtinCommitLogConsensus) SetStateSnapshotApplier(applier commitLogStateSnapshotApplier) error {
	setter, ok := c.inner.(internalcommitlog.StateSnapshotApplierSetter)
	if !ok {
		return nil
	}
	if applier == nil {
		return setter.SetStateSnapshotApplier(nil)
	}
	return setter.SetStateSnapshotApplier(internalStateSnapshotApplier{applier: applier})
}

func newBuiltinCommitLogConsensus(opts Options, provider CommitLogProvider) (commitLogConsensus, error) {
	cfg := internalcommitlog.Config{
		Provider: internalcommitlog.Provider(provider),
		DataDir:  opts.DataDir,
		NodeID:   opts.NodeID,
	}
	if opts.Raft != nil {
		cfg.Peers = append([]string(nil), opts.Raft.Peers...)
	}
	if opts.CommitLog != nil {
		cfg.SnapshotPolicy = internalcommitlog.SnapshotPolicy{
			AppliedEntries: opts.CommitLog.SnapshotPolicy.AppliedEntries,
			RetainEntries:  opts.CommitLog.SnapshotPolicy.RetainEntries,
		}
		if opts.CommitLog.Transport != nil {
			cfg.Transport = internalPeerTransport{transport: opts.CommitLog.Transport}
		}
	}
	consensus, err := internalcommitlog.NewBuiltin(cfg)
	if err != nil {
		return nil, err
	}
	return &builtinCommitLogConsensus{inner: consensus}, nil
}

type internalPeerTransport struct {
	transport CommitLogPeerTransport
}

type internalCommittedEntryObserver struct {
	observer commitLogCommittedEntryObserver
}

type internalStateSnapshotter struct {
	snapshotter commitLogStateSnapshotter
}

type internalStateSnapshotApplier struct {
	applier commitLogStateSnapshotApplier
}

func (s internalStateSnapshotter) CaptureStateSnapshot(index uint64) ([]byte, error) {
	if s.snapshotter == nil {
		return nil, nil
	}
	return s.snapshotter.CaptureStateSnapshot(index)
}

func (a internalStateSnapshotApplier) ApplyStateSnapshot(index uint64, data []byte) error {
	if a.applier == nil {
		return nil
	}
	copied := append([]byte(nil), data...)
	return a.applier.ApplyStateSnapshot(index, copied)
}

func (o internalCommittedEntryObserver) ObserveCommittedControl(entry internalcommitlog.ControlCommittedEntry) error {
	if o.observer == nil {
		return nil
	}
	return o.observer.ObserveCommittedControl(fromInternalControlCommittedEntry(entry))
}

func (o internalCommittedEntryObserver) ObserveCommittedData(entry internalcommitlog.DataCommittedEntry) error {
	if o.observer == nil {
		return nil
	}
	return o.observer.ObserveCommittedData(fromInternalDataCommittedEntry(entry))
}

func (t internalPeerTransport) Send(ctx context.Context, messages []internalcommitlog.PeerMessage) error {
	if t.transport == nil {
		return nil
	}
	return t.transport.Send(ctx, fromInternalPeerMessages(messages))
}

func newEtcdRaftCommitLogConsensus(opts Options) (commitLogConsensus, error) {
	return newBuiltinCommitLogConsensus(opts, CommitLogProviderEtcdRaft)
}

func toInternalMembershipChange(change commitLogMembershipChange) internalcommitlog.MembershipChange {
	out := internalcommitlog.MembershipChange{
		NodeID: change.NodeID,
	}
	switch change.Type {
	case CommitLogMembershipChangeAddNode:
		out.Type = internalcommitlog.MembershipChangeAddNode
	case CommitLogMembershipChangeRemoveNode:
		out.Type = internalcommitlog.MembershipChangeRemoveNode
	default:
		out.Type = internalcommitlog.MembershipChangeType(change.Type)
	}
	return out
}

func cloneCommitLogPeerMessages(messages []CommitLogPeerMessage) []CommitLogPeerMessage {
	if len(messages) == 0 {
		return nil
	}
	out := make([]CommitLogPeerMessage, len(messages))
	for i := range messages {
		out[i] = messages[i]
		out[i].Payload = append([]byte(nil), messages[i].Payload...)
	}
	return out
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
	status := CommitLogRuntimeStatus{
		Mode:           s.Mode,
		Index:          s.Index,
		Term:           s.Term,
		SnapshotIndex:  s.SnapshotIndex,
		Leader:         s.Leader,
		Replicas:       s.Replicas,
		WriteAvailable: s.WriteAvailable,
		LeaderKnown:    s.LeaderKnown,
		Health:         s.Health,
		LastErrorCode:  s.LastErrorCode,
		LastError:      s.LastError,
	}
	if !s.LastErrorAt.IsZero() {
		lastErrorAt := s.LastErrorAt
		status.LastErrorAt = &lastErrorAt
	}
	return status
}

func toInternalPeerMessages(messages []CommitLogPeerMessage) []internalcommitlog.PeerMessage {
	if len(messages) == 0 {
		return nil
	}
	out := make([]internalcommitlog.PeerMessage, len(messages))
	for i, msg := range messages {
		out[i] = internalcommitlog.PeerMessage{
			From:    msg.From,
			To:      msg.To,
			Payload: append([]byte(nil), msg.Payload...),
		}
	}
	return out
}

func fromInternalPeerMessages(messages []internalcommitlog.PeerMessage) []CommitLogPeerMessage {
	if len(messages) == 0 {
		return nil
	}
	out := make([]CommitLogPeerMessage, len(messages))
	for i, msg := range messages {
		out[i] = CommitLogPeerMessage{
			From:    msg.From,
			To:      msg.To,
			Payload: append([]byte(nil), msg.Payload...),
		}
	}
	return out
}

func copyCommitLogPeerMessages(messages []CommitLogPeerMessage) []CommitLogPeerMessage {
	if len(messages) == 0 {
		return nil
	}
	out := make([]CommitLogPeerMessage, len(messages))
	for i, msg := range messages {
		out[i] = CommitLogPeerMessage{
			From:    msg.From,
			To:      msg.To,
			Payload: append([]byte(nil), msg.Payload...),
		}
	}
	return out
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
