package commitlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"lsmengine/internal/lsm/raftid"
)

type raftCommitProposal struct {
	ID      uint64           `json:"id"`
	Kind    string           `json:"kind"`
	Control *ControlMutation `json:"control,omitempty"`
	Data    *DataMutation    `json:"data,omitempty"`
}

type raftCommittedProposal struct {
	ID      uint64
	Kind    string
	Control *ControlCommittedEntry
	Data    *DataCommittedEntry
}

type pendingRaftProposal struct {
	done    bool
	control *ControlCommittedEntry
	data    *DataCommittedEntry
	err     error
}

type etcdRaftConsensus struct {
	mu sync.Mutex

	nodeID          uint64
	rawNode         *raft.RawNode
	storage         *raftPersistentStorage
	transport       RaftMessageTransport
	observer        CommittedEntryObserver
	snapshotter     StateSnapshotter
	snapshotApplier StateSnapshotApplier
	proposalSeq     uint64
	pending         map[uint64]*pendingRaftProposal
	committed       []raftCommittedProposal
	index           uint64
	term            uint64
	snapshotPolicy  SnapshotPolicy
	snapshotIndex   uint64
	replicas        int

	lastErrorCode string
	lastError     string
	lastErrorAt   time.Time

	closed    bool
	closeOnce sync.Once
	stopCh    chan struct{}
	doneCh    chan struct{}
}

const (
	etcdRaftElectionTick   = 10
	etcdRaftHeartbeatTick  = 1
	etcdRaftApplyTimeout   = 5 * time.Second
	etcdRaftMemberTimeout  = 10 * time.Second
	etcdRaftAdvanceMaxStep = 2048
	etcdRaftSendTimeout    = 2 * time.Second
	etcdRaftTickInterval   = 100 * time.Millisecond
)

func newEtcdRaftConsensus(cfg Config) (*etcdRaftConsensus, error) {
	nodeName := strings.TrimSpace(cfg.NodeID)
	if nodeName == "" {
		nodeName = "node-0"
	}
	nodeID := stableRaftNodeID(nodeName)
	dataDir := strings.TrimSpace(cfg.DataDir)
	if dataDir == "" {
		return nil, fmt.Errorf("raft data dir is required")
	}
	peerIDs, err := resolveRaftPeerIDs(nodeName, cfg.Peers)
	if err != nil {
		return nil, err
	}
	transport := cfg.Transport
	if len(peerIDs) > 1 && transport == nil {
		return nil, fmt.Errorf("raft transport is required when raft peers > 1")
	}
	storage, loadedLog, err := newRaftPersistentStorage(dataDir, nodeID)
	if err != nil {
		return nil, err
	}
	rawNode, err := raft.NewRawNode(&raft.Config{
		ID:              nodeID,
		ElectionTick:    etcdRaftElectionTick,
		HeartbeatTick:   etcdRaftHeartbeatTick,
		Storage:         storage,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 256,
		PreVote:         true,
	})
	if err != nil {
		return nil, fmt.Errorf("new etcd raft node: %w", err)
	}
	if !loadedLog && !cfg.Join {
		bootstrapPeers := make([]raft.Peer, 0, len(peerIDs))
		for _, id := range peerIDs {
			bootstrapPeers = append(bootstrapPeers, raft.Peer{ID: id})
		}
		if err := rawNode.Bootstrap(bootstrapPeers); err != nil {
			return nil, fmt.Errorf("bootstrap etcd raft node: %w", err)
		}
	}
	c := &etcdRaftConsensus{
		nodeID:         nodeID,
		rawNode:        rawNode,
		storage:        storage,
		transport:      transport,
		snapshotPolicy: cfg.SnapshotPolicy,
		pending:        make(map[uint64]*pendingRaftProposal),
		replicas:       len(peerIDs),
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
	}
	if hardState, _, err := storage.InitialState(); err == nil {
		c.index = hardState.Commit
		c.term = hardState.Term
	} else {
		return nil, fmt.Errorf("read raft restored state: %w", err)
	}
	if snapshot, err := storage.Snapshot(); err == nil && !raft.IsEmptySnap(snapshot) {
		c.snapshotIndex = snapshot.Metadata.Index
	} else if err != nil && !errors.Is(err, raft.ErrSnapshotTemporarilyUnavailable) {
		return nil, fmt.Errorf("read raft restored snapshot: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), etcdRaftApplyTimeout)
	defer cancel()
	if err := c.advanceUntilStableLocked(ctx); err != nil {
		return nil, err
	}
	// Multi-peer clusters may not have enough live peers yet during startup.
	// We only force immediate self-election in cluster-of-one mode.
	if len(peerIDs) == 1 {
		if err := c.ensureLeader(ctx); err != nil {
			return nil, err
		}
	}
	c.startBackgroundLoop()
	return c, nil
}

func (c *etcdRaftConsensus) CommitControl(ctx context.Context, mutation ControlMutation) (ControlCommittedEntry, error) {
	cloned := cloneControlMutation(mutation)
	pending, err := c.commitMutation(ctx, raftCommitProposal{
		Kind:    "control",
		Control: &cloned,
	})
	if err != nil {
		c.recordRuntimeError(err)
		return ControlCommittedEntry{}, err
	}
	if pending.control == nil {
		err := fmt.Errorf("raft committed non-control entry")
		c.recordRuntimeError(err)
		return ControlCommittedEntry{}, err
	}
	if pending.err != nil {
		c.recordRuntimeError(pending.err)
	}
	return *pending.control, pending.err
}

func (c *etcdRaftConsensus) CommitData(ctx context.Context, mutation DataMutation) (DataCommittedEntry, error) {
	cloned := cloneDataMutation(mutation)
	pending, err := c.commitMutation(ctx, raftCommitProposal{
		Kind: "data",
		Data: &cloned,
	})
	if err != nil {
		c.recordRuntimeError(err)
		return DataCommittedEntry{}, err
	}
	if pending.data == nil {
		err := fmt.Errorf("raft committed non-data entry")
		c.recordRuntimeError(err)
		return DataCommittedEntry{}, err
	}
	if pending.err != nil {
		c.recordRuntimeError(pending.err)
	}
	return *pending.data, pending.err
}

func (c *etcdRaftConsensus) HandlePeerMessages(ctx context.Context, messages []raftpb.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.rawNode == nil || c.storage == nil {
		err := fmt.Errorf("%w: etcd raft commit log is unavailable", ErrUnavailable)
		c.recordRuntimeErrorLocked(err)
		return err
	}
	if len(messages) == 0 {
		return nil
	}
	runCtx, cancel := withDefaultTimeout(ctx, etcdRaftApplyTimeout)
	defer cancel()
	for _, msg := range messages {
		if msg.To != 0 && msg.To != c.nodeID {
			continue
		}
		if err := c.rawNode.Step(msg); err != nil {
			err := fmt.Errorf("raft step peer message: %w", err)
			c.recordRuntimeErrorLocked(err)
			return err
		}
		if err := c.advanceUntilStableLocked(runCtx); err != nil {
			c.recordRuntimeErrorLocked(err)
			return err
		}
	}
	return nil
}

func (c *etcdRaftConsensus) ChangeMembership(ctx context.Context, change MembershipChange) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.rawNode == nil || c.storage == nil {
		err := fmt.Errorf("%w: etcd raft commit log is unavailable", ErrUnavailable)
		c.recordRuntimeErrorLocked(err)
		return err
	}
	target := strings.TrimSpace(change.NodeID)
	if target == "" {
		err := fmt.Errorf("raft membership node id is required")
		c.recordRuntimeErrorLocked(err)
		return err
	}
	cc := raftpb.ConfChange{NodeID: stableRaftNodeID(target)}
	switch change.Type {
	case MembershipChangeAddNode:
		cc.Type = raftpb.ConfChangeAddNode
	case MembershipChangeRemoveNode:
		cc.Type = raftpb.ConfChangeRemoveNode
	default:
		err := fmt.Errorf("unknown raft membership change type %q", change.Type)
		c.recordRuntimeErrorLocked(err)
		return err
	}
	if c.membershipChangeAppliedLocked(cc) {
		return nil
	}

	runCtx, cancel := withDefaultTimeout(ctx, etcdRaftMemberTimeout)
	defer cancel()
	if err := c.rawNode.ProposeConfChange(cc); err != nil {
		err := fmt.Errorf("raft propose membership change: %w", err)
		c.recordRuntimeErrorLocked(err)
		return err
	}
	for step := 0; step < etcdRaftAdvanceMaxStep; step++ {
		if err := c.advanceUntilStableLocked(runCtx); err != nil {
			if c.membershipChangeAppliedLocked(cc) {
				return nil
			}
			c.recordRuntimeErrorLocked(err)
			return err
		}
		if c.membershipChangeAppliedLocked(cc) {
			return nil
		}
		if err := runCtx.Err(); err != nil {
			if c.membershipChangeAppliedLocked(cc) {
				return nil
			}
			err := fmt.Errorf("%w: raft membership change timed out: %v", ErrUnavailable, err)
			c.recordRuntimeErrorLocked(err)
			return err
		}
		c.rawNode.Tick()
		c.mu.Unlock()
		select {
		case <-runCtx.Done():
			c.mu.Lock()
			if c.membershipChangeAppliedLocked(cc) {
				return nil
			}
			err := fmt.Errorf("%w: raft membership change timed out: %v", ErrUnavailable, runCtx.Err())
			c.recordRuntimeErrorLocked(err)
			return err
		case <-time.After(time.Millisecond):
			c.mu.Lock()
		}
	}
	err := fmt.Errorf("%w: raft membership change did not apply", ErrUnavailable)
	c.recordRuntimeErrorLocked(err)
	return err
}

func (c *etcdRaftConsensus) Provider() Provider {
	return ProviderEtcdRaft
}

func (c *etcdRaftConsensus) RuntimeStatus() RuntimeStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	replicas := c.replicas
	if replicas <= 0 {
		replicas = 1
	}
	mode := "raft_single_node"
	if replicas > 1 {
		mode = "raft_transport_foundation"
	}
	status := RuntimeStatus{
		Mode:          mode,
		Index:         c.index,
		Term:          c.term,
		SnapshotIndex: c.snapshotIndex,
		Replicas:      replicas,
		LastErrorCode: c.lastErrorCode,
		LastError:     c.lastError,
		LastErrorAt:   c.lastErrorAt,
	}
	if c.closed || c.rawNode == nil || c.storage == nil {
		status.Health = "unavailable"
		return status
	}
	lead := c.rawNode.Status().Lead
	status.LeaderKnown = lead != 0
	status.Leader = status.LeaderKnown && lead == c.nodeID
	status.WriteAvailable = status.Leader
	switch {
	case status.Leader:
		status.Health = "ready"
	case status.LeaderKnown:
		status.Health = "follower"
	default:
		status.Health = "no_leader"
	}
	return status
}

func (c *etcdRaftConsensus) SetCommittedEntryObserver(observer CommittedEntryObserver) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.observer = observer
	if observer == nil {
		return nil
	}
	for _, committed := range c.committed {
		if err := c.notifyCommittedEntryLocked(committed); err != nil {
			return err
		}
	}
	return nil
}

func (c *etcdRaftConsensus) SetStateSnapshotter(snapshotter StateSnapshotter) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshotter = snapshotter
	return nil
}

func (c *etcdRaftConsensus) SetStateSnapshotApplier(applier StateSnapshotApplier) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshotApplier = applier
	return nil
}

func (c *etcdRaftConsensus) Close() error {
	if c == nil {
		return nil
	}
	if c.stopCh == nil || c.doneCh == nil {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		return nil
	}
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		close(c.stopCh)
		<-c.doneCh
	})
	return nil
}

func (c *etcdRaftConsensus) startBackgroundLoop() {
	go func() {
		defer close(c.doneCh)
		initialDelay := time.Duration(c.nodeID%9+1) * (etcdRaftTickInterval / 10)
		timer := time.NewTimer(initialDelay)
		select {
		case <-c.stopCh:
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
		ticker := time.NewTicker(etcdRaftTickInterval)
		defer ticker.Stop()
		for {
			select {
			case <-c.stopCh:
				return
			case <-ticker.C:
				c.backgroundTick()
			}
		}
	}()
}

func (c *etcdRaftConsensus) backgroundTick() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.rawNode == nil || c.storage == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), etcdRaftApplyTimeout)
	defer cancel()
	c.rawNode.Tick()
	if err := c.advanceUntilStableLocked(ctx); err != nil {
		c.recordRuntimeErrorLocked(err)
	}
}

func (c *etcdRaftConsensus) commitMutation(
	ctx context.Context,
	proposal raftCommitProposal,
) (*pendingRaftProposal, error) {
	runCtx, cancel := withDefaultTimeout(ctx, etcdRaftApplyTimeout)
	defer cancel()
	if err := c.ensureLeader(runCtx); err != nil {
		return nil, err
	}

	c.mu.Lock()
	if c.closed || c.rawNode == nil || c.storage == nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("%w: etcd raft commit log is unavailable", ErrUnavailable)
	}
	c.proposalSeq++
	proposal.ID = c.proposalSeq
	payload, err := json.Marshal(proposal)
	if err != nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("marshal raft proposal: %w", err)
	}

	pending := &pendingRaftProposal{}
	c.pending[proposal.ID] = pending
	if err := c.rawNode.Propose(payload); err != nil {
		delete(c.pending, proposal.ID)
		c.mu.Unlock()
		return nil, fmt.Errorf("raft propose: %w", err)
	}
	c.mu.Unlock()

	for {
		c.mu.Lock()
		if c.closed {
			delete(c.pending, proposal.ID)
			c.mu.Unlock()
			return nil, fmt.Errorf("%w: etcd raft commit log is unavailable", ErrUnavailable)
		}
		if pending.done {
			c.mu.Unlock()
			return pending, nil
		}
		if err := c.advanceUntilStableLocked(runCtx); err != nil {
			delete(c.pending, proposal.ID)
			c.mu.Unlock()
			return nil, err
		}
		if !pending.done {
			c.rawNode.Tick()
		}
		c.mu.Unlock()
		select {
		case <-runCtx.Done():
			c.mu.Lock()
			delete(c.pending, proposal.ID)
			c.mu.Unlock()
			return nil, fmt.Errorf("%w: raft apply timeout: %w", ErrUnavailable, runCtx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}

func (c *etcdRaftConsensus) ensureLeader(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.rawNode == nil || c.storage == nil {
		return fmt.Errorf("%w: etcd raft commit log is unavailable", ErrUnavailable)
	}
	status := c.rawNode.Status()
	if status.Lead == c.nodeID {
		return nil
	}
	if status.Lead != 0 {
		return ErrNotLeader
	}
	if err := c.rawNode.Campaign(); err != nil {
		return fmt.Errorf("raft campaign: %w", err)
	}
	return c.waitForLeaderLocked(ctx)
}

func (c *etcdRaftConsensus) recordRuntimeError(err error) {
	if err == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recordRuntimeErrorLocked(err)
}

func (c *etcdRaftConsensus) recordRuntimeErrorLocked(err error) {
	if err == nil {
		return
	}
	c.lastErrorCode = runtimeErrorCode(err)
	c.lastError = err.Error()
	c.lastErrorAt = time.Now().UTC()
}

func runtimeErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrNotLeader):
		return "not_leader"
	case errors.Is(err, ErrUnavailable):
		return "unavailable"
	default:
		return "error"
	}
}

func (c *etcdRaftConsensus) waitForLeaderLocked(ctx context.Context) error {
	for {
		if c.rawNode.Status().Lead == c.nodeID {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: raft leader election timeout: %w", ErrUnavailable, ctx.Err())
		default:
		}
		if err := c.advanceUntilStableLocked(ctx); err != nil {
			return err
		}
		if c.rawNode.Status().Lead == c.nodeID {
			return nil
		}
		c.rawNode.Tick()
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			c.mu.Lock()
			return fmt.Errorf("%w: raft leader election timeout: %w", ErrUnavailable, ctx.Err())
		case <-time.After(time.Millisecond):
			c.mu.Lock()
		}
	}
}

func (c *etcdRaftConsensus) advanceUntilStableLocked(ctx context.Context) error {
	steps := 0
	for c.rawNode.HasReady() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if steps >= etcdRaftAdvanceMaxStep {
			return fmt.Errorf("raft did not reach stable state in %d steps", etcdRaftAdvanceMaxStep)
		}
		if err := c.advanceOneReadyLocked(ctx); err != nil {
			return err
		}
		steps++
	}
	return nil
}

func (c *etcdRaftConsensus) advanceOneReadyLocked(ctx context.Context) error {
	rd := c.rawNode.Ready()
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := c.applyIncomingSnapshotLocked(rd.Snapshot); err != nil {
			return err
		}
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		if err := c.storage.SetHardState(rd.HardState); err != nil {
			return fmt.Errorf("raft storage set hard state: %w", err)
		}
	}
	if err := c.storage.Append(rd.Entries); err != nil {
		return fmt.Errorf("raft storage append: %w", err)
	}
	if err := c.storage.Persist(); err != nil {
		return fmt.Errorf("raft storage persist: %w", err)
	}
	outbound := make([]raftpb.Message, 0, len(rd.Messages))
	for _, msg := range rd.Messages {
		if msg.To != c.nodeID {
			if msg.To != 0 {
				outbound = append(outbound, msg)
			}
			continue
		}
		if err := c.rawNode.Step(msg); err != nil {
			return fmt.Errorf("raft step self message: %w", err)
		}
	}
	if len(outbound) > 0 {
		if c.transport == nil {
			return fmt.Errorf("raft transport is not configured for peer messages")
		}
		sendCtx, cancel := withDefaultTimeout(ctx, etcdRaftSendTimeout)
		err := c.transport.Send(sendCtx, outbound)
		cancel()
		if err != nil {
			return fmt.Errorf("raft transport send: %w", err)
		}
	}
	for _, entry := range rd.CommittedEntries {
		if err := c.applyCommittedEntryLocked(entry); err != nil {
			return err
		}
	}
	if c.snapshotter == nil {
		if err := c.maybeSnapshotLocked(c.index); err != nil {
			return err
		}
	}
	c.rawNode.Advance(rd)
	return nil
}

func (c *etcdRaftConsensus) applyIncomingSnapshotLocked(snapshot raftpb.Snapshot) error {
	if err := c.storage.ApplySnapshot(snapshot); err != nil {
		return fmt.Errorf("raft storage apply snapshot: %w", err)
	}
	c.snapshotIndex = snapshot.Metadata.Index
	if snapshot.Metadata.Index > c.index {
		c.index = snapshot.Metadata.Index
	}
	c.updateReplicaCountLocked()
	if len(snapshot.Data) == 0 || c.snapshotApplier == nil {
		return nil
	}
	data := append([]byte(nil), snapshot.Data...)
	if err := c.snapshotApplier.ApplyStateSnapshot(snapshot.Metadata.Index, data); err != nil {
		return fmt.Errorf("apply raft state snapshot data: %w", err)
	}
	return nil
}

func (c *etcdRaftConsensus) applyCommittedEntryLocked(entry raftpb.Entry) error {
	if entry.Index > 0 {
		c.index = entry.Index
	}
	if entry.Term > 0 {
		c.term = entry.Term
	}
	switch entry.Type {
	case raftpb.EntryNormal:
		if len(entry.Data) == 0 {
			return nil
		}
		var proposal raftCommitProposal
		if err := json.Unmarshal(entry.Data, &proposal); err != nil {
			return fmt.Errorf("unmarshal raft proposal: %w", err)
		}
		committed, err := proposal.committedEntry(Commit{Index: entry.Index, Term: entry.Term})
		if err != nil {
			return err
		}
		c.committed = append(c.committed, committed)
		if pending, ok := c.pending[proposal.ID]; ok {
			pending.control = committed.Control
			pending.data = committed.Data
			pending.done = true
			delete(c.pending, proposal.ID)
			return nil
		}
		if err := c.notifyCommittedEntryLocked(committed); err != nil {
			return err
		}
		if c.snapshotter != nil {
			return c.maybeSnapshotLocked(entry.Index)
		}
		return nil
	case raftpb.EntryConfChange:
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(entry.Data); err != nil {
			return fmt.Errorf("unmarshal conf change: %w", err)
		}
		c.rawNode.ApplyConfChange(cc)
		c.updateReplicaCountLocked()
		return nil
	case raftpb.EntryConfChangeV2:
		var cc raftpb.ConfChangeV2
		if err := cc.Unmarshal(entry.Data); err != nil {
			return fmt.Errorf("unmarshal conf change v2: %w", err)
		}
		c.rawNode.ApplyConfChange(cc)
		c.updateReplicaCountLocked()
		return nil
	default:
		return nil
	}
}

func (c *etcdRaftConsensus) ObserveCommittedIndex(index uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.snapshotter == nil {
		return
	}
	if c.index > 0 && index > c.index {
		index = c.index
	}
	if err := c.maybeSnapshotLocked(index); err != nil {
		c.recordRuntimeErrorLocked(err)
	}
}

func (c *etcdRaftConsensus) maybeSnapshotLocked(appliedIndex uint64) error {
	policy := c.snapshotPolicy
	if policy.AppliedEntries == 0 || appliedIndex == 0 {
		return nil
	}
	if appliedIndex <= c.snapshotIndex || appliedIndex-c.snapshotIndex < policy.AppliedEntries {
		return nil
	}
	snapshotIndex := appliedIndex
	compactIndex := snapshotIndex
	var data []byte
	if c.snapshotter != nil {
		if policy.RetainEntries > 0 {
			if snapshotIndex <= policy.RetainEntries {
				return nil
			}
			compactIndex = snapshotIndex - policy.RetainEntries
		}
		payload, err := c.snapshotter.CaptureStateSnapshot(snapshotIndex)
		if err != nil {
			return fmt.Errorf("capture raft state snapshot: %w", err)
		}
		data = append([]byte(nil), payload...)
	} else if policy.RetainEntries > 0 {
		if appliedIndex <= policy.RetainEntries {
			return nil
		}
		snapshotIndex = appliedIndex - policy.RetainEntries
		compactIndex = snapshotIndex
	}
	if snapshotIndex <= c.snapshotIndex {
		return nil
	}
	confState := c.raftConfStateLocked()
	if _, err := c.storage.CreateSnapshot(snapshotIndex, &confState, data); err != nil {
		if errors.Is(err, raft.ErrSnapOutOfDate) {
			return nil
		}
		return fmt.Errorf("raft storage create snapshot: %w", err)
	}
	if err := c.storage.Compact(compactIndex); err != nil {
		if !errors.Is(err, raft.ErrCompacted) {
			return fmt.Errorf("raft storage compact snapshot: %w", err)
		}
	}
	if err := c.storage.Persist(); err != nil {
		return fmt.Errorf("raft storage persist snapshot: %w", err)
	}
	c.snapshotIndex = snapshotIndex
	return nil
}

func (c *etcdRaftConsensus) raftConfStateLocked() raftpb.ConfState {
	status := c.rawNode.Status()
	return raftpb.ConfState{
		Voters:         sortedRaftIDs(status.Config.Voters[0]),
		Learners:       sortedRaftIDs(status.Config.Learners),
		VotersOutgoing: sortedRaftIDs(status.Config.Voters[1]),
		LearnersNext:   sortedRaftIDs(status.Config.LearnersNext),
		AutoLeave:      status.Config.AutoLeave,
	}
}

func (c *etcdRaftConsensus) membershipChangeAppliedLocked(cc raftpb.ConfChange) bool {
	status := c.rawNode.Status()
	_, voter := status.Config.Voters[0][cc.NodeID]
	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		return voter
	case raftpb.ConfChangeRemoveNode:
		return !voter
	default:
		return false
	}
}

func (c *etcdRaftConsensus) updateReplicaCountLocked() {
	status := c.rawNode.Status()
	replicas := len(status.Config.Voters[0])
	if replicas == 0 {
		replicas = len(status.Config.Learners)
	}
	if replicas > 0 {
		c.replicas = replicas
	}
}

func sortedRaftIDs(in map[uint64]struct{}) []uint64 {
	if len(in) == 0 {
		return nil
	}
	out := make([]uint64, 0, len(in))
	for id := range in {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (c *etcdRaftConsensus) notifyCommittedEntryLocked(committed raftCommittedProposal) error {
	if c.observer == nil {
		return nil
	}
	if committed.Control != nil {
		return c.observer.ObserveCommittedControl(*committed.Control)
	}
	if committed.Data != nil {
		return c.observer.ObserveCommittedData(*committed.Data)
	}
	return nil
}

func (p raftCommitProposal) committedEntry(commit Commit) (raftCommittedProposal, error) {
	out := raftCommittedProposal{
		ID:   p.ID,
		Kind: p.Kind,
	}
	switch p.Kind {
	case "control":
		if p.Control == nil {
			return out, fmt.Errorf("raft control proposal missing mutation")
		}
		control := ControlCommittedEntry{
			Commit:   commit,
			Mutation: cloneControlMutation(*p.Control),
		}
		out.Control = &control
	case "data":
		if p.Data == nil {
			return out, fmt.Errorf("raft data proposal missing mutation")
		}
		data := DataCommittedEntry{
			Commit:   commit,
			Mutation: cloneDataMutation(*p.Data),
			Seq:      commit.Index,
		}
		out.Data = &data
	default:
		return out, fmt.Errorf("unknown raft proposal kind %q", p.Kind)
	}
	return out, nil
}

func stableRaftNodeID(nodeID string) uint64 {
	return raftid.StableNodeID(nodeID)
}

func resolveRaftPeerIDs(nodeName string, peers []string) ([]uint64, error) {
	if len(peers) == 0 {
		return []uint64{stableRaftNodeID(nodeName)}, nil
	}
	seenNames := make(map[string]struct{}, len(peers))
	peerNames := make([]string, 0, len(peers))
	localIncluded := false
	for _, raw := range peers {
		peer := strings.TrimSpace(raw)
		if peer == "" {
			continue
		}
		if _, ok := seenNames[peer]; ok {
			continue
		}
		seenNames[peer] = struct{}{}
		peerNames = append(peerNames, peer)
		if peer == nodeName {
			localIncluded = true
		}
	}
	if len(peerNames) == 0 {
		return nil, fmt.Errorf("raft peers must contain at least one node")
	}
	if !localIncluded {
		return nil, fmt.Errorf("raft peers must include local node %q", nodeName)
	}
	peerIDs := make([]uint64, 0, len(peerNames))
	seenIDs := make(map[uint64]string, len(peerNames))
	for _, peer := range peerNames {
		id := stableRaftNodeID(peer)
		if other, exists := seenIDs[id]; exists && other != peer {
			return nil, fmt.Errorf("raft peer id collision between %q and %q", other, peer)
		}
		seenIDs[id] = peer
		peerIDs = append(peerIDs, id)
	}
	return peerIDs, nil
}

func withDefaultTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		return context.WithTimeout(context.Background(), timeout)
	}
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func cloneControlMutation(in ControlMutation) ControlMutation {
	out := in
	out.Split = append([]byte(nil), in.Split...)
	return out
}

func cloneDataMutation(in DataMutation) DataMutation {
	out := in
	out.Key = append([]byte(nil), in.Key...)
	out.Value = append([]byte(nil), in.Value...)
	return out
}
