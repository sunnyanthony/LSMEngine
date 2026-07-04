package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// CommitLogProvider selects the commit-log backend.
type CommitLogProvider string

const (
	CommitLogProviderLocal    CommitLogProvider = "local"
	CommitLogProviderEtcdRaft CommitLogProvider = "etcd-raft"
)

// CommitLogOptions controls commit-log execution.
type CommitLogOptions struct {
	Provider  CommitLogProvider    `json:"provider" yaml:"provider"`
	Transport RaftMessageTransport `json:"-" yaml:"-"`
	Factory   CommitLogFactory     `json:"-" yaml:"-"`
}

// RaftMessageTransport sends raft protocol messages to peer nodes.
//
// This foundation is intentionally outbound-only; inbound delivery wiring is
// introduced in later branches.
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

type controlMutation = CommitLogControlMutation
type dataMutation = CommitLogDataMutation
type commitResult = CommitLogCommit
type controlCommittedEntry = CommitLogControlCommittedEntry
type dataCommittedEntry = CommitLogDataCommittedEntry
type commitLogConsensus = CommitLogConsensus

type commitLogIndexObserver interface {
	ObserveCommittedIndex(index uint64)
}

type localCommitLogConsensus struct {
	mu    sync.Mutex
	index uint64
	term  uint64
}

func newLocalCommitLogConsensus() *localCommitLogConsensus {
	return &localCommitLogConsensus{term: 1}
}

func (c *localCommitLogConsensus) CommitControl(_ context.Context, mutation controlMutation) (controlCommittedEntry, error) {
	commit := c.nextCommit()
	return controlCommittedEntry{
		Commit:   commit,
		Mutation: cloneControlMutation(mutation),
	}, nil
}

func (c *localCommitLogConsensus) CommitData(_ context.Context, mutation dataMutation) (dataCommittedEntry, error) {
	commit := c.nextCommit()
	return dataCommittedEntry{
		Commit:   commit,
		Mutation: cloneDataMutation(mutation),
		Seq:      commit.Index,
	}, nil
}

func (c *localCommitLogConsensus) nextCommit() commitResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.index++
	return commitResult{
		Index: c.index,
		Term:  c.term,
	}
}

func (c *localCommitLogConsensus) ObserveCommittedIndex(index uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if index > c.index {
		c.index = index
	}
}

func (c *localCommitLogConsensus) Provider() CommitLogProvider {
	return CommitLogProviderLocal
}

func (c *localCommitLogConsensus) RuntimeStatus() CommitLogRuntimeStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	return CommitLogRuntimeStatus{
		Mode:     "local",
		Index:    c.index,
		Term:     c.term,
		Leader:   true,
		Replicas: 1,
	}
}

type raftCommitProposal struct {
	ID      uint64           `json:"id"`
	Kind    string           `json:"kind"`
	Control *controlMutation `json:"control,omitempty"`
	Data    *dataMutation    `json:"data,omitempty"`
}

type raftCommittedProposal struct {
	ID      uint64
	Kind    string
	Control *controlCommittedEntry
	Data    *dataCommittedEntry
}

type pendingRaftProposal struct {
	done    bool
	control *controlCommittedEntry
	data    *dataCommittedEntry
	err     error
}

type etcdRaftCommitLogConsensus struct {
	mu sync.Mutex

	nodeID      uint64
	rawNode     *raft.RawNode
	storage     *raft.MemoryStorage
	transport   RaftMessageTransport
	proposalSeq uint64
	pending     map[uint64]*pendingRaftProposal
	committed   []raftCommittedProposal
	index       uint64
	term        uint64
	replicas    int
}

const (
	etcdRaftElectionTick   = 10
	etcdRaftHeartbeatTick  = 1
	etcdRaftApplyTimeout   = 5 * time.Second
	etcdRaftAdvanceMaxStep = 2048
	etcdRaftSendTimeout    = 2 * time.Second
)

func newEtcdRaftCommitLogConsensus(opts Options) (*etcdRaftCommitLogConsensus, error) {
	nodeName := strings.TrimSpace(opts.NodeID)
	if nodeName == "" {
		nodeName = "node-0"
	}
	nodeID := stableRaftNodeID(nodeName)
	peerIDs, err := resolveRaftPeerIDs(nodeName, opts.Raft)
	if err != nil {
		return nil, err
	}
	var transport RaftMessageTransport
	if opts.CommitLog != nil {
		transport = opts.CommitLog.Transport
	}
	if len(peerIDs) > 1 && transport == nil {
		return nil, fmt.Errorf("raft transport is required when raft peers > 1")
	}
	storage := raft.NewMemoryStorage()
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
	bootstrapPeers := make([]raft.Peer, 0, len(peerIDs))
	for _, id := range peerIDs {
		bootstrapPeers = append(bootstrapPeers, raft.Peer{ID: id})
	}
	if err := rawNode.Bootstrap(bootstrapPeers); err != nil {
		return nil, fmt.Errorf("bootstrap etcd raft node: %w", err)
	}
	c := &etcdRaftCommitLogConsensus{
		nodeID:    nodeID,
		rawNode:   rawNode,
		storage:   storage,
		transport: transport,
		pending:   make(map[uint64]*pendingRaftProposal),
		replicas:  len(peerIDs),
	}
	ctx, cancel := context.WithTimeout(context.Background(), etcdRaftApplyTimeout)
	defer cancel()
	if err := c.advanceUntilStableLocked(ctx); err != nil {
		return nil, err
	}
	// Multi-peer clusters may not have enough live peers yet during startup.
	// We only force immediate self-election in cluster-of-one mode.
	if len(peerIDs) == 1 {
		if err := c.ensureLeaderLocked(ctx); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (c *etcdRaftCommitLogConsensus) CommitControl(ctx context.Context, mutation controlMutation) (controlCommittedEntry, error) {
	cloned := cloneControlMutation(mutation)
	pending, err := c.commitMutation(ctx, raftCommitProposal{
		Kind:    "control",
		Control: &cloned,
	})
	if err != nil {
		return controlCommittedEntry{}, err
	}
	if pending.control == nil {
		return controlCommittedEntry{}, fmt.Errorf("raft committed non-control entry")
	}
	return *pending.control, pending.err
}

func (c *etcdRaftCommitLogConsensus) CommitData(ctx context.Context, mutation dataMutation) (dataCommittedEntry, error) {
	cloned := cloneDataMutation(mutation)
	pending, err := c.commitMutation(ctx, raftCommitProposal{
		Kind: "data",
		Data: &cloned,
	})
	if err != nil {
		return dataCommittedEntry{}, err
	}
	if pending.data == nil {
		return dataCommittedEntry{}, fmt.Errorf("raft committed non-data entry")
	}
	return *pending.data, pending.err
}

func (c *etcdRaftCommitLogConsensus) Provider() CommitLogProvider {
	return CommitLogProviderEtcdRaft
}

func (c *etcdRaftCommitLogConsensus) RuntimeStatus() CommitLogRuntimeStatus {
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
	status := CommitLogRuntimeStatus{
		Mode:     mode,
		Index:    c.index,
		Term:     c.term,
		Replicas: replicas,
	}
	if c.rawNode == nil {
		return status
	}
	lead := c.rawNode.Status().Lead
	status.Leader = lead != 0 && lead == c.nodeID
	return status
}

func (c *etcdRaftCommitLogConsensus) commitMutation(
	ctx context.Context,
	proposal raftCommitProposal,
) (*pendingRaftProposal, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rawNode == nil || c.storage == nil {
		return nil, fmt.Errorf("etcd raft commit log is unavailable")
	}

	runCtx, cancel := withDefaultTimeout(ctx, etcdRaftApplyTimeout)
	defer cancel()
	if err := c.ensureLeaderLocked(runCtx); err != nil {
		return nil, err
	}

	c.proposalSeq++
	proposal.ID = c.proposalSeq
	payload, err := json.Marshal(proposal)
	if err != nil {
		return nil, fmt.Errorf("marshal raft proposal: %w", err)
	}

	pending := &pendingRaftProposal{}
	c.pending[proposal.ID] = pending
	if err := c.rawNode.Propose(payload); err != nil {
		delete(c.pending, proposal.ID)
		return nil, fmt.Errorf("raft propose: %w", err)
	}

	for {
		if pending.done {
			return pending, nil
		}
		select {
		case <-runCtx.Done():
			delete(c.pending, proposal.ID)
			return nil, fmt.Errorf("raft apply timeout: %w", runCtx.Err())
		default:
		}
		if err := c.advanceUntilStableLocked(runCtx); err != nil {
			delete(c.pending, proposal.ID)
			return nil, err
		}
		if !pending.done {
			c.rawNode.Tick()
		}
	}
}

func (c *etcdRaftCommitLogConsensus) ensureLeaderLocked(ctx context.Context) error {
	if c.rawNode.Status().Lead == c.nodeID {
		return nil
	}
	if err := c.rawNode.Campaign(); err != nil {
		return fmt.Errorf("raft campaign: %w", err)
	}
	for {
		if c.rawNode.Status().Lead == c.nodeID {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("raft leader election timeout: %w", ctx.Err())
		default:
		}
		if err := c.advanceUntilStableLocked(ctx); err != nil {
			return err
		}
		c.rawNode.Tick()
	}
}

func (c *etcdRaftCommitLogConsensus) advanceUntilStableLocked(ctx context.Context) error {
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

func (c *etcdRaftCommitLogConsensus) advanceOneReadyLocked(ctx context.Context) error {
	rd := c.rawNode.Ready()
	if !raft.IsEmptyHardState(rd.HardState) {
		if err := c.storage.SetHardState(rd.HardState); err != nil {
			return fmt.Errorf("raft storage set hard state: %w", err)
		}
	}
	if err := c.storage.Append(rd.Entries); err != nil {
		return fmt.Errorf("raft storage append: %w", err)
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
	c.rawNode.Advance(rd)
	return nil
}

func (c *etcdRaftCommitLogConsensus) applyCommittedEntryLocked(entry raftpb.Entry) error {
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
		committed, err := proposal.committedEntry(commitResult{Index: entry.Index, Term: entry.Term})
		if err != nil {
			return err
		}
		c.committed = append(c.committed, committed)
		if pending, ok := c.pending[proposal.ID]; ok {
			pending.control = committed.Control
			pending.data = committed.Data
			pending.done = true
			delete(c.pending, proposal.ID)
		}
		return nil
	case raftpb.EntryConfChange:
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(entry.Data); err != nil {
			return fmt.Errorf("unmarshal conf change: %w", err)
		}
		c.rawNode.ApplyConfChange(cc)
		return nil
	case raftpb.EntryConfChangeV2:
		var cc raftpb.ConfChangeV2
		if err := cc.Unmarshal(entry.Data); err != nil {
			return fmt.Errorf("unmarshal conf change v2: %w", err)
		}
		c.rawNode.ApplyConfChange(cc)
		return nil
	default:
		return nil
	}
}

func (p raftCommitProposal) committedEntry(commit commitResult) (raftCommittedProposal, error) {
	out := raftCommittedProposal{
		ID:   p.ID,
		Kind: p.Kind,
	}
	switch p.Kind {
	case "control":
		if p.Control == nil {
			return out, fmt.Errorf("raft control proposal missing mutation")
		}
		control := controlCommittedEntry{
			Commit:   commit,
			Mutation: cloneControlMutation(*p.Control),
		}
		out.Control = &control
	case "data":
		if p.Data == nil {
			return out, fmt.Errorf("raft data proposal missing mutation")
		}
		data := dataCommittedEntry{
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
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(nodeID))
	id := hasher.Sum64()
	if id == 0 {
		return 1
	}
	return id
}

func resolveRaftPeerIDs(nodeName string, raftOpts *RaftOptions) ([]uint64, error) {
	if raftOpts == nil || len(raftOpts.Peers) == 0 {
		return []uint64{stableRaftNodeID(nodeName)}, nil
	}
	seenNames := make(map[string]struct{}, len(raftOpts.Peers))
	peerNames := make([]string, 0, len(raftOpts.Peers))
	localIncluded := false
	for _, raw := range raftOpts.Peers {
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

func newCommitLogConsensus(opts Options) (commitLogConsensus, error) {
	if opts.CommitLog != nil && opts.CommitLog.Factory != nil {
		consensus, err := opts.CommitLog.Factory.New(opts)
		if err != nil {
			return nil, fmt.Errorf("build commit log from factory: %w", err)
		}
		if consensus == nil {
			return nil, fmt.Errorf("build commit log from factory: nil consensus")
		}
		return consensus, nil
	}
	provider := CommitLogProviderLocal
	if opts.CommitLog != nil {
		if trimmed := strings.TrimSpace(string(opts.CommitLog.Provider)); trimmed != "" {
			provider = CommitLogProvider(trimmed)
		}
	}
	switch provider {
	case CommitLogProviderLocal:
		return newLocalCommitLogConsensus(), nil
	case CommitLogProviderEtcdRaft:
		return newEtcdRaftCommitLogConsensus(opts)
	default:
		return nil, fmt.Errorf("unknown commit log provider %q", provider)
	}
}

func cloneControlMutation(in controlMutation) controlMutation {
	out := in
	out.Split = append([]byte(nil), in.Split...)
	return out
}

func cloneDataMutation(in dataMutation) dataMutation {
	out := in
	out.Key = append([]byte(nil), in.Key...)
	out.Value = append([]byte(nil), in.Value...)
	return out
}
