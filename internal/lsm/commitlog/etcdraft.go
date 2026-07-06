package commitlog

import (
	"context"
	"encoding/json"
	"fmt"
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

	nodeID      uint64
	rawNode     *raft.RawNode
	storage     *raftPersistentStorage
	transport   RaftMessageTransport
	observer    CommittedEntryObserver
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
	if !loadedLog {
		bootstrapPeers := make([]raft.Peer, 0, len(peerIDs))
		for _, id := range peerIDs {
			bootstrapPeers = append(bootstrapPeers, raft.Peer{ID: id})
		}
		if err := rawNode.Bootstrap(bootstrapPeers); err != nil {
			return nil, fmt.Errorf("bootstrap etcd raft node: %w", err)
		}
	}
	c := &etcdRaftConsensus{
		nodeID:    nodeID,
		rawNode:   rawNode,
		storage:   storage,
		transport: transport,
		pending:   make(map[uint64]*pendingRaftProposal),
		replicas:  len(peerIDs),
	}
	if hardState, _, err := storage.InitialState(); err == nil {
		c.index = hardState.Commit
		c.term = hardState.Term
	} else {
		return nil, fmt.Errorf("read raft restored state: %w", err)
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

func (c *etcdRaftConsensus) CommitControl(ctx context.Context, mutation ControlMutation) (ControlCommittedEntry, error) {
	cloned := cloneControlMutation(mutation)
	pending, err := c.commitMutation(ctx, raftCommitProposal{
		Kind:    "control",
		Control: &cloned,
	})
	if err != nil {
		return ControlCommittedEntry{}, err
	}
	if pending.control == nil {
		return ControlCommittedEntry{}, fmt.Errorf("raft committed non-control entry")
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
		return DataCommittedEntry{}, err
	}
	if pending.data == nil {
		return DataCommittedEntry{}, fmt.Errorf("raft committed non-data entry")
	}
	return *pending.data, pending.err
}

func (c *etcdRaftConsensus) HandlePeerMessages(ctx context.Context, messages []raftpb.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rawNode == nil || c.storage == nil {
		return fmt.Errorf("etcd raft commit log is unavailable")
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
			return fmt.Errorf("raft step peer message: %w", err)
		}
		if err := c.advanceUntilStableLocked(runCtx); err != nil {
			return err
		}
	}
	return nil
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

func (c *etcdRaftConsensus) commitMutation(
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

func (c *etcdRaftConsensus) ensureLeaderLocked(ctx context.Context) error {
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
	c.rawNode.Advance(rd)
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
		return c.notifyCommittedEntryLocked(committed)
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
