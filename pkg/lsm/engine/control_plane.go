package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"lsmengine/internal/lsm/iofs"
	"lsmengine/pkg/lsm/errs"
)

// StorageMode describes how node-local data is persisted.
type StorageMode string

const (
	StorageModeLocal StorageMode = "local"
	StorageModePVC   StorageMode = "pvc"
)

// RaftOptions captures control-plane raft settings for shard groups.
type RaftOptions struct {
	Replicas          int           `json:"replicas" yaml:"replicas"`
	ElectionTimeout   time.Duration `json:"election_timeout,omitempty" yaml:"election_timeout,omitempty"`
	HeartbeatInterval time.Duration `json:"heartbeat_interval,omitempty" yaml:"heartbeat_interval,omitempty"`
	Peers             []string      `json:"peers,omitempty" yaml:"peers,omitempty"`
	Join              bool          `json:"join,omitempty" yaml:"join,omitempty"`
}

// ShardConfig defines a fixed shard range and replica group.
type ShardConfig struct {
	ID       string   `json:"id" yaml:"id"`
	StartKey []byte   `json:"start_key,omitempty" yaml:"start_key,omitempty"`
	EndKey   []byte   `json:"end_key,omitempty" yaml:"end_key,omitempty"`
	Replicas []string `json:"replicas,omitempty" yaml:"replicas,omitempty"`
	Leader   string   `json:"leader,omitempty" yaml:"leader,omitempty"`
}

// ReplicaStatus is runtime status for one shard replica.
type ReplicaStatus struct {
	NodeID  string `json:"node_id"`
	Role    string `json:"role"`
	Healthy bool   `json:"healthy"`
}

// ShardStatus is runtime status for one shard.
type ShardStatus struct {
	ID       string          `json:"id"`
	StartKey []byte          `json:"start_key,omitempty"`
	EndKey   []byte          `json:"end_key,omitempty"`
	Leader   string          `json:"leader"`
	Replicas []ReplicaStatus `json:"replicas"`
}

// ClusterStatus is node-level control-plane status.
type ClusterStatus struct {
	NodeID           string                 `json:"node_id"`
	ClusterID        string                 `json:"cluster_id"`
	StorageMode      StorageMode            `json:"storage_mode"`
	ShardCount       int                    `json:"shard_count"`
	Draining         bool                   `json:"draining"`
	Revision         uint64                 `json:"revision"`
	CommitLog        string                 `json:"commit_log"`
	CommitLogRuntime CommitLogRuntimeStatus `json:"commit_log_runtime"`
	Raft             RaftOptions            `json:"raft"`
}

// ControlWriteOptions carries optional optimistic concurrency and idempotency inputs.
type ControlWriteOptions struct {
	OperationID      string  `json:"operation_id,omitempty"`
	ExpectedRevision *uint64 `json:"expected_revision,omitempty"`
}

type appliedOperationState struct {
	ID          string `json:"id"`
	Fingerprint string `json:"fingerprint"`
	Revision    uint64 `json:"revision"`
}

type controlPlaneState struct {
	Version               int                     `json:"version"`
	NodeID                string                  `json:"node_id"`
	ClusterID             string                  `json:"cluster_id"`
	StorageMode           StorageMode             `json:"storage_mode"`
	Raft                  RaftOptions             `json:"raft"`
	Draining              bool                    `json:"draining"`
	Revision              uint64                  `json:"revision"`
	CommitLogAppliedIndex uint64                  `json:"commit_log_applied_index,omitempty"`
	Order                 []string                `json:"order"`
	Shards                []ShardStatus           `json:"shards"`
	AppliedOps            []appliedOperationState `json:"applied_ops,omitempty"`
}

type shardRoute struct {
	id    string
	start []byte
	end   []byte
}

type controlPlane struct {
	mu                    sync.RWMutex
	nodeID                string
	clusterID             string
	storageMode           StorageMode
	raft                  RaftOptions
	draining              bool
	revision              uint64
	commitLogAppliedIndex uint64
	order                 []string
	shards                map[string]ShardStatus
	routes                []shardRoute
	appliedOps            map[string]appliedOperationState
	appliedOrder          []string

	fs        iofs.FS
	statePath string
	consensus commitLogConsensus
}

const maxAppliedControlOps = 256

var errControlNoop = errors.New("control noop")

func newControlPlane(opts Options) (*controlPlane, error) {
	nodeID := strings.TrimSpace(opts.NodeID)
	if nodeID == "" {
		nodeID = "node-0"
	}
	clusterID := strings.TrimSpace(opts.ClusterID)
	if clusterID == "" {
		clusterID = "cluster-local"
	}
	mode := StorageMode(strings.TrimSpace(opts.StorageMode))
	if mode == "" {
		mode = StorageModeLocal
	}
	switch mode {
	case StorageModeLocal, StorageModePVC:
	default:
		return nil, fmt.Errorf("unknown storage mode %q", mode)
	}

	raft := RaftOptions{Replicas: 3}
	if opts.Raft != nil {
		raft = *opts.Raft
	}
	if raft.Replicas <= 0 {
		raft.Replicas = 3
	}

	fs := opts.IOFS
	if fs == nil {
		fs = iofs.OSFS{}
	}
	statePath := opts.ControlStatePath
	if statePath == "" {
		statePath = filepath.Join(opts.DataDir, "control_state.json")
	}
	consensus, err := newCommitLogConsensus(opts)
	if err != nil {
		return nil, err
	}
	c := &controlPlane{
		nodeID:       nodeID,
		clusterID:    clusterID,
		storageMode:  mode,
		raft:         raft,
		revision:     0,
		order:        nil,
		shards:       make(map[string]ShardStatus),
		routes:       nil,
		appliedOps:   make(map[string]appliedOperationState),
		appliedOrder: nil,
		fs:           fs,
		statePath:    statePath,
		consensus:    consensus,
	}

	if err := c.loadOrBootstrap(opts.ShardMap); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *controlPlane) loadOrBootstrap(shardMap []ShardConfig) error {
	state, err := c.loadState()
	if err == nil && state != nil {
		if state.NodeID != c.nodeID || state.ClusterID != c.clusterID {
			return fmt.Errorf("control state identity mismatch: file=%s/%s current=%s/%s",
				state.ClusterID, state.NodeID, c.clusterID, c.nodeID)
		}
		if err := c.applyState(*state); err != nil {
			return fmt.Errorf("apply control state: %w", err)
		}
		return nil
	}
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("load control state: %w", err)
	}

	if len(shardMap) == 0 {
		shard := ShardStatus{
			ID:     "default",
			Leader: c.nodeID,
			Replicas: []ReplicaStatus{
				{NodeID: c.nodeID, Role: "leader", Healthy: true},
			},
		}
		c.order = []string{shard.ID}
		c.shards[shard.ID] = shard
		if err := c.rebuildRoutesLocked(); err != nil {
			return err
		}
		return c.save()
	}

	c.order = make([]string, 0, len(shardMap))
	for _, cfg := range shardMap {
		id := strings.TrimSpace(cfg.ID)
		if id == "" {
			return fmt.Errorf("shard id is required")
		}
		if _, exists := c.shards[id]; exists {
			return fmt.Errorf("duplicate shard id %q", id)
		}
		replicas := append([]string(nil), cfg.Replicas...)
		if len(replicas) == 0 {
			replicas = []string{c.nodeID}
		}
		leader := strings.TrimSpace(cfg.Leader)
		if leader == "" {
			leader = replicas[0]
		}
		if !slices.Contains(replicas, leader) {
			replicas = append(replicas, leader)
		}
		shard := ShardStatus{
			ID:       id,
			StartKey: append([]byte(nil), cfg.StartKey...),
			EndKey:   append([]byte(nil), cfg.EndKey...),
			Leader:   leader,
			Replicas: make([]ReplicaStatus, 0, len(replicas)),
		}
		for _, replica := range replicas {
			role := "follower"
			if replica == leader {
				role = "leader"
			}
			shard.Replicas = append(shard.Replicas, ReplicaStatus{
				NodeID:  replica,
				Role:    role,
				Healthy: true,
			})
		}
		c.order = append(c.order, id)
		c.shards[id] = shard
	}
	if err := c.rebuildRoutesLocked(); err != nil {
		return err
	}
	return c.save()
}

func (c *controlPlane) status() ClusterStatus {
	if c == nil {
		return ClusterStatus{}
	}
	c.mu.RLock()
	status := ClusterStatus{
		NodeID:      c.nodeID,
		ClusterID:   c.clusterID,
		StorageMode: c.storageMode,
		ShardCount:  len(c.order),
		Draining:    c.draining,
		Revision:    c.revision,
		Raft:        c.raft,
	}
	consensus := c.consensus
	c.mu.RUnlock()

	status.CommitLog = string(CommitLogProviderLocal)
	if consensus != nil {
		status.CommitLog = string(consensus.Provider())
		status.CommitLogRuntime = consensus.RuntimeStatus()
	}
	return status
}

func (c *controlPlane) shardsSnapshot() []ShardStatus {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]ShardStatus, 0, len(c.order))
	for _, id := range c.order {
		shard := c.shards[id]
		shard.StartKey = append([]byte(nil), shard.StartKey...)
		shard.EndKey = append([]byte(nil), shard.EndKey...)
		shard.Replicas = append([]ReplicaStatus(nil), shard.Replicas...)
		out = append(out, shard)
	}
	return out
}

func (c *controlPlane) allowWrite(key []byte) error {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.draining {
		return errs.ErrBackpressure
	}
	shard, ok := c.shardForKeyLocked(key)
	if !ok {
		return errs.ErrShardNotFound
	}
	if shard.Leader != c.nodeID {
		return errs.ErrNotLeader
	}
	return nil
}

func (c *controlPlane) shardIDForKey(key []byte) (string, bool) {
	if c == nil {
		return "", false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	shard, ok := c.shardForKeyLocked(key)
	if !ok {
		return "", false
	}
	return shard.ID, true
}

func (c *controlPlane) transferLeader(shardID, target string) error {
	return c.transferLeaderWithOptions(shardID, target, ControlWriteOptions{})
}

func (c *controlPlane) transferLeaderWithOptions(shardID, target string, opts ControlWriteOptions) error {
	if c == nil {
		return errs.ErrShardNotFound
	}
	shardID = strings.TrimSpace(shardID)
	target = strings.TrimSpace(target)
	if shardID == "" || target == "" {
		return errs.ErrShardNotFound
	}
	fingerprint := transferLeaderFingerprint(shardID, target)
	return c.applyControlMutation(
		controlMutation{
			Kind:    "transfer-leader",
			ShardID: shardID,
			Target:  target,
		},
		opts,
		fingerprint,
		func(mutation controlMutation) error {
			if mutation.Kind != "transfer-leader" || mutation.ShardID != shardID || mutation.Target != target {
				return fmt.Errorf("committed control mutation mismatch")
			}
			shard, ok := c.shards[shardID]
			if !ok {
				return errs.ErrShardNotFound
			}
			if !hasReplica(shard.Replicas, target) {
				shard.Replicas = append(shard.Replicas, ReplicaStatus{
					NodeID:  target,
					Role:    "follower",
					Healthy: true,
				})
			}
			shard.Leader = target
			for i := range shard.Replicas {
				if shard.Replicas[i].NodeID == target {
					shard.Replicas[i].Role = "leader"
					continue
				}
				shard.Replicas[i].Role = "follower"
			}
			c.shards[shardID] = shard
			return nil
		},
	)
}

func (c *controlPlane) triggerSplit(shardID string, splitKey []byte) error {
	return c.triggerSplitWithOptions(shardID, splitKey, ControlWriteOptions{})
}

func (c *controlPlane) addReplica(shardID, target string) error {
	return c.addReplicaWithOptions(shardID, target, ControlWriteOptions{})
}

func (c *controlPlane) addReplicaWithOptions(shardID, target string, opts ControlWriteOptions) error {
	if c == nil {
		return errs.ErrShardNotFound
	}
	shardID = strings.TrimSpace(shardID)
	target = strings.TrimSpace(target)
	if shardID == "" || target == "" {
		return errs.ErrShardNotFound
	}
	fingerprint := addReplicaFingerprint(shardID, target)
	return c.applyControlMutation(
		controlMutation{
			Kind:    "add-replica",
			ShardID: shardID,
			Target:  target,
		},
		opts,
		fingerprint,
		func(mutation controlMutation) error {
			if mutation.Kind != "add-replica" || mutation.ShardID != shardID || mutation.Target != target {
				return fmt.Errorf("committed control mutation mismatch")
			}
			return c.applyAddReplicaMutationLocked(mutation)
		},
	)
}

func (c *controlPlane) removeReplica(shardID, target string) error {
	return c.removeReplicaWithOptions(shardID, target, ControlWriteOptions{})
}

func (c *controlPlane) removeReplicaWithOptions(shardID, target string, opts ControlWriteOptions) error {
	if c == nil {
		return errs.ErrShardNotFound
	}
	shardID = strings.TrimSpace(shardID)
	target = strings.TrimSpace(target)
	if shardID == "" || target == "" {
		return errs.ErrShardNotFound
	}
	fingerprint := removeReplicaFingerprint(shardID, target)
	return c.applyControlMutation(
		controlMutation{
			Kind:    "remove-replica",
			ShardID: shardID,
			Target:  target,
		},
		opts,
		fingerprint,
		func(mutation controlMutation) error {
			if mutation.Kind != "remove-replica" || mutation.ShardID != shardID || mutation.Target != target {
				return fmt.Errorf("committed control mutation mismatch")
			}
			return c.applyRemoveReplicaMutationLocked(mutation)
		},
	)
}

func (c *controlPlane) triggerSplitWithOptions(shardID string, splitKey []byte, opts ControlWriteOptions) error {
	if c == nil {
		return errs.ErrShardNotFound
	}
	shardID = strings.TrimSpace(shardID)
	if shardID == "" {
		return errs.ErrShardNotFound
	}
	fingerprint := splitFingerprint(shardID, splitKey)
	return c.applyControlMutation(
		controlMutation{
			Kind:    "split",
			ShardID: shardID,
			Split:   append([]byte(nil), splitKey...),
		},
		opts,
		fingerprint,
		func(mutation controlMutation) error {
			if mutation.Kind != "split" || mutation.ShardID != shardID || !bytes.Equal(mutation.Split, splitKey) {
				return fmt.Errorf("committed control mutation mismatch")
			}
			shard, ok := c.shards[shardID]
			if !ok {
				return errs.ErrShardNotFound
			}
			if !keyInRange(splitKey, shard.StartKey, shard.EndKey) {
				return fmt.Errorf("split key outside shard range")
			}
			if (len(shard.StartKey) > 0 && bytes.Equal(splitKey, shard.StartKey)) ||
				(len(shard.EndKey) > 0 && bytes.Equal(splitKey, shard.EndKey)) {
				return fmt.Errorf("split key must be inside range")
			}

			left := shard
			right := shard
			left.ID = c.uniqueShardID(shardID + "-a")
			right.ID = c.uniqueShardID(shardID + "-b")
			left.EndKey = append([]byte(nil), splitKey...)
			right.StartKey = append([]byte(nil), splitKey...)
			delete(c.shards, shardID)
			c.shards[left.ID] = left
			c.shards[right.ID] = right

			nextOrder := make([]string, 0, len(c.order)+1)
			for _, id := range c.order {
				if id == shardID {
					nextOrder = append(nextOrder, left.ID, right.ID)
					continue
				}
				nextOrder = append(nextOrder, id)
			}
			c.order = nextOrder
			return c.rebuildRoutesLocked()
		},
	)
}

func (c *controlPlane) triggerRebalance(shardID, target string) error {
	return c.triggerRebalanceWithOptions(shardID, target, ControlWriteOptions{})
}

func (c *controlPlane) triggerRebalanceWithOptions(shardID, target string, opts ControlWriteOptions) error {
	return c.transferLeaderWithOptions(shardID, target, opts)
}

func (c *controlPlane) prepareDrain(nodeID string) error {
	return c.prepareDrainWithOptions(nodeID, ControlWriteOptions{})
}

func (c *controlPlane) prepareDrainWithOptions(nodeID string, opts ControlWriteOptions) error {
	if c == nil {
		return errs.ErrShardNotFound
	}
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		nodeID = c.nodeID
	}
	if nodeID != c.nodeID {
		return fmt.Errorf("drain supported only for local node")
	}
	fingerprint := prepareDrainFingerprint(nodeID)
	return c.applyControlMutation(
		controlMutation{
			Kind:   "prepare-drain",
			NodeID: nodeID,
		},
		opts,
		fingerprint,
		func(mutation controlMutation) error {
			if mutation.Kind != "prepare-drain" || mutation.NodeID != nodeID {
				return fmt.Errorf("committed control mutation mismatch")
			}
			targets := make(map[string]string)
			for _, id := range c.order {
				shard := c.shards[id]
				if shard.Leader != nodeID {
					continue
				}
				target := ""
				for _, replica := range shard.Replicas {
					if replica.NodeID != nodeID && replica.Healthy {
						target = replica.NodeID
						break
					}
				}
				if target == "" {
					return fmt.Errorf("cannot drain: shard %q has no alternate healthy replica", id)
				}
				targets[id] = target
			}
			for _, id := range c.order {
				target, ok := targets[id]
				if !ok {
					continue
				}
				shard := c.shards[id]
				shard.Leader = target
				for i := range shard.Replicas {
					if shard.Replicas[i].NodeID == target {
						shard.Replicas[i].Role = "leader"
						continue
					}
					shard.Replicas[i].Role = "follower"
				}
				c.shards[id] = shard
			}
			c.draining = true
			return nil
		},
	)
}

func (c *controlPlane) shardForKeyLocked(key []byte) (ShardStatus, bool) {
	if len(c.routes) == 0 {
		return ShardStatus{}, false
	}
	idx := sort.Search(len(c.routes), func(i int) bool {
		end := c.routes[i].end
		return len(end) == 0 || bytes.Compare(key, end) < 0
	})
	if idx >= len(c.routes) {
		return ShardStatus{}, false
	}
	route := c.routes[idx]
	if len(route.start) > 0 && bytes.Compare(key, route.start) < 0 {
		return ShardStatus{}, false
	}
	shard, ok := c.shards[route.id]
	return shard, ok
}

func (c *controlPlane) uniqueShardID(base string) string {
	if _, exists := c.shards[base]; !exists {
		return base
	}
	for i := 2; ; i++ {
		candidate := fmt.Sprintf("%s-%d", base, i)
		if _, exists := c.shards[candidate]; !exists {
			return candidate
		}
	}
}

func (c *controlPlane) checkOperationPreconditions(opts ControlWriteOptions, fingerprint string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.checkOperationPreconditionsLocked(opts, fingerprint)
}

func (c *controlPlane) applyControlMutation(
	mutation controlMutation,
	opts ControlWriteOptions,
	fingerprint string,
	mutate func(controlMutation) error,
) error {
	if c == nil {
		return errs.ErrShardNotFound
	}
	if c.consensus == nil {
		return fmt.Errorf("commit log consensus unavailable")
	}
	applied, err := c.checkOperationPreconditions(opts, fingerprint)
	if err != nil || applied {
		return err
	}
	entry, err := c.consensus.CommitControl(context.Background(), mutation)
	if err != nil {
		return err
	}
	err = c.applyCommittedControlMutation(entry, opts, fingerprint, mutate)
	if errors.Is(err, errControlNoop) {
		return nil
	}
	if err == nil {
		if observer, ok := c.consensus.(commitLogIndexObserver); ok {
			observer.ObserveCommittedIndex(entry.Commit.Index)
		}
	}
	return err
}

func (c *controlPlane) applyCommittedControlMutation(
	entry controlCommittedEntry,
	opts ControlWriteOptions,
	fingerprint string,
	mutate func(controlMutation) error,
) error {
	if entry.Commit.Index == 0 || entry.Commit.Term == 0 {
		return fmt.Errorf("invalid committed control entry")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	appliedLocked, err := c.checkOperationPreconditionsLocked(opts, fingerprint)
	if err != nil {
		return err
	}
	if appliedLocked {
		return errControlNoop
	}
	previous := c.snapshotStateLocked()
	if err := mutate(entry.Mutation); err != nil {
		_ = c.applyState(previous)
		return err
	}
	c.markCommitLogAppliedLocked(entry.Commit.Index)
	c.noteOperationAppliedLocked(opts, fingerprint)
	return c.saveLockedWithRollback(previous)
}

func (c *controlPlane) applyReplicatedControlEntry(entry controlCommittedEntry) error {
	if c == nil {
		return errs.ErrShardNotFound
	}
	if entry.Commit.Index == 0 || entry.Commit.Term == 0 {
		return fmt.Errorf("invalid committed control entry")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry.Commit.Index <= c.commitLogAppliedIndex {
		return nil
	}
	previous := c.snapshotStateLocked()
	if err := c.applyControlMutationPayloadLocked(entry.Mutation); err != nil {
		_ = c.applyState(previous)
		return err
	}
	c.markCommitLogAppliedLocked(entry.Commit.Index)
	c.revision++
	return c.saveLockedWithRollback(previous)
}

func (c *controlPlane) applyControlMutationPayloadLocked(mutation controlMutation) error {
	switch mutation.Kind {
	case "transfer-leader":
		return c.applyTransferLeaderMutationLocked(mutation)
	case "add-replica":
		return c.applyAddReplicaMutationLocked(mutation)
	case "remove-replica":
		return c.applyRemoveReplicaMutationLocked(mutation)
	case "split":
		return c.applySplitMutationLocked(mutation)
	case "prepare-drain":
		return c.applyPrepareDrainMutationLocked(mutation)
	default:
		return fmt.Errorf("unknown control mutation kind %q", mutation.Kind)
	}
}

func (c *controlPlane) applyTransferLeaderMutationLocked(mutation controlMutation) error {
	shardID := strings.TrimSpace(mutation.ShardID)
	target := strings.TrimSpace(mutation.Target)
	if shardID == "" || target == "" {
		return errs.ErrShardNotFound
	}
	shard, ok := c.shards[shardID]
	if !ok {
		return errs.ErrShardNotFound
	}
	if !hasReplica(shard.Replicas, target) {
		shard.Replicas = append(shard.Replicas, ReplicaStatus{
			NodeID:  target,
			Role:    "follower",
			Healthy: true,
		})
	}
	shard.Leader = target
	for i := range shard.Replicas {
		if shard.Replicas[i].NodeID == target {
			shard.Replicas[i].Role = "leader"
			continue
		}
		shard.Replicas[i].Role = "follower"
	}
	c.shards[shardID] = shard
	return nil
}

func (c *controlPlane) applyAddReplicaMutationLocked(mutation controlMutation) error {
	shardID := strings.TrimSpace(mutation.ShardID)
	target := strings.TrimSpace(mutation.Target)
	if shardID == "" || target == "" {
		return errs.ErrShardNotFound
	}
	shard, ok := c.shards[shardID]
	if !ok {
		return errs.ErrShardNotFound
	}
	if hasReplica(shard.Replicas, target) {
		return nil
	}
	shard.Replicas = append(shard.Replicas, ReplicaStatus{
		NodeID:  target,
		Role:    "follower",
		Healthy: true,
	})
	c.shards[shardID] = shard
	return nil
}

func (c *controlPlane) applyRemoveReplicaMutationLocked(mutation controlMutation) error {
	shardID := strings.TrimSpace(mutation.ShardID)
	target := strings.TrimSpace(mutation.Target)
	if shardID == "" || target == "" {
		return errs.ErrShardNotFound
	}
	shard, ok := c.shards[shardID]
	if !ok {
		return errs.ErrShardNotFound
	}
	next := make([]ReplicaStatus, 0, len(shard.Replicas)-1)
	removed := false
	for _, replica := range shard.Replicas {
		if replica.NodeID == target {
			removed = true
			continue
		}
		next = append(next, replica)
	}
	if !removed {
		return nil
	}
	if shard.Leader == target {
		return fmt.Errorf("cannot remove leader replica %q from shard %q", target, shardID)
	}
	if len(shard.Replicas) <= 1 {
		return fmt.Errorf("cannot remove last replica from shard %q", shardID)
	}
	shard.Replicas = next
	c.shards[shardID] = shard
	return nil
}

func (c *controlPlane) applySplitMutationLocked(mutation controlMutation) error {
	shardID := strings.TrimSpace(mutation.ShardID)
	if shardID == "" {
		return errs.ErrShardNotFound
	}
	splitKey := mutation.Split
	shard, ok := c.shards[shardID]
	if !ok {
		return errs.ErrShardNotFound
	}
	if !keyInRange(splitKey, shard.StartKey, shard.EndKey) {
		return fmt.Errorf("split key outside shard range")
	}
	if (len(shard.StartKey) > 0 && bytes.Equal(splitKey, shard.StartKey)) ||
		(len(shard.EndKey) > 0 && bytes.Equal(splitKey, shard.EndKey)) {
		return fmt.Errorf("split key must be inside range")
	}
	left := shard
	right := shard
	left.ID = c.uniqueShardID(shardID + "-a")
	right.ID = c.uniqueShardID(shardID + "-b")
	left.EndKey = append([]byte(nil), splitKey...)
	right.StartKey = append([]byte(nil), splitKey...)
	delete(c.shards, shardID)
	c.shards[left.ID] = left
	c.shards[right.ID] = right

	nextOrder := make([]string, 0, len(c.order)+1)
	for _, id := range c.order {
		if id == shardID {
			nextOrder = append(nextOrder, left.ID, right.ID)
			continue
		}
		nextOrder = append(nextOrder, id)
	}
	c.order = nextOrder
	return c.rebuildRoutesLocked()
}

func (c *controlPlane) applyPrepareDrainMutationLocked(mutation controlMutation) error {
	nodeID := strings.TrimSpace(mutation.NodeID)
	if nodeID == "" {
		return errs.ErrShardNotFound
	}
	targets := make(map[string]string)
	for _, id := range c.order {
		shard := c.shards[id]
		if shard.Leader != nodeID {
			continue
		}
		target := ""
		for _, replica := range shard.Replicas {
			if replica.NodeID != nodeID && replica.Healthy {
				target = replica.NodeID
				break
			}
		}
		if target == "" {
			return fmt.Errorf("cannot drain: shard %q has no alternate healthy replica", id)
		}
		targets[id] = target
	}
	for _, id := range c.order {
		target, ok := targets[id]
		if !ok {
			continue
		}
		if err := c.applyTransferLeaderMutationLocked(controlMutation{
			Kind:    "transfer-leader",
			ShardID: id,
			Target:  target,
		}); err != nil {
			return err
		}
	}
	if nodeID == c.nodeID {
		c.draining = true
	}
	return nil
}

func (c *controlPlane) commitLogApplied() uint64 {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.commitLogAppliedIndex
}

func (c *controlPlane) markCommitLogAppliedLocked(index uint64) {
	if index > c.commitLogAppliedIndex {
		c.commitLogAppliedIndex = index
	}
}

func (c *controlPlane) checkOperationPreconditionsLocked(opts ControlWriteOptions, fingerprint string) (bool, error) {
	opID := strings.TrimSpace(opts.OperationID)
	if opID != "" {
		if applied, exists := c.appliedOps[opID]; exists {
			if applied.Fingerprint != fingerprint {
				return false, errs.ErrControlOperationConflict
			}
			return true, nil
		}
	}
	if opts.ExpectedRevision != nil && *opts.ExpectedRevision != c.revision {
		return false, errs.ErrControlRevisionConflict
	}
	return false, nil
}

func (c *controlPlane) noteOperationAppliedLocked(opts ControlWriteOptions, fingerprint string) {
	c.revision++
	opID := strings.TrimSpace(opts.OperationID)
	if opID == "" {
		return
	}
	record := appliedOperationState{
		ID:          opID,
		Fingerprint: fingerprint,
		Revision:    c.revision,
	}
	if _, exists := c.appliedOps[opID]; !exists {
		c.appliedOrder = append(c.appliedOrder, opID)
	}
	c.appliedOps[opID] = record
	if len(c.appliedOrder) <= maxAppliedControlOps {
		return
	}
	overflow := len(c.appliedOrder) - maxAppliedControlOps
	for _, oldID := range c.appliedOrder[:overflow] {
		delete(c.appliedOps, oldID)
	}
	c.appliedOrder = append([]string(nil), c.appliedOrder[overflow:]...)
}

func transferLeaderFingerprint(shardID, target string) string {
	return fmt.Sprintf("transfer:%s:%s", shardID, target)
}

func addReplicaFingerprint(shardID, target string) string {
	return fmt.Sprintf("add-replica:%s:%s", shardID, target)
}

func removeReplicaFingerprint(shardID, target string) string {
	return fmt.Sprintf("remove-replica:%s:%s", shardID, target)
}

func splitFingerprint(shardID string, splitKey []byte) string {
	return fmt.Sprintf("split:%s:%x", shardID, splitKey)
}

func prepareDrainFingerprint(nodeID string) string {
	return fmt.Sprintf("drain:%s", nodeID)
}

func (c *controlPlane) save() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.saveLocked()
}

func (c *controlPlane) saveLocked() error {
	if c == nil || c.fs == nil || c.statePath == "" {
		return nil
	}
	if err := c.fs.MkdirAll(filepath.Dir(c.statePath), 0o755); err != nil {
		return err
	}
	state := c.snapshotStateLocked()
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	tmpPath := c.statePath + ".tmp"
	if err := c.fs.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	if err := c.fs.Rename(tmpPath, c.statePath); err != nil {
		_ = c.fs.Remove(tmpPath)
		return err
	}
	return nil
}

func (c *controlPlane) saveLockedWithRollback(previous controlPlaneState) error {
	if err := c.saveLocked(); err != nil {
		if restoreErr := c.applyState(previous); restoreErr != nil {
			return fmt.Errorf("save control state: %w; rollback control state: %v", err, restoreErr)
		}
		return err
	}
	return nil
}

func (c *controlPlane) loadState() (*controlPlaneState, error) {
	if c == nil || c.fs == nil || c.statePath == "" {
		return nil, os.ErrNotExist
	}
	data, err := c.fs.ReadFile(c.statePath)
	if err != nil {
		return nil, err
	}
	var state controlPlaneState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	if state.Version == 0 {
		state.Version = 1
	}
	if err := validateControlPlaneState(&state); err != nil {
		return nil, err
	}
	return &state, nil
}

func validateControlPlaneState(state *controlPlaneState) error {
	if state == nil {
		return fmt.Errorf("nil state")
	}
	if len(state.Order) == 0 {
		return fmt.Errorf("state order is required")
	}
	if len(state.Shards) == 0 {
		return fmt.Errorf("state contains no shards")
	}
	shards := make(map[string]ShardStatus, len(state.Shards))
	for _, shard := range state.Shards {
		if strings.TrimSpace(shard.ID) == "" {
			return fmt.Errorf("state has shard with empty id")
		}
		if _, exists := shards[shard.ID]; exists {
			return fmt.Errorf("state has duplicate shard id %q", shard.ID)
		}
		shards[shard.ID] = shard
	}
	if _, err := buildShardRoutes(state.Order, shards); err != nil {
		return err
	}
	if len(state.AppliedOps) > maxAppliedControlOps {
		return fmt.Errorf("state contains too many applied operations: %d", len(state.AppliedOps))
	}
	seenOps := make(map[string]struct{}, len(state.AppliedOps))
	for _, op := range state.AppliedOps {
		opID := strings.TrimSpace(op.ID)
		if opID == "" {
			return fmt.Errorf("state has applied operation with empty id")
		}
		if _, exists := seenOps[opID]; exists {
			return fmt.Errorf("state has duplicate applied operation id %q", opID)
		}
		seenOps[opID] = struct{}{}
		if strings.TrimSpace(op.Fingerprint) == "" {
			return fmt.Errorf("state has applied operation %q without fingerprint", opID)
		}
	}
	return nil
}

func (c *controlPlane) snapshotStateLocked() controlPlaneState {
	shards := make([]ShardStatus, 0, len(c.order))
	for _, id := range c.order {
		shard := c.shards[id]
		shard.StartKey = append([]byte(nil), shard.StartKey...)
		shard.EndKey = append([]byte(nil), shard.EndKey...)
		shard.Replicas = append([]ReplicaStatus(nil), shard.Replicas...)
		shards = append(shards, shard)
	}
	return controlPlaneState{
		Version:               1,
		NodeID:                c.nodeID,
		ClusterID:             c.clusterID,
		StorageMode:           c.storageMode,
		Raft:                  c.raft,
		Draining:              c.draining,
		Revision:              c.revision,
		CommitLogAppliedIndex: c.commitLogAppliedIndex,
		Order:                 append([]string(nil), c.order...),
		Shards:                shards,
		AppliedOps:            c.appliedOpsSnapshotLocked(),
	}
}

func (c *controlPlane) applyState(state controlPlaneState) error {
	c.draining = state.Draining
	c.revision = state.Revision
	c.commitLogAppliedIndex = state.CommitLogAppliedIndex
	c.order = append([]string(nil), state.Order...)
	c.shards = make(map[string]ShardStatus, len(state.Shards))
	for _, shard := range state.Shards {
		shardCopy := shard
		shardCopy.StartKey = append([]byte(nil), shard.StartKey...)
		shardCopy.EndKey = append([]byte(nil), shard.EndKey...)
		shardCopy.Replicas = append([]ReplicaStatus(nil), shard.Replicas...)
		c.shards[shard.ID] = shardCopy
	}
	c.appliedOps = make(map[string]appliedOperationState, len(state.AppliedOps))
	c.appliedOrder = make([]string, 0, len(state.AppliedOps))
	for _, op := range state.AppliedOps {
		opID := strings.TrimSpace(op.ID)
		c.appliedOps[opID] = appliedOperationState{
			ID:          opID,
			Fingerprint: op.Fingerprint,
			Revision:    op.Revision,
		}
		c.appliedOrder = append(c.appliedOrder, opID)
	}
	if err := c.rebuildRoutesLocked(); err != nil {
		return err
	}
	return nil
}

func (c *controlPlane) appliedOpsSnapshotLocked() []appliedOperationState {
	if len(c.appliedOrder) == 0 {
		return nil
	}
	out := make([]appliedOperationState, 0, len(c.appliedOrder))
	for _, opID := range c.appliedOrder {
		op, ok := c.appliedOps[opID]
		if !ok {
			continue
		}
		out = append(out, op)
	}
	return out
}

func (c *controlPlane) rebuildRoutesLocked() error {
	routes, err := buildShardRoutes(c.order, c.shards)
	if err != nil {
		return err
	}
	c.routes = routes
	return nil
}

func buildShardRoutes(order []string, shards map[string]ShardStatus) ([]shardRoute, error) {
	if len(order) == 0 {
		return nil, fmt.Errorf("shard order is required")
	}
	if len(shards) == 0 {
		return nil, fmt.Errorf("at least one shard is required")
	}
	if len(order) != len(shards) {
		return nil, fmt.Errorf("shard order count %d does not match shard map count %d", len(order), len(shards))
	}

	routes := make([]shardRoute, 0, len(order))
	seen := make(map[string]struct{}, len(order))
	var prevEnd []byte
	var prevID string
	for i, id := range order {
		id = strings.TrimSpace(id)
		if id == "" {
			return nil, fmt.Errorf("shard order contains empty id")
		}
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("shard order contains duplicate shard %q", id)
		}
		seen[id] = struct{}{}

		shard, exists := shards[id]
		if !exists {
			return nil, fmt.Errorf("state order references unknown shard %q", id)
		}
		start := append([]byte(nil), shard.StartKey...)
		end := append([]byte(nil), shard.EndKey...)
		if len(end) > 0 && bytes.Compare(start, end) >= 0 {
			return nil, fmt.Errorf("invalid shard range %q: start key must be < end key", id)
		}
		if i > 0 {
			if len(prevEnd) == 0 {
				return nil, fmt.Errorf("open-ended shard %q must be last", prevID)
			}
			if len(start) == 0 {
				return nil, fmt.Errorf("shard %q has empty start key after first shard", id)
			}
			if bytes.Compare(start, prevEnd) < 0 {
				return nil, fmt.Errorf("overlapping shard ranges between %q and %q", prevID, id)
			}
		}
		routes = append(routes, shardRoute{
			id:    id,
			start: start,
			end:   end,
		})
		prevID = id
		prevEnd = end
	}
	return routes, nil
}

func hasReplica(replicas []ReplicaStatus, nodeID string) bool {
	for _, replica := range replicas {
		if replica.NodeID == nodeID {
			return true
		}
	}
	return false
}

func keyInRange(key, start, end []byte) bool {
	if len(start) > 0 && bytes.Compare(key, start) < 0 {
		return false
	}
	if len(end) > 0 && bytes.Compare(key, end) >= 0 {
		return false
	}
	return true
}

// ClusterStatus returns node-level control-plane metadata.
func (l *LSM) ClusterStatus() ClusterStatus {
	if l == nil || l.control == nil {
		return ClusterStatus{}
	}
	return l.control.status()
}

// Shards returns shard-level runtime metadata.
func (l *LSM) Shards() []ShardStatus {
	if l == nil || l.control == nil {
		return nil
	}
	return l.control.shardsSnapshot()
}

// TransferLeader sets a new leader for a shard.
func (l *LSM) TransferLeader(shardID, target string) error {
	if l == nil || l.control == nil {
		return errs.ErrShardNotFound
	}
	return l.control.transferLeader(shardID, target)
}

// TransferLeaderWithOptions sets a new leader with optional concurrency controls.
func (l *LSM) TransferLeaderWithOptions(shardID, target string, opts ControlWriteOptions) error {
	if l == nil || l.control == nil {
		return errs.ErrShardNotFound
	}
	return l.control.transferLeaderWithOptions(shardID, target, opts)
}

// AddReplica adds a follower replica to a shard's control-plane membership.
func (l *LSM) AddReplica(shardID, target string) error {
	if l == nil || l.control == nil {
		return errs.ErrShardNotFound
	}
	return l.control.addReplica(shardID, target)
}

// AddReplicaWithOptions adds a replica with optional concurrency controls.
func (l *LSM) AddReplicaWithOptions(shardID, target string, opts ControlWriteOptions) error {
	if l == nil || l.control == nil {
		return errs.ErrShardNotFound
	}
	return l.control.addReplicaWithOptions(shardID, target, opts)
}

// RemoveReplica removes a non-leader replica from a shard's control-plane membership.
func (l *LSM) RemoveReplica(shardID, target string) error {
	if l == nil || l.control == nil {
		return errs.ErrShardNotFound
	}
	return l.control.removeReplica(shardID, target)
}

// RemoveReplicaWithOptions removes a replica with optional concurrency controls.
func (l *LSM) RemoveReplicaWithOptions(shardID, target string, opts ControlWriteOptions) error {
	if l == nil || l.control == nil {
		return errs.ErrShardNotFound
	}
	return l.control.removeReplicaWithOptions(shardID, target, opts)
}

// TriggerSplit splits a shard into two ranges at splitKey.
func (l *LSM) TriggerSplit(shardID string, splitKey []byte) error {
	if l == nil || l.control == nil {
		return errs.ErrShardNotFound
	}
	return l.control.triggerSplit(shardID, splitKey)
}

// TriggerSplitWithOptions splits a shard with optional concurrency controls.
func (l *LSM) TriggerSplitWithOptions(shardID string, splitKey []byte, opts ControlWriteOptions) error {
	if l == nil || l.control == nil {
		return errs.ErrShardNotFound
	}
	return l.control.triggerSplitWithOptions(shardID, splitKey, opts)
}

// TriggerRebalance moves shard leadership to target.
func (l *LSM) TriggerRebalance(shardID, target string) error {
	if l == nil || l.control == nil {
		return errs.ErrShardNotFound
	}
	return l.control.triggerRebalance(shardID, target)
}

// TriggerRebalanceWithOptions rebalances leadership with optional concurrency controls.
func (l *LSM) TriggerRebalanceWithOptions(shardID, target string, opts ControlWriteOptions) error {
	if l == nil || l.control == nil {
		return errs.ErrShardNotFound
	}
	return l.control.triggerRebalanceWithOptions(shardID, target, opts)
}

// PrepareDrain transfers local leadership off-node before drain.
func (l *LSM) PrepareDrain(nodeID string) error {
	if l == nil || l.control == nil {
		return errs.ErrShardNotFound
	}
	return l.control.prepareDrain(nodeID)
}

// PrepareDrainWithOptions prepares drain with optional concurrency controls.
func (l *LSM) PrepareDrainWithOptions(nodeID string, opts ControlWriteOptions) error {
	if l == nil || l.control == nil {
		return errs.ErrShardNotFound
	}
	return l.control.prepareDrainWithOptions(nodeID, opts)
}
