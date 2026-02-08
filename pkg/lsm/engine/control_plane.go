package engine

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

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
	NodeID      string      `json:"node_id"`
	ClusterID   string      `json:"cluster_id"`
	StorageMode StorageMode `json:"storage_mode"`
	ShardCount  int         `json:"shard_count"`
	Draining    bool        `json:"draining"`
	Raft        RaftOptions `json:"raft"`
}

type controlPlane struct {
	mu          sync.RWMutex
	nodeID      string
	clusterID   string
	storageMode StorageMode
	raft        RaftOptions
	draining    bool
	order       []string
	shards      map[string]ShardStatus
}

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

	c := &controlPlane{
		nodeID:      nodeID,
		clusterID:   clusterID,
		storageMode: mode,
		raft:        raft,
		order:       nil,
		shards:      make(map[string]ShardStatus),
	}
	if len(opts.ShardMap) == 0 {
		shard := ShardStatus{
			ID:     "default",
			Leader: nodeID,
			Replicas: []ReplicaStatus{
				{NodeID: nodeID, Role: "leader", Healthy: true},
			},
		}
		c.order = []string{shard.ID}
		c.shards[shard.ID] = shard
		return c, nil
	}

	c.order = make([]string, 0, len(opts.ShardMap))
	for _, cfg := range opts.ShardMap {
		id := strings.TrimSpace(cfg.ID)
		if id == "" {
			return nil, fmt.Errorf("shard id is required")
		}
		if _, exists := c.shards[id]; exists {
			return nil, fmt.Errorf("duplicate shard id %q", id)
		}
		replicas := append([]string(nil), cfg.Replicas...)
		if len(replicas) == 0 {
			replicas = []string{nodeID}
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
	return c, nil
}

func (c *controlPlane) status() ClusterStatus {
	if c == nil {
		return ClusterStatus{}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return ClusterStatus{
		NodeID:      c.nodeID,
		ClusterID:   c.clusterID,
		StorageMode: c.storageMode,
		ShardCount:  len(c.order),
		Draining:    c.draining,
		Raft:        c.raft,
	}
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

func (c *controlPlane) transferLeader(shardID, target string) error {
	if c == nil {
		return errs.ErrShardNotFound
	}
	shardID = strings.TrimSpace(shardID)
	target = strings.TrimSpace(target)
	if shardID == "" || target == "" {
		return errs.ErrShardNotFound
	}
	c.mu.Lock()
	defer c.mu.Unlock()

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

func (c *controlPlane) triggerSplit(shardID string, splitKey []byte) error {
	if c == nil {
		return errs.ErrShardNotFound
	}
	shardID = strings.TrimSpace(shardID)
	if shardID == "" {
		return errs.ErrShardNotFound
	}
	c.mu.Lock()
	defer c.mu.Unlock()

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
	return nil
}

func (c *controlPlane) triggerRebalance(shardID, target string) error {
	return c.transferLeader(shardID, target)
}

func (c *controlPlane) prepareDrain(nodeID string) error {
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

	c.mu.Lock()
	defer c.mu.Unlock()
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
}

func (c *controlPlane) shardForKeyLocked(key []byte) (ShardStatus, bool) {
	for _, id := range c.order {
		shard := c.shards[id]
		if keyInRange(key, shard.StartKey, shard.EndKey) {
			return shard, true
		}
	}
	return ShardStatus{}, false
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

// TriggerSplit splits a shard into two ranges at splitKey.
func (l *LSM) TriggerSplit(shardID string, splitKey []byte) error {
	if l == nil || l.control == nil {
		return errs.ErrShardNotFound
	}
	return l.control.triggerSplit(shardID, splitKey)
}

// TriggerRebalance moves shard leadership to target.
func (l *LSM) TriggerRebalance(shardID, target string) error {
	if l == nil || l.control == nil {
		return errs.ErrShardNotFound
	}
	return l.control.triggerRebalance(shardID, target)
}

// PrepareDrain transfers local leadership off-node before drain.
func (l *LSM) PrepareDrain(nodeID string) error {
	if l == nil || l.control == nil {
		return errs.ErrShardNotFound
	}
	return l.control.prepareDrain(nodeID)
}
