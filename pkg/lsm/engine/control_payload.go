package engine

import (
	"bytes"
	"errors"
	"fmt"
	"lsmengine/pkg/lsm/errs"
)

// applyCommittedControlFromLog executes committed state without proposal locks.
// Caller serialization establishes ordering across data and control entries.
func (c *controlPlane) applyCommittedControlFromLog(entry controlCommittedEntry) error {
	m := entry.Mutation
	var fingerprint string
	switch m.Kind {
	case "transfer-leader":
		fingerprint = transferLeaderFingerprint(m.ShardID, m.Target)
	case "split":
		fingerprint = splitFingerprint(m.ShardID, m.Split)
	case "prepare-drain":
		fingerprint = prepareDrainFingerprint(m.NodeID)
	default:
		return fmt.Errorf("unknown committed control mutation %q", m.Kind)
	}
	err := c.applyCommittedControlMutation(entry, ControlWriteOptions{OperationID: m.OperationID}, fingerprint, c.applyControlPayloadLocked)
	if errors.Is(err, errControlNoop) {
		return nil
	}
	return err
}

func (c *controlPlane) applyControlPayloadLocked(m controlMutation) error {
	switch m.Kind {
	case "transfer-leader":
		return c.applyTransferLeaderPayloadLocked(m)
	case "split":
		return c.applySplitPayloadLocked(m)
	case "prepare-drain":
		return c.applyDrainPayloadLocked(m)
	default:
		return fmt.Errorf("unknown committed control mutation %q", m.Kind)
	}
}

func (c *controlPlane) applyTransferLeaderPayloadLocked(mutation controlMutation) error {
	shardID, target := mutation.ShardID, mutation.Target
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

func (c *controlPlane) applySplitPayloadLocked(mutation controlMutation) error {
	shardID, splitKey := mutation.ShardID, mutation.Split
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

func (c *controlPlane) applyDrainPayloadLocked(mutation controlMutation) error {
	nodeID := mutation.NodeID
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
	if nodeID == c.nodeID {
		c.draining = true
	}
	return nil
}
