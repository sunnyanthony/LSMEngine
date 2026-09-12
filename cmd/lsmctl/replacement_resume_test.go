package main

import (
	"lsmengine/pkg/lsm"
	"testing"
)

func TestReplacementResumeRequiresSelectedMembership(t *testing.T) {
	for _, tc := range []struct {
		name      string
		shards    []lsm.ShardStatus
		requested []string
		pass      bool
	}{
		{"old-present", []lsm.ShardStatus{{ID: "users", Replicas: []lsm.ReplicaStatus{{NodeID: "old"}}}}, []string{"users"}, true},
		{"new-present", []lsm.ShardStatus{{ID: "users", Replicas: []lsm.ReplicaStatus{{NodeID: "new"}}}}, []string{"users"}, true},
		{"neither", []lsm.ShardStatus{{ID: "users", Replicas: []lsm.ReplicaStatus{{NodeID: "other"}}}}, []string{"users"}, false},
		{"missing-shard", nil, []string{"users"}, false},
		{"unselected", nil, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ids, err := replacementResumeShardIDs(tc.shards, "old", "new", tc.requested)
			if (err == nil) != tc.pass {
				t.Fatalf("ids=%v err=%v", ids, err)
			}
		})
	}
}
