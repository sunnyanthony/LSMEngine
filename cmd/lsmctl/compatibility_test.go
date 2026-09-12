package main

import (
	"fmt"
	"testing"

	"lsmengine/pkg/lsm"
)

func TestCompatibilityGateChecksEveryContract(t *testing.T) {
	base := lsm.CompatibilityStatus{ClusterStatusVersion: 1, ControlStateVersion: 1, StateSnapshotVersion: 1, RaftPeerMessageVersion: 1}
	for field := 0; field < 4; field++ {
		for _, version := range []int{-1, 0, 1, 2} {
			t.Run(fmt.Sprintf("field%d/version%d", field, version), func(t *testing.T) {
				other := base
				fields := []*int{&other.ClusterStatusVersion, &other.ControlStateVersion, &other.StateSnapshotVersion, &other.RaftPeerMessageVersion}
				*fields[field] = version
				statuses := clusterStatusResult{Nodes: []clusterStatusNodeResult{
					{Node: "node-a", Status: &lsm.ClusterStatus{Compatibility: base, CommitLogRuntime: lsm.CommitLogRuntimeStatus{Health: "ready", Leader: true, WriteAvailable: true}}},
					{Node: "node-b", Status: &lsm.ClusterStatus{Compatibility: other, CommitLogRuntime: lsm.CommitLogRuntimeStatus{Health: "follower"}}},
				}}
				result := evaluateClusterWait(statuses, waitClusterOptions{RequiredReadyNodes: 2, RequireWriteLeader: true, RequireCompatible: true})
				if result.Ready != (version == 1) || result.Compatible != (version == 1) {
					t.Fatalf("unexpected result: %+v", result)
				}
			})
		}
	}
}

func TestCompatibilityGateDoesNotClaimOfflineNodeCompatibility(t *testing.T) {
	known := lsm.CompatibilityStatus{ClusterStatusVersion: 1, ControlStateVersion: 1, StateSnapshotVersion: 1, RaftPeerMessageVersion: 1}
	statuses := clusterStatusResult{Nodes: []clusterStatusNodeResult{
		{Node: "node-a", Status: &lsm.ClusterStatus{Compatibility: known, CommitLogRuntime: lsm.CommitLogRuntimeStatus{Health: "ready", Leader: true, WriteAvailable: true}}},
		{Node: "node-b", Error: "unreachable"},
	}}
	opts := waitClusterOptions{RequiredReadyNodes: 2, RequireWriteLeader: true, RequireCompatible: true}
	if result := evaluateClusterWait(statuses, opts); result.Ready {
		t.Fatalf("offline node counted as ready: %+v", result)
	}
	opts.RequiredReadyNodes = 1
	if result := evaluateClusterWait(statuses, opts); !result.Ready || !result.Compatible {
		t.Fatalf("degraded ready-set check failed: %+v", result)
	}
}
