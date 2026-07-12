package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"lsmengine/pkg/lsm"
	serverconfig "lsmengine/pkg/lsm/server/config"
)

func TestServeSignalsIncludeContainerTermination(t *testing.T) {
	signals := serveSignals()
	if !containsSignal(signals, os.Interrupt) {
		t.Fatalf("expected serve signals to include interrupt")
	}
	if !containsSignal(signals, syscall.SIGTERM) {
		t.Fatalf("expected serve signals to include SIGTERM for container stop")
	}
}

func containsSignal(signals []os.Signal, want os.Signal) bool {
	for _, signal := range signals {
		if signal == want {
			return true
		}
	}
	return false
}

func TestParseWriteConsistencyDefault(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    lsm.WriteConsistency
		wantErr bool
	}{
		{name: "empty defaults to accepted", input: "", want: lsm.WriteConsistencyAccepted},
		{name: "accepted", input: "accepted", want: lsm.WriteConsistencyAccepted},
		{name: "local committed", input: "local_committed", want: lsm.WriteConsistencyLocalCommitted},
		{name: "invalid", input: "eventual", wantErr: true},
		{name: "linearizable rejected", input: "linearizable", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseWriteConsistencyDefault(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, got)
			}
		})
	}
}

func TestNodeEndpointFlagsSet(t *testing.T) {
	var endpoints nodeEndpointFlags
	if err := endpoints.Set("node-a=127.0.0.1:8080"); err != nil {
		t.Fatalf("set endpoint: %v", err)
	}
	if got := endpoints["node-a"]; got != "http://127.0.0.1:8080" {
		t.Fatalf("unexpected endpoint %q", got)
	}
	if err := endpoints.Set("missing-separator"); err == nil {
		t.Fatalf("expected invalid endpoint error")
	}
}

func TestClusterWriteOptionsFromConfigMergesPeerURLsAndOverrides(t *testing.T) {
	opts, err := clusterWriteOptionsFromConfig(serverconfig.Config{
		NodeID: "node-a",
		Raft: serverconfig.RaftConfig{
			PeerURLs: map[string]string{
				"node-a": "http://internal-a:8080",
				"node-b": "http://internal-b:8080",
			},
		},
	}, "http://127.0.0.1:8080", true, nodeEndpointFlags{
		"node-b": "http://127.0.0.1:8081",
	})
	if err != nil {
		t.Fatalf("cluster options: %v", err)
	}
	if !opts.Enabled {
		t.Fatalf("expected cluster writes enabled")
	}
	if got := opts.NodeEndpoints["node-a"]; got != "http://127.0.0.1:8080" {
		t.Fatalf("expected bootstrap addr to override node-a, got %q", got)
	}
	if got := opts.NodeEndpoints["node-b"]; got != "http://127.0.0.1:8081" {
		t.Fatalf("expected explicit override for node-b, got %q", got)
	}
}

func TestClusterNodeEndpointsFromConfigUsesAddrFallback(t *testing.T) {
	got, err := clusterNodeEndpointsFromConfig(serverconfig.Config{}, "127.0.0.1:8080", nil)
	if err != nil {
		t.Fatalf("cluster endpoints: %v", err)
	}
	if got["addr"] != "http://127.0.0.1:8080" {
		t.Fatalf("expected addr fallback endpoint, got %+v", got)
	}
}

func TestClusterNodeEndpointsFromConfigLoadsPeerURLFile(t *testing.T) {
	path := t.TempDir() + "/peers.yaml"
	if err := os.WriteFile(path, []byte(`
node-a: "http://file-a:8080/"
node-c: "http://file-c:8080"
`), 0o644); err != nil {
		t.Fatalf("write peer url file: %v", err)
	}
	got, err := clusterNodeEndpointsFromConfig(serverconfig.Config{
		NodeID: "node-a",
		Raft: serverconfig.RaftConfig{
			PeerURLFile: path,
			PeerURLs: map[string]string{
				"node-a": "http://static-a:8080",
				"node-b": "http://static-b:8080",
			},
			JoinPeerURLs: map[string]string{
				"node-d": "http://static-d:8080",
			},
		},
	}, "http://127.0.0.1:8080", nodeEndpointFlags{
		"node-c": "http://127.0.0.1:8082",
	})
	if err != nil {
		t.Fatalf("cluster endpoints: %v", err)
	}
	want := map[string]string{
		"node-a": "http://127.0.0.1:8080",
		"node-b": "http://static-b:8080",
		"node-c": "http://127.0.0.1:8082",
		"node-d": "http://static-d:8080",
	}
	for nodeID, endpoint := range want {
		if got[nodeID] != endpoint {
			t.Fatalf("expected %s endpoint %q, got %q in %+v", nodeID, endpoint, got[nodeID], got)
		}
	}
}

func TestClusterNodeEndpointsFromConfigRejectsInvalidPeerURLFile(t *testing.T) {
	path := t.TempDir() + "/peers.yaml"
	if err := os.WriteFile(path, []byte(`node-a: "127.0.0.1:8080"`), 0o644); err != nil {
		t.Fatalf("write peer url file: %v", err)
	}
	_, err := clusterNodeEndpointsFromConfig(serverconfig.Config{
		Raft: serverconfig.RaftConfig{PeerURLFile: path},
	}, "", nil)
	if err == nil {
		t.Fatalf("expected invalid peer url file error")
	}
}

func TestReadClusterStatusesRecordsPartialFailures(t *testing.T) {
	okServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cluster/status" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
			NodeID:     "node-b",
			Revision:   7,
			ShardCount: 2,
			CommitLogRuntime: lsm.CommitLogRuntimeStatus{
				Health:         "ready",
				Leader:         true,
				LeaderKnown:    true,
				WriteAvailable: true,
				Term:           3,
				Index:          9,
			},
		})
	}))
	defer okServer.Close()

	downServer := httptest.NewServer(http.NotFoundHandler())
	downURL := downServer.URL
	downServer.Close()

	got, err := readClusterStatuses(map[string]string{
		"node-a": downURL,
		"node-b": okServer.URL,
	})
	if err != nil {
		t.Fatalf("read cluster statuses: %v", err)
	}
	if len(got.Nodes) != 2 {
		t.Fatalf("expected two nodes, got %+v", got)
	}
	if got.Nodes[0].Node != "node-a" || got.Nodes[0].Error == "" {
		t.Fatalf("expected node-a error, got %+v", got.Nodes[0])
	}
	if got.Nodes[1].Node != "node-b" || got.Nodes[1].Status == nil {
		t.Fatalf("expected node-b status, got %+v", got.Nodes[1])
	}
	if !got.Nodes[1].Status.CommitLogRuntime.WriteAvailable {
		t.Fatalf("expected node-b write availability")
	}
}

func TestWriteClusterStatuses(t *testing.T) {
	var buf bytes.Buffer
	writeClusterStatuses(&buf, clusterStatusResult{
		Nodes: []clusterStatusNodeResult{
			{
				Node:     "node-a",
				Endpoint: "http://127.0.0.1:8080",
				Status: &lsm.ClusterStatus{
					NodeID:     "node-a",
					Revision:   4,
					ShardCount: 1,
					CommitLogRuntime: lsm.CommitLogRuntimeStatus{
						Health:         "ready",
						Leader:         true,
						LeaderKnown:    true,
						WriteAvailable: true,
						Term:           2,
						Index:          11,
					},
				},
			},
		},
	})
	out := buf.String()
	for _, want := range []string{
		"node=node-a",
		"ok=true",
		"health=ready",
		"write_available=true",
		"term=2",
		"index=11",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got %q", want, out)
		}
	}
}

func TestEvaluateClusterWaitRequiresReadyNodesAndWriteLeader(t *testing.T) {
	got := evaluateClusterWait(clusterStatusResult{
		Nodes: []clusterStatusNodeResult{
			{
				Node:     "node-a",
				Endpoint: "http://127.0.0.1:8080",
				Status: &lsm.ClusterStatus{
					NodeID: "node-a",
					CommitLogRuntime: lsm.CommitLogRuntimeStatus{
						Health:         "follower",
						LeaderKnown:    true,
						WriteAvailable: false,
					},
				},
			},
			{
				Node:     "node-b",
				Endpoint: "http://127.0.0.1:8081",
				Status: &lsm.ClusterStatus{
					NodeID: "node-b",
					CommitLogRuntime: lsm.CommitLogRuntimeStatus{
						Health:         "ready",
						Leader:         true,
						LeaderKnown:    true,
						WriteAvailable: true,
					},
				},
			},
			{
				Node:     "node-c",
				Endpoint: "http://127.0.0.1:8082",
				Error:    "connection refused",
			},
		},
	}, waitClusterOptions{
		RequiredReadyNodes: 2,
		RequireWriteLeader: true,
	})
	if !got.Ready {
		t.Fatalf("expected cluster wait ready, got %+v", got)
	}
	if got.ReadyNodes != 2 || got.WriteLeader != "node-b" || got.WriteLeaderEndpoint != "http://127.0.0.1:8081" {
		t.Fatalf("unexpected wait result: %+v", got)
	}
}

func TestEvaluateClusterWaitRejectsMissingWriteLeader(t *testing.T) {
	got := evaluateClusterWait(clusterStatusResult{
		Nodes: []clusterStatusNodeResult{
			{
				Node:     "node-a",
				Endpoint: "http://127.0.0.1:8080",
				Status: &lsm.ClusterStatus{
					NodeID: "node-a",
					CommitLogRuntime: lsm.CommitLogRuntimeStatus{
						Health:         "follower",
						LeaderKnown:    false,
						WriteAvailable: false,
					},
				},
			},
		},
	}, waitClusterOptions{
		RequiredReadyNodes: 1,
		RequireWriteLeader: true,
	})
	if got.Ready {
		t.Fatalf("expected missing write leader to keep wait result not ready: %+v", got)
	}
	if got.ReadyNodes != 1 {
		t.Fatalf("expected one healthy node, got %+v", got)
	}
}

func TestDrainClusterNodeSubmitsToWriteLeaderAndWaitsForDrain(t *testing.T) {
	var drainCalls atomic.Int32

	nodeA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID:   "node-a",
				Draining: true,
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         false,
					WriteAvailable: false,
					Health:         "follower",
					LeaderKnown:    true,
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeA.Close()

	nodeB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID: "node-b",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         true,
					WriteAvailable: true,
					Health:         "ready",
					LeaderKnown:    true,
				},
			})
		case "/cluster/nodes/node-a/drain":
			drainCalls.Add(1)
			var req drainRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode drain: %v", err)
			}
			if req.OperationID != "drain-node-a" {
				t.Fatalf("expected operation id drain-node-a, got %q", req.OperationID)
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		case "/cluster/shards":
			_ = json.NewEncoder(w).Encode([]lsm.ShardStatus{
				{
					ID:       "users",
					StartKey: []byte("a"),
					EndKey:   []byte("z"),
					Leader:   "node-b",
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeB.Close()

	result, err := drainClusterNode(map[string]string{
		"node-a": nodeA.URL,
		"node-b": nodeB.URL,
	}, "node-a", controlRequestOptions{OperationID: "drain-node-a"})
	if err != nil {
		t.Fatalf("drain node: %v", err)
	}
	if result.Target != "node-a" || result.SubmittedTo != "node-b" {
		t.Fatalf("unexpected drain result: %+v", result)
	}
	if drainCalls.Load() != 1 {
		t.Fatalf("expected one drain call, got %d", drainCalls.Load())
	}
	if len(result.Shards) != 1 || result.Shards[0].Leader != "node-b" {
		t.Fatalf("expected shard leader node-b, got %+v", result.Shards)
	}
}

func TestDrainCompleteCanAllowUnavailableReplacementTarget(t *testing.T) {
	result := drainNodeResult{
		Target: "node-a",
		Shards: []lsm.ShardStatus{
			{ID: "users", Leader: "node-b"},
		},
		Statuses: clusterStatusResult{
			Nodes: []clusterStatusNodeResult{
				{Node: "node-a", Endpoint: "http://127.0.0.1:8080", Error: "connection refused"},
			},
		},
	}
	if drainComplete(result, false) {
		t.Fatalf("strict drain should require target draining status")
	}
	if !drainComplete(result, true) {
		t.Fatalf("replacement drain should allow unavailable target after leadership moved")
	}
	result.Shards[0].Leader = "node-a"
	if drainComplete(result, true) {
		t.Fatalf("replacement drain should not complete while target still leads a shard")
	}
}

func TestResumeClusterNodeSubmitsToWriteLeaderAndWaitsForResume(t *testing.T) {
	var resumeCalls atomic.Int32

	nodeA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID:   "node-a",
				Draining: false,
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         false,
					WriteAvailable: false,
					Health:         "follower",
					LeaderKnown:    true,
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeA.Close()

	nodeB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID: "node-b",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         true,
					WriteAvailable: true,
					Health:         "ready",
					LeaderKnown:    true,
				},
			})
		case "/cluster/nodes/node-a/resume":
			resumeCalls.Add(1)
			var req drainRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode resume: %v", err)
			}
			if req.OperationID != "resume-node-a" {
				t.Fatalf("expected operation id resume-node-a, got %q", req.OperationID)
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		case "/cluster/shards":
			_ = json.NewEncoder(w).Encode([]lsm.ShardStatus{
				{
					ID:       "users",
					StartKey: []byte("a"),
					EndKey:   []byte("z"),
					Leader:   "node-b",
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeB.Close()

	result, err := resumeClusterNode(map[string]string{
		"node-a": nodeA.URL,
		"node-b": nodeB.URL,
	}, "node-a", controlRequestOptions{OperationID: "resume-node-a"})
	if err != nil {
		t.Fatalf("resume node: %v", err)
	}
	if result.Target != "node-a" || result.SubmittedTo != "node-b" {
		t.Fatalf("unexpected resume result: %+v", result)
	}
	if resumeCalls.Load() != 1 {
		t.Fatalf("expected one resume call, got %d", resumeCalls.Load())
	}
}

func TestChangeRaftMembershipSubmitsToWriteLeader(t *testing.T) {
	var raftAddCalls atomic.Int32

	nodeA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID: "node-a",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         false,
					WriteAvailable: false,
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeA.Close()

	nodeB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID: "node-b",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         true,
					WriteAvailable: true,
					Health:         "ready",
				},
			})
		case "/cluster/nodes/node-c/raft-add":
			raftAddCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeB.Close()

	result, err := changeRaftMembership(map[string]string{
		"node-a": nodeA.URL,
		"node-b": nodeB.URL,
	}, "raft-add", "node-c")
	if err != nil {
		t.Fatalf("raft membership: %v", err)
	}
	if result.Operation != "raft-add" || result.Node != "node-c" || result.SubmittedTo != "node-b" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if raftAddCalls.Load() != 1 {
		t.Fatalf("expected one raft-add call, got %d", raftAddCalls.Load())
	}
}

func TestChangeShardReplicaSubmitsToWriteLeaderAndWaitsForMembership(t *testing.T) {
	var addCalls atomic.Int32

	nodeA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID: "node-a",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         false,
					WriteAvailable: false,
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeA.Close()

	nodeB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID: "node-b",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         true,
					WriteAvailable: true,
				},
			})
		case "/cluster/shards/users/add-replica":
			addCalls.Add(1)
			var req targetRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode add replica: %v", err)
			}
			if req.Target != "node-c" || req.OperationID != "add-node-c" {
				t.Fatalf("unexpected request: %+v", req)
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		case "/cluster/shards":
			_ = json.NewEncoder(w).Encode([]lsm.ShardStatus{
				{
					ID:     "users",
					Leader: "node-b",
					Replicas: []lsm.ReplicaStatus{
						{NodeID: "node-a", Role: "follower", Healthy: true},
						{NodeID: "node-b", Role: "leader", Healthy: true},
						{NodeID: "node-c", Role: "follower", Healthy: true},
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeB.Close()

	result, err := changeShardReplica(map[string]string{
		"node-a": nodeA.URL,
		"node-b": nodeB.URL,
	}, "add-replica", "users", "node-c", controlRequestOptions{OperationID: "add-node-c"})
	if err != nil {
		t.Fatalf("change shard replica: %v", err)
	}
	if result.Operation != "add-replica" || result.Shard != "users" || result.Node != "node-c" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if addCalls.Load() != 1 {
		t.Fatalf("expected one add-replica call, got %d", addCalls.Load())
	}
}

func TestShardReplicaActionCompleteForRemove(t *testing.T) {
	result := membershipActionResult{
		Operation: "remove-replica",
		Shard:     "users",
		Node:      "node-c",
		Shards: []lsm.ShardStatus{
			{
				ID:     "users",
				Leader: "node-b",
				Replicas: []lsm.ReplicaStatus{
					{NodeID: "node-a", Role: "follower", Healthy: true},
					{NodeID: "node-b", Role: "leader", Healthy: true},
				},
			},
		},
	}
	if !shardReplicaActionComplete(result) {
		t.Fatalf("expected remove-replica to be complete")
	}
}

func TestReplaceClusterNodeRunsMembershipWorkflow(t *testing.T) {
	var raftAddCalls atomic.Int32
	var raftRemoveCalls atomic.Int32
	var addReplicaCalls atomic.Int32
	var drainCalls atomic.Int32
	var removeReplicaCalls atomic.Int32

	nodeA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID:   "node-a",
				Draining: drainCalls.Load() > 0,
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         false,
					WriteAvailable: false,
					Health:         "follower",
					LeaderKnown:    true,
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeA.Close()

	nodeC := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID: "node-c",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         false,
					WriteAvailable: false,
					Health:         "follower",
					LeaderKnown:    true,
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeC.Close()

	nodeD := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID: "node-d",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         false,
					WriteAvailable: false,
					Health:         "follower",
					LeaderKnown:    true,
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeD.Close()

	nodeB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID: "node-b",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         true,
					WriteAvailable: true,
					Health:         "ready",
					LeaderKnown:    true,
				},
			})
		case "/cluster/shards":
			replicas := []lsm.ReplicaStatus{
				{NodeID: "node-a", Role: "leader", Healthy: true},
				{NodeID: "node-b", Role: "follower", Healthy: true},
				{NodeID: "node-c", Role: "follower", Healthy: true},
			}
			leader := "node-a"
			if addReplicaCalls.Load() > 0 {
				replicas = append(replicas, lsm.ReplicaStatus{NodeID: "node-d", Role: "follower", Healthy: true})
			}
			if drainCalls.Load() > 0 {
				leader = "node-b"
				for i := range replicas {
					if replicas[i].NodeID == "node-b" {
						replicas[i].Role = "leader"
					} else {
						replicas[i].Role = "follower"
					}
				}
			}
			if removeReplicaCalls.Load() > 0 {
				next := replicas[:0]
				for _, replica := range replicas {
					if replica.NodeID != "node-a" {
						next = append(next, replica)
					}
				}
				replicas = next
			}
			_ = json.NewEncoder(w).Encode([]lsm.ShardStatus{
				{
					ID:       "users",
					StartKey: []byte("a"),
					EndKey:   []byte("z"),
					Leader:   leader,
					Replicas: replicas,
				},
			})
		case "/cluster/nodes/node-d/raft-add":
			raftAddCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		case "/cluster/shards/users/add-replica":
			addReplicaCalls.Add(1)
			var req targetRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode add replica: %v", err)
			}
			if req.Target != "node-d" || req.OperationID != "replace-node-a-with-node-d-add-users-node-d" {
				t.Fatalf("unexpected add request: %+v", req)
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		case "/cluster/nodes/node-a/drain":
			drainCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		case "/cluster/shards/users/remove-replica":
			removeReplicaCalls.Add(1)
			var req targetRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode remove replica: %v", err)
			}
			if req.Target != "node-a" || req.OperationID != "replace-node-a-with-node-d-remove-users-node-a" {
				t.Fatalf("unexpected remove request: %+v", req)
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		case "/cluster/nodes/node-a/raft-remove":
			raftRemoveCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeB.Close()

	result, err := replaceClusterNode(map[string]string{
		"node-a": nodeA.URL,
		"node-b": nodeB.URL,
		"node-c": nodeC.URL,
		"node-d": nodeD.URL,
	}, replaceNodeOptions{
		OldNode: "node-a",
		NewNode: "node-d",
	})
	if err != nil {
		t.Fatalf("replace node: %v", err)
	}
	if result.OldNode != "node-a" || result.NewNode != "node-d" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if len(result.Shards) != 1 || result.Shards[0] != "users" {
		t.Fatalf("unexpected replacement shards: %+v", result.Shards)
	}
	if raftAddCalls.Load() != 1 || addReplicaCalls.Load() != 1 || drainCalls.Load() != 1 || removeReplicaCalls.Load() != 1 || raftRemoveCalls.Load() != 1 {
		t.Fatalf("unexpected call counts raftAdd=%d add=%d drain=%d remove=%d raftRemove=%d",
			raftAddCalls.Load(), addReplicaCalls.Load(), drainCalls.Load(), removeReplicaCalls.Load(), raftRemoveCalls.Load())
	}
}

func TestReplaceClusterNodeDryRunOnlyPreflights(t *testing.T) {
	var mutationCalls atomic.Int32

	nodeA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID: "node-a",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         false,
					WriteAvailable: false,
					Health:         "follower",
					LeaderKnown:    true,
				},
			})
		default:
			mutationCalls.Add(1)
			http.NotFound(w, r)
		}
	}))
	defer nodeA.Close()

	nodeC := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID: "node-c",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         false,
					WriteAvailable: false,
					Health:         "follower",
					LeaderKnown:    true,
				},
			})
		default:
			mutationCalls.Add(1)
			http.NotFound(w, r)
		}
	}))
	defer nodeC.Close()

	nodeD := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID: "node-d",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         false,
					WriteAvailable: false,
					Health:         "follower",
					LeaderKnown:    true,
				},
			})
		default:
			mutationCalls.Add(1)
			http.NotFound(w, r)
		}
	}))
	defer nodeD.Close()

	nodeB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID: "node-b",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         true,
					WriteAvailable: true,
					Health:         "ready",
					LeaderKnown:    true,
				},
			})
		case "/cluster/shards":
			_ = json.NewEncoder(w).Encode([]lsm.ShardStatus{
				{
					ID:     "users",
					Leader: "node-a",
					Replicas: []lsm.ReplicaStatus{
						{NodeID: "node-a", Role: "leader", Healthy: true},
						{NodeID: "node-b", Role: "follower", Healthy: true},
						{NodeID: "node-c", Role: "follower", Healthy: true},
					},
				},
			})
		default:
			mutationCalls.Add(1)
			http.NotFound(w, r)
		}
	}))
	defer nodeB.Close()

	result, err := replaceClusterNode(map[string]string{
		"node-a": nodeA.URL,
		"node-b": nodeB.URL,
		"node-c": nodeC.URL,
		"node-d": nodeD.URL,
	}, replaceNodeOptions{
		OldNode: "node-a",
		NewNode: "node-d",
		DryRun:  true,
	})
	if err != nil {
		t.Fatalf("replace node dry-run: %v", err)
	}
	if !result.DryRun {
		t.Fatalf("expected dry-run result")
	}
	if result.Preflight.WriteLeader != "node-b" {
		t.Fatalf("expected node-b write leader, got %+v", result.Preflight)
	}
	if len(result.Shards) != 1 || result.Shards[0] != "users" {
		t.Fatalf("unexpected replacement shards: %+v", result.Shards)
	}
	if len(result.Steps) != 0 || result.Drain.Target != "" {
		t.Fatalf("dry-run should not submit replacement steps: %+v", result)
	}
	if mutationCalls.Load() != 0 {
		t.Fatalf("dry-run submitted %d mutation calls", mutationCalls.Load())
	}
}

func TestReplaceClusterNodePreflightRequiresReachableReplacement(t *testing.T) {
	nodeA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID:           "node-a",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{Health: "follower", LeaderKnown: true},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeA.Close()

	nodeB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID: "node-b",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         true,
					WriteAvailable: true,
					Health:         "ready",
					LeaderKnown:    true,
				},
			})
		case "/cluster/shards":
			_ = json.NewEncoder(w).Encode([]lsm.ShardStatus{
				{
					ID:     "users",
					Leader: "node-a",
					Replicas: []lsm.ReplicaStatus{
						{NodeID: "node-a", Role: "leader", Healthy: true},
						{NodeID: "node-b", Role: "follower", Healthy: true},
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeB.Close()

	nodeC := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	nodeC.Close()

	_, err := replaceClusterNode(map[string]string{
		"node-a": nodeA.URL,
		"node-b": nodeB.URL,
		"node-c": nodeC.URL,
	}, replaceNodeOptions{
		OldNode: "node-a",
		NewNode: "node-c",
		DryRun:  true,
	})
	if err == nil {
		t.Fatalf("expected unreachable replacement error")
	}
	if !strings.Contains(err.Error(), "node \"node-c\" endpoint") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPlanReplacementNodeSelectsUnavailableCandidate(t *testing.T) {
	nodeA := httptest.NewServer(http.NotFoundHandler())
	nodeAURL := nodeA.URL
	nodeA.Close()

	nodeD := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID:           "node-d",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{Health: "follower", LeaderKnown: true},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeD.Close()

	nodeB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID: "node-b",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         true,
					WriteAvailable: true,
					Health:         "ready",
					LeaderKnown:    true,
				},
			})
		case "/cluster/shards":
			_ = json.NewEncoder(w).Encode([]lsm.ShardStatus{
				{
					ID:     "users",
					Leader: "node-b",
					Replicas: []lsm.ReplicaStatus{
						{NodeID: "node-a", Role: "follower", Healthy: false},
						{NodeID: "node-b", Role: "leader", Healthy: true},
						{NodeID: "node-c", Role: "follower", Healthy: true},
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeB.Close()

	nodeC := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID:           "node-c",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{Health: "follower", LeaderKnown: true},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeC.Close()

	result, err := planReplacementNode(map[string]string{
		"node-a": nodeAURL,
		"node-b": nodeB.URL,
		"node-c": nodeC.URL,
		"node-d": nodeD.URL,
	}, replaceNodeOptions{
		NewNode:         "node-d",
		OperationPrefix: "repair-node-a-node-d",
		DryRun:          true,
	})
	if err != nil {
		t.Fatalf("plan replacement: %v", err)
	}
	if result.OldNode != "node-a" || result.Reason != "status-error" {
		t.Fatalf("unexpected candidate: %+v", result)
	}
	if len(result.Shards) != 1 || result.Shards[0] != "users" {
		t.Fatalf("unexpected shards: %+v", result.Shards)
	}
	if !containsString(result.DryRunCommand, "--dry-run") {
		t.Fatalf("expected dry-run command, got %+v", result.DryRunCommand)
	}
	if containsString(result.ApplyCommand, "--dry-run") {
		t.Fatalf("apply command must not include --dry-run: %+v", result.ApplyCommand)
	}
	if !containsString(result.ApplyCommand, "node-d="+nodeD.URL) {
		t.Fatalf("expected replacement endpoint in command: %+v", result.ApplyCommand)
	}
}

func TestReplaceNodeCommandArgsPreservesConfigEndpointSource(t *testing.T) {
	args := replaceNodeCommandArgs(map[string]string{
		"node-a": "http://internal-a:8080",
		"node-b": "http://internal-b:8080",
		"node-c": "http://internal-c:8080",
		"node-d": "http://internal-d:8080",
	}, replaceNodeOptions{
		OldNode:         "node-a",
		NewNode:         "node-d",
		ShardIDs:        []string{"users"},
		OperationPrefix: "repair-node-a-node-d",
		DryRun:          true,
		CommandEndpoints: replacementCommandEndpointSource{
			ConfigPath: "/tmp/lsmctl.yaml",
			Addr:       "http://127.0.0.1:8081",
			Overrides: nodeEndpointFlags{
				"node-d": "http://127.0.0.1:8083",
			},
		},
	})
	for _, want := range []string{
		"--dry-run",
		"--operation-prefix",
		"repair-node-a-node-d",
		"--config",
		"/tmp/lsmctl.yaml",
		"--addr",
		"http://127.0.0.1:8081",
		"--node-endpoint",
		"node-d=http://127.0.0.1:8083",
		"--shard",
		"users",
	} {
		if !containsString(args, want) {
			t.Fatalf("expected generated command to contain %q: %+v", want, args)
		}
	}
	if containsString(args, "node-a=http://internal-a:8080") {
		t.Fatalf("generated command should preserve config source instead of expanding resolved endpoints: %+v", args)
	}
}

func TestPlanReplacementNodeRequiresExplicitOldNodeForMultipleCandidates(t *testing.T) {
	nodeA := httptest.NewServer(http.NotFoundHandler())
	nodeAURL := nodeA.URL
	nodeA.Close()
	nodeB := httptest.NewServer(http.NotFoundHandler())
	nodeBURL := nodeB.URL
	nodeB.Close()

	nodeD := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID:           "node-d",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{Health: "follower", LeaderKnown: true},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeD.Close()

	_, err := planReplacementNode(map[string]string{
		"node-a": nodeAURL,
		"node-b": nodeBURL,
		"node-d": nodeD.URL,
	}, replaceNodeOptions{
		NewNode: "node-d",
		DryRun:  true,
	})
	if err == nil {
		t.Fatalf("expected multiple candidate error")
	}
	if !strings.Contains(err.Error(), "multiple unavailable replacement candidates") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPlanReplacementNodeRejectsInsufficientHealthyRemainingReplicas(t *testing.T) {
	nodeA := httptest.NewServer(http.NotFoundHandler())
	nodeAURL := nodeA.URL
	nodeA.Close()
	nodeC := httptest.NewServer(http.NotFoundHandler())
	nodeCURL := nodeC.URL
	nodeC.Close()

	nodeD := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID:           "node-d",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{Health: "follower", LeaderKnown: true},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeD.Close()

	nodeB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID: "node-b",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         true,
					WriteAvailable: true,
					Health:         "ready",
					LeaderKnown:    true,
				},
			})
		case "/cluster/shards":
			_ = json.NewEncoder(w).Encode([]lsm.ShardStatus{
				{
					ID:     "users",
					Leader: "node-b",
					Replicas: []lsm.ReplicaStatus{
						{NodeID: "node-a", Role: "follower", Healthy: false},
						{NodeID: "node-b", Role: "leader", Healthy: true},
						{NodeID: "node-c", Role: "follower", Healthy: false},
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeB.Close()

	_, err := planReplacementNode(map[string]string{
		"node-a": nodeAURL,
		"node-b": nodeB.URL,
		"node-c": nodeCURL,
		"node-d": nodeD.URL,
	}, replaceNodeOptions{
		OldNode: "node-a",
		NewNode: "node-d",
		DryRun:  true,
	})
	if err == nil {
		t.Fatalf("expected replacement quorum policy error")
	}
	if !strings.Contains(err.Error(), "replacement quorum policy failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestApplyPlannedReplacementExecutesSelectedCandidate(t *testing.T) {
	var raftAddCalls atomic.Int32
	var raftRemoveCalls atomic.Int32
	var addReplicaCalls atomic.Int32
	var drainCalls atomic.Int32
	var removeReplicaCalls atomic.Int32

	nodeA := httptest.NewServer(http.NotFoundHandler())
	nodeAURL := nodeA.URL
	nodeA.Close()

	nodeD := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID:           "node-d",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{Health: "follower", LeaderKnown: true},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeD.Close()

	nodeB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID: "node-b",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         true,
					WriteAvailable: true,
					Health:         "ready",
					LeaderKnown:    true,
				},
			})
		case "/cluster/shards":
			replicas := []lsm.ReplicaStatus{
				{NodeID: "node-a", Role: "follower", Healthy: false},
				{NodeID: "node-b", Role: "leader", Healthy: true},
				{NodeID: "node-c", Role: "follower", Healthy: true},
			}
			if addReplicaCalls.Load() > 0 {
				replicas = append(replicas, lsm.ReplicaStatus{NodeID: "node-d", Role: "follower", Healthy: true})
			}
			if removeReplicaCalls.Load() > 0 {
				next := replicas[:0]
				for _, replica := range replicas {
					if replica.NodeID != "node-a" {
						next = append(next, replica)
					}
				}
				replicas = next
			}
			_ = json.NewEncoder(w).Encode([]lsm.ShardStatus{
				{
					ID:       "users",
					Leader:   "node-b",
					Replicas: replicas,
				},
			})
		case "/cluster/nodes/node-d/raft-add":
			raftAddCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		case "/cluster/shards/users/add-replica":
			addReplicaCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		case "/cluster/nodes/node-a/drain":
			drainCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		case "/cluster/shards/users/remove-replica":
			removeReplicaCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		case "/cluster/nodes/node-a/raft-remove":
			raftRemoveCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeB.Close()

	nodeC := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID:           "node-c",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{Health: "follower", LeaderKnown: true},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeC.Close()

	result, err := applyPlannedReplacement(map[string]string{
		"node-a": nodeAURL,
		"node-b": nodeB.URL,
		"node-c": nodeC.URL,
		"node-d": nodeD.URL,
	}, replaceNodeOptions{
		NewNode:         "node-d",
		OperationPrefix: "repair-node-a-node-d",
	})
	if err != nil {
		t.Fatalf("apply replacement: %v", err)
	}
	if result.Plan.OldNode != "node-a" || result.Plan.Reason != "status-error" {
		t.Fatalf("unexpected plan: %+v", result.Plan)
	}
	if result.Result.OldNode != "node-a" || result.Result.NewNode != "node-d" {
		t.Fatalf("unexpected result: %+v", result.Result)
	}
	if raftAddCalls.Load() != 1 || addReplicaCalls.Load() != 1 || drainCalls.Load() != 1 || removeReplicaCalls.Load() != 1 || raftRemoveCalls.Load() != 1 {
		t.Fatalf("unexpected call counts raftAdd=%d add=%d drain=%d remove=%d raftRemove=%d",
			raftAddCalls.Load(), addReplicaCalls.Load(), drainCalls.Load(), removeReplicaCalls.Load(), raftRemoveCalls.Load())
	}
}

func TestWriteKVPutClusterTransfersShardToCurrentWriteLeader(t *testing.T) {
	var transferCalls atomic.Int32
	var putCalls atomic.Int32

	nodeA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID: "node-a",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         false,
					WriteAvailable: false,
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeA.Close()

	nodeB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cluster/status":
			_ = json.NewEncoder(w).Encode(lsm.ClusterStatus{
				NodeID: "node-b",
				CommitLogRuntime: lsm.CommitLogRuntimeStatus{
					Leader:         true,
					WriteAvailable: true,
				},
			})
		case "/cluster/shards":
			_ = json.NewEncoder(w).Encode([]lsm.ShardStatus{
				{
					ID:       "users",
					StartKey: []byte("a"),
					EndKey:   []byte("z"),
					Leader:   "node-a",
				},
			})
		case "/cluster/shards/users/transfer-leader":
			transferCalls.Add(1)
			var req targetRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode transfer: %v", err)
			}
			if req.Target != "node-b" {
				t.Fatalf("expected transfer target node-b, got %q", req.Target)
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		case "/kv/put":
			putCalls.Add(1)
			var req kvWriteRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode put: %v", err)
			}
			if req.KeyBase64 != base64.StdEncoding.EncodeToString([]byte("k")) {
				t.Fatalf("unexpected key %q", req.KeyBase64)
			}
			_ = json.NewEncoder(w).Encode(lsm.WriteRequestStatus{
				RequestID:   "routed-put",
				Operation:   "put",
				Consistency: req.Consistency,
				State:       lsm.WriteRequestCommitted,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer nodeB.Close()

	status, err := writeKVPutWithCluster("", "", []byte("k"), []byte("v"), lsm.WriteConsistencyLocalCommitted, clusterWriteOptions{
		Enabled: true,
		NodeEndpoints: map[string]string{
			"node-a": nodeA.URL,
			"node-b": nodeB.URL,
		},
	})
	if err != nil {
		t.Fatalf("cluster put: %v", err)
	}
	if status.RequestID != "routed-put" || status.State != lsm.WriteRequestCommitted {
		t.Fatalf("unexpected status: %+v", status)
	}
	if transferCalls.Load() != 1 {
		t.Fatalf("expected one transfer call, got %d", transferCalls.Load())
	}
	if putCalls.Load() != 1 {
		t.Fatalf("expected one routed put, got %d", putCalls.Load())
	}
}

func TestToRaftOptionsIncludesPeers(t *testing.T) {
	got := toRaftOptions(serverconfig.RaftConfig{
		Join:  true,
		Peers: []string{"node-a", "node-b", "node-c"},
	})
	if got == nil {
		t.Fatalf("expected raft options")
	}
	if len(got.Peers) != 3 {
		t.Fatalf("expected peers length 3, got %d", len(got.Peers))
	}
	if got.Peers[2] != "node-c" {
		t.Fatalf("expected node-c peer, got %q", got.Peers[2])
	}
	if !got.Join {
		t.Fatalf("expected join mode")
	}
}

func TestToCommitLogOptionsBuildsRaftHTTPTransport(t *testing.T) {
	got, err := toCommitLogOptions(
		serverconfig.CommitLogConfig{
			Provider: string(lsm.CommitLogProviderEtcdRaft),
			SnapshotPolicy: serverconfig.CommitLogSnapshotPolicy{
				AppliedEntries: 1024,
				RetainEntries:  128,
			},
		},
		serverconfig.RaftConfig{
			PeerURLs: map[string]string{"node-b": "http://127.0.0.1:9091"},
		},
	)
	if err != nil {
		t.Fatalf("to commit log options: %v", err)
	}
	if got == nil {
		t.Fatalf("expected commit log options")
	}
	if got.Provider != lsm.CommitLogProviderEtcdRaft {
		t.Fatalf("expected etcd raft provider, got %q", got.Provider)
	}
	if got.Transport == nil {
		t.Fatalf("expected raft http transport")
	}
	if got.SnapshotPolicy.AppliedEntries != 1024 || got.SnapshotPolicy.RetainEntries != 128 {
		t.Fatalf("unexpected snapshot policy: %+v", got.SnapshotPolicy)
	}
}

func TestToCommitLogOptionsBuildsTransportWithJoinPeerURLs(t *testing.T) {
	got, err := toCommitLogOptions(
		serverconfig.CommitLogConfig{Provider: string(lsm.CommitLogProviderEtcdRaft)},
		serverconfig.RaftConfig{
			PeerURLs:     map[string]string{"node-b": "http://127.0.0.1:9091"},
			JoinPeerURLs: map[string]string{"node-c": "http://127.0.0.1:9092"},
		},
	)
	if err != nil {
		t.Fatalf("to commit log options: %v", err)
	}
	if got == nil || got.Transport == nil {
		t.Fatalf("expected raft transport")
	}
}

func TestToCommitLogOptionsBuildsTransportWithPeerURLFile(t *testing.T) {
	peerID := lsm.RaftPeerID("node-c")
	got := make(chan struct{}, 1)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cluster/raft/messages" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		got <- struct{}{}
		_ = json.NewEncoder(w).Encode(map[string]bool{"accepted": true})
	}))
	defer peer.Close()

	path := t.TempDir() + "/peers.yaml"
	if err := os.WriteFile(path, []byte("node-c: "+peer.URL+"\n"), 0o644); err != nil {
		t.Fatalf("write peer url file: %v", err)
	}
	opts, err := toCommitLogOptions(
		serverconfig.CommitLogConfig{Provider: string(lsm.CommitLogProviderEtcdRaft)},
		serverconfig.RaftConfig{
			PeerURLFile: path,
		},
	)
	if err != nil {
		t.Fatalf("to commit log options: %v", err)
	}
	if opts == nil || opts.Transport == nil {
		t.Fatalf("expected raft transport")
	}
	if err := opts.Transport.Send(context.Background(), []lsm.RaftPeerMessage{
		{From: lsm.RaftPeerID("node-a"), To: peerID, Type: "MsgApp"},
	}); err != nil {
		t.Fatalf("send peer message: %v", err)
	}
	select {
	case <-got:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for peer message")
	}
}

func TestToRaftPeerURLMapUsesStablePeerIDs(t *testing.T) {
	got := toRaftPeerURLMap(map[string]string{
		"node-b": "http://127.0.0.1:9091",
	})
	if got[lsm.RaftPeerID("node-b")] != "http://127.0.0.1:9091" {
		t.Fatalf("expected node-b url keyed by stable raft id")
	}
}

func TestParseBytesFlag(t *testing.T) {
	got, err := parseKeyFlag("hello", "")
	if err != nil {
		t.Fatalf("parse text key: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("expected text key hello, got %q", got)
	}
	got, err = parseValueFlag("", base64.StdEncoding.EncodeToString([]byte("world")))
	if err != nil {
		t.Fatalf("parse base64 value: %v", err)
	}
	if string(got) != "world" {
		t.Fatalf("expected base64 value world, got %q", got)
	}
	if _, err := parseKeyFlag("a", base64.StdEncoding.EncodeToString([]byte("a"))); err == nil {
		t.Fatalf("expected conflict error")
	}
	if _, err := parseKeyFlag("", "%%%"); err == nil {
		t.Fatalf("expected invalid base64 error")
	}
	if _, err := parseKeyFlag("", ""); err == nil {
		t.Fatalf("expected required key error")
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func TestReadKVRemote(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/kv/get" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("key_base64"); got != base64.StdEncoding.EncodeToString([]byte("k")) {
			t.Fatalf("unexpected key query %q", got)
		}
		_ = json.NewEncoder(w).Encode(kvGetResult{
			Found:       true,
			KeyBase64:   base64.StdEncoding.EncodeToString([]byte("k")),
			ValueBase64: base64.StdEncoding.EncodeToString([]byte("v")),
			Seq:         7,
		})
	}))
	defer server.Close()

	got, err := readKV(server.URL, "", []byte("k"))
	if err != nil {
		t.Fatalf("read kv: %v", err)
	}
	if !got.Found || got.Seq != 7 {
		t.Fatalf("unexpected get result: %+v", got)
	}
	value, err := base64.StdEncoding.DecodeString(got.ValueBase64)
	if err != nil {
		t.Fatalf("decode value: %v", err)
	}
	if string(value) != "v" {
		t.Fatalf("expected value v, got %q", value)
	}
}

func TestReadKVRemoteNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer server.Close()

	got, err := readKV(server.URL, "", []byte("missing"))
	if err != nil {
		t.Fatalf("read missing kv: %v", err)
	}
	if got.Found {
		t.Fatalf("expected missing key to return found=false")
	}
}

func TestReadKVFromClusterUsesFirstReachableEndpoint(t *testing.T) {
	down := httptest.NewServer(http.NotFoundHandler())
	downURL := down.URL
	down.Close()

	okServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/kv/get" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(kvGetResult{
			Found:       true,
			KeyBase64:   base64.StdEncoding.EncodeToString([]byte("k")),
			ValueBase64: base64.StdEncoding.EncodeToString([]byte("v")),
			Seq:         9,
		})
	}))
	defer okServer.Close()

	got, err := readKVFromCluster(map[string]string{
		"node-a": downURL,
		"node-b": okServer.URL,
	}, []byte("k"))
	if err != nil {
		t.Fatalf("cluster read: %v", err)
	}
	if !got.Found || got.Seq != 9 {
		t.Fatalf("unexpected cluster get result: %+v", got)
	}
}

func TestReadKVRangeRemote(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/kv/range" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("start_key_base64"); got != base64.StdEncoding.EncodeToString([]byte("a")) {
			t.Fatalf("unexpected start query %q", got)
		}
		if got := r.URL.Query().Get("end_key_base64"); got != base64.StdEncoding.EncodeToString([]byte("z")) {
			t.Fatalf("unexpected end query %q", got)
		}
		if got := r.URL.Query().Get("limit"); got != "2" {
			t.Fatalf("unexpected limit query %q", got)
		}
		_ = json.NewEncoder(w).Encode(kvRangeResult{
			Entries: []kvRangeEntry{
				{
					KeyBase64:   base64.StdEncoding.EncodeToString([]byte("a")),
					ValueBase64: base64.StdEncoding.EncodeToString([]byte("1")),
					Seq:         1,
				},
			},
			Limit: 2,
		})
	}))
	defer server.Close()

	got, err := readKVRange(server.URL, "", []byte("a"), []byte("z"), 2)
	if err != nil {
		t.Fatalf("read range: %v", err)
	}
	if len(got.Entries) != 1 || got.Limit != 2 {
		t.Fatalf("unexpected range result: %+v", got)
	}
}

func TestReadKVRangeFromClusterUsesFirstReachableEndpoint(t *testing.T) {
	down := httptest.NewServer(http.NotFoundHandler())
	downURL := down.URL
	down.Close()

	okServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/kv/range" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(kvRangeResult{
			Entries: []kvRangeEntry{
				{
					KeyBase64:   base64.StdEncoding.EncodeToString([]byte("k")),
					ValueBase64: base64.StdEncoding.EncodeToString([]byte("v")),
					Seq:         10,
				},
			},
			Limit: 1,
		})
	}))
	defer okServer.Close()

	got, err := readKVRangeFromCluster(map[string]string{
		"node-a": downURL,
		"node-b": okServer.URL,
	}, []byte("k"), []byte("l"), 1)
	if err != nil {
		t.Fatalf("cluster range: %v", err)
	}
	if len(got.Entries) != 1 || got.Entries[0].Seq != 10 {
		t.Fatalf("unexpected cluster range result: %+v", got)
	}
}

func TestReadKVRangeRejectsInvalidLimit(t *testing.T) {
	if _, err := readKVRange("http://example.test", "", nil, nil, 0); err == nil {
		t.Fatalf("expected limit error")
	}
}

func TestWriteKVPutRemote(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/kv/put" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		var req kvWriteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.KeyBase64 != base64.StdEncoding.EncodeToString([]byte("k")) {
			t.Fatalf("unexpected key: %+v", req)
		}
		if req.ValueBase64 != base64.StdEncoding.EncodeToString([]byte("v")) {
			t.Fatalf("unexpected value: %+v", req)
		}
		if req.Consistency != lsm.WriteConsistencyLocalCommitted {
			t.Fatalf("unexpected consistency: %+v", req)
		}
		_ = json.NewEncoder(w).Encode(lsm.WriteRequestStatus{
			RequestID:   "req-1",
			Operation:   "put",
			Consistency: lsm.WriteConsistencyLocalCommitted,
			State:       lsm.WriteRequestCommitted,
		})
	}))
	defer server.Close()

	got, err := writeKVPut(server.URL, "", []byte("k"), []byte("v"), lsm.WriteConsistencyLocalCommitted)
	if err != nil {
		t.Fatalf("put kv: %v", err)
	}
	if got.RequestID != "req-1" || got.State != lsm.WriteRequestCommitted {
		t.Fatalf("unexpected status: %+v", got)
	}
}

func TestWriteKVPutRemoteAccepted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/kv/put" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		var req kvWriteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.Consistency != lsm.WriteConsistencyAccepted {
			t.Fatalf("unexpected consistency: %+v", req)
		}
		_ = json.NewEncoder(w).Encode(lsm.WriteRequestStatus{
			RequestID:   "req-async",
			Operation:   "put",
			Consistency: lsm.WriteConsistencyAccepted,
			State:       lsm.WriteRequestPending,
		})
	}))
	defer server.Close()

	got, err := writeKVPut(server.URL, "", []byte("k"), []byte("v"), lsm.WriteConsistencyAccepted)
	if err != nil {
		t.Fatalf("put kv: %v", err)
	}
	if got.RequestID != "req-async" || got.State != lsm.WriteRequestPending {
		t.Fatalf("unexpected status: %+v", got)
	}
}

func TestWriteKVDeleteRemote(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/kv/delete" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		var req kvWriteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.KeyBase64 != base64.StdEncoding.EncodeToString([]byte("k")) {
			t.Fatalf("unexpected key: %+v", req)
		}
		_ = json.NewEncoder(w).Encode(lsm.WriteRequestStatus{
			RequestID:   "req-2",
			Operation:   "delete",
			Consistency: lsm.WriteConsistencyLocalCommitted,
			State:       lsm.WriteRequestCommitted,
		})
	}))
	defer server.Close()

	got, err := writeKVDelete(server.URL, "", []byte("k"), lsm.WriteConsistencyLocalCommitted)
	if err != nil {
		t.Fatalf("delete kv: %v", err)
	}
	if got.RequestID != "req-2" || got.Operation != "delete" {
		t.Fatalf("unexpected status: %+v", got)
	}
}

func TestReadWriteStatusRemote(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/kv/write-status/req-3" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(lsm.WriteRequestStatus{
			RequestID:   "req-3",
			Operation:   "put",
			Consistency: lsm.WriteConsistencyAccepted,
			State:       lsm.WriteRequestCommitted,
		})
	}))
	defer server.Close()

	got, err := readWriteStatus(server.URL, "req-3")
	if err != nil {
		t.Fatalf("read write status: %v", err)
	}
	if got.RequestID != "req-3" || got.State != lsm.WriteRequestCommitted {
		t.Fatalf("unexpected status: %+v", got)
	}
}

func TestReadWriteStatusRequiresAddr(t *testing.T) {
	if _, err := readWriteStatus("", "req-3"); err == nil {
		t.Fatalf("expected addr error")
	}
}

func TestNormalizeHTTPBaseURL(t *testing.T) {
	if got := normalizeHTTPBaseURL("127.0.0.1:8080/"); got != "http://127.0.0.1:8080" {
		t.Fatalf("unexpected normalized url %q", got)
	}
	if got := normalizeHTTPBaseURL("https://example.test/"); got != "https://example.test" {
		t.Fatalf("unexpected normalized url %q", got)
	}
}
