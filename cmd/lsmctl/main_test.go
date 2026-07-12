package main

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"syscall"
	"testing"

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
