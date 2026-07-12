package main

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"lsmengine/pkg/lsm"
	serverconfig "lsmengine/pkg/lsm/server/config"
)

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

func TestToRaftOptionsIncludesPeers(t *testing.T) {
	got := toRaftOptions(serverconfig.RaftConfig{
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

func TestNormalizeHTTPBaseURL(t *testing.T) {
	if got := normalizeHTTPBaseURL("127.0.0.1:8080/"); got != "http://127.0.0.1:8080" {
		t.Fatalf("unexpected normalized url %q", got)
	}
	if got := normalizeHTTPBaseURL("https://example.test/"); got != "https://example.test" {
		t.Fatalf("unexpected normalized url %q", got)
	}
}
