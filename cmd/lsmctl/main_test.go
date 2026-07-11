package main

import (
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
