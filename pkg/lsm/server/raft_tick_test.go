package server

import (
	"net/http/httptest"
	"testing"
	"time"

	"lsmengine/pkg/lsm"
)

func TestRaftTicksElectWithoutWritesAndReplaceStoppedLeader(t *testing.T) {
	names := []string{"node-a", "node-b", "node-c"}
	servers := make([]*httptest.Server, 3)
	stores := make([]*lsm.LSM, 3)
	urls := make(map[uint64]string)
	for i, name := range names {
		servers[i] = httptest.NewUnstartedServer(nil)
		urls[lsm.RaftPeerID(name)] = "http://" + servers[i].Listener.Addr().String()
	}
	defer func() {
		for _, server := range servers {
			server.Close()
		}
		for _, store := range stores {
			if store != nil {
				store.Close()
			}
		}
	}()
	for i, name := range names {
		transport, err := NewRaftHTTPTransport(RaftHTTPTransportOptions{PeerURLs: urls})
		if err != nil {
			t.Fatal(err)
		}
		store, err := lsm.New(lsm.Options{DataDir: t.TempDir(), NodeID: name,
			CommitLog: &lsm.CommitLogOptions{Provider: lsm.CommitLogProviderEtcdRaft, Transport: transport},
			Raft:      &lsm.RaftOptions{Peers: names}})
		if err != nil {
			t.Fatal(err)
		}
		stores[i] = store
		servers[i].Config.Handler = NewHandler(store)
		servers[i].Start()
	}
	awaitLeader := func(excluded int) int {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			leader, count := -1, 0
			for i, store := range stores {
				if i != excluded && store.ClusterStatus().CommitLogRuntime.Leader {
					leader = i
					count++
				}
			}
			if count == 1 {
				return leader
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("no unique raft leader without client writes")
		return -1
	}
	first := awaitLeader(-1)
	servers[first].Close()
	if err := stores[first].Close(); err != nil {
		t.Fatal(err)
	}
	second := awaitLeader(first)
	if second == first {
		t.Fatal("stopped node remained leader")
	}
}
