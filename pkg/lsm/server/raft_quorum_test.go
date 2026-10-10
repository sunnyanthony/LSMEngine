package server

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"lsmengine/pkg/lsm"
)

func TestRaftQuorumWritesSurviveOneStoppedPeer(t *testing.T) {
	for _, stopLeader := range []bool{false, true} {
		name := "follower-stopped"
		if stopLeader {
			name = "leader-stopped"
		}
		t.Run(name, func(t *testing.T) {
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
				stores[i], err = lsm.New(lsm.Options{DataDir: t.TempDir(), NodeID: name,
					CommitLog: &lsm.CommitLogOptions{Provider: lsm.CommitLogProviderEtcdRaft, Transport: transport},
					Raft:      &lsm.RaftOptions{Peers: names},
					ShardMap:  []lsm.ShardConfig{{ID: "shared", Leader: "node-a", Replicas: names}},
				})
				if err != nil {
					t.Fatal(err)
				}
				servers[i].Config.Handler = NewHandler(stores[i])
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
				t.Fatal("healthy majority did not elect a leader")
				return -1
			}
			leader := awaitLeader(-1)
			endpoints := make(map[string]string)
			for i, name := range names {
				endpoints[name] = servers[i].URL
			}
			gateway, err := NewGateway(GatewayOptions{BootstrapURL: servers[leader].URL, NodeEndpoints: endpoints})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := gateway.Put(context.Background(), []byte("before"), []byte("retained"), lsm.WriteConsistencyLocalCommitted); err != nil {
				t.Fatal(err)
			}
			stopped := (leader + 1) % 3
			if stopLeader {
				stopped = leader
			}
			servers[stopped].Close()
			if err := stores[stopped].Close(); err != nil {
				t.Fatal(err)
			}
			leader = awaitLeader(stopped)
			if _, err := gateway.Put(context.Background(), []byte("after"), []byte("value"), lsm.WriteConsistencyLocalCommitted); err != nil {
				t.Fatalf("healthy quorum put failed: %v", err)
			}
			if _, err := gateway.Delete(context.Background(), []byte("before"), lsm.WriteConsistencyLocalCommitted); err != nil {
				t.Fatalf("healthy quorum delete failed: %v", err)
			}
			if err := stores[leader].TransferLeader("shared", names[leader]); err != nil {
				t.Fatalf("healthy quorum control failed: %v", err)
			}
			if stores[leader].ClusterStatus().CommitLogRuntime.DeliveryFailures == 0 {
				t.Fatal("failed deliveries were not observable")
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				matched := true
				for i, store := range stores {
					if i == stopped {
						continue
					}
					value, ok := store.Get([]byte("after"))
					_, before := store.Get([]byte("before"))
					if !ok || string(value.Value) != "value" || before || store.ClusterStatus().Revision != 1 {
						matched = false
					}
				}
				if matched {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("healthy replicas did not apply data/control")
				}
				time.Sleep(20 * time.Millisecond)
			}
			for i := range stores {
				if i != stopped && i != leader {
					servers[i].Close()
					if err := stores[i].Close(); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := stores[leader].Put([]byte("no-quorum"), []byte("must-not-apply")); err == nil {
				t.Fatal("minority acknowledged a write")
			}
			if _, ok := stores[leader].Get([]byte("no-quorum")); ok {
				t.Fatal("uncommitted minority write applied")
			}
		})
	}
}
