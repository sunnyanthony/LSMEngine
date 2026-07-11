//go:build test

package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"lsmengine/pkg/lsm"
	lsmserver "lsmengine/pkg/lsm/server"
)

type asyncInProcessRaftTransport struct {
	mu      sync.RWMutex
	nodes   map[uint64]*lsm.LSM
	errs    []error
	pending sync.WaitGroup
}

func newAsyncInProcessRaftTransport() *asyncInProcessRaftTransport {
	return &asyncInProcessRaftTransport{
		nodes: make(map[uint64]*lsm.LSM),
	}
}

func (t *asyncInProcessRaftTransport) Register(nodeID string, store *lsm.LSM) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[lsm.RaftPeerID(nodeID)] = store
}

func (t *asyncInProcessRaftTransport) Send(_ context.Context, messages []lsm.RaftPeerMessage) error {
	for _, message := range messages {
		msg := message
		msg.Payload = append([]byte(nil), message.Payload...)
		t.mu.RLock()
		target := t.nodes[msg.To]
		t.mu.RUnlock()
		if target == nil {
			return fmt.Errorf("target node %d not registered", msg.To)
		}
		t.pending.Add(1)
		go func() {
			defer t.pending.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := target.HandlePeerMessages(ctx, []lsm.RaftPeerMessage{msg}); err != nil {
				t.recordErr(err)
			}
		}()
	}
	return nil
}

func (t *asyncInProcessRaftTransport) Wait() {
	t.pending.Wait()
}

func (t *asyncInProcessRaftTransport) Errors() []error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]error, len(t.errs))
	copy(out, t.errs)
	return out
}

func (t *asyncInProcessRaftTransport) recordErr(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.errs = append(t.errs, err)
}

func TestEtcdRaftThreeNodeWriteReplicatesToFollowers(t *testing.T) {
	peers := []string{"node-a", "node-b", "node-c"}
	transport := newAsyncInProcessRaftTransport()
	stores := make(map[string]*lsm.LSM, len(peers))
	for _, nodeID := range peers {
		store, err := lsm.New(lsm.Options{
			DataDir:   t.TempDir(),
			NodeID:    nodeID,
			ClusterID: "cluster-a",
			CommitLog: &lsm.CommitLogOptions{
				Provider:  lsm.CommitLogProviderEtcdRaft,
				Transport: transport,
			},
			Raft: &lsm.RaftOptions{
				Peers: peers,
			},
			ShardMap: []lsm.ShardConfig{
				{
					ID:       "users",
					StartKey: []byte("a"),
					EndKey:   []byte("z"),
					Replicas: peers,
					Leader:   "node-a",
				},
			},
		})
		if err != nil {
			t.Fatalf("new %s: %v", nodeID, err)
		}
		stores[nodeID] = store
		transport.Register(nodeID, store)
	}
	t.Cleanup(func() {
		for _, nodeID := range peers {
			if err := stores[nodeID].Close(); err != nil {
				t.Fatalf("close %s: %v", nodeID, err)
			}
		}
	})

	if err := stores["node-a"].Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put on leader: %v", err)
	}
	transport.Wait()
	if errs := transport.Errors(); len(errs) != 0 {
		t.Fatalf("transport errors: %v", errs)
	}

	for _, nodeID := range peers {
		nodeID := nodeID
		t.Run(nodeID, func(t *testing.T) {
			eventually(t, 2*time.Second, func() bool {
				entry, ok := stores[nodeID].Get([]byte("k"))
				return ok && string(entry.Value) == "v"
			})
		})
	}
}

func TestEtcdRaftThreeNodeHTTPWriteReplicatesToFollowers(t *testing.T) {
	cluster := newThreeNodeHTTPRaftCluster(t)

	if err := cluster.stores["node-a"].Put([]byte("k"), []byte("v-http")); err != nil {
		t.Fatalf("put on leader: %v", err)
	}
	for _, nodeID := range cluster.peers {
		nodeID := nodeID
		t.Run(nodeID, func(t *testing.T) {
			eventually(t, 2*time.Second, func() bool {
				entry, ok := cluster.stores[nodeID].Get([]byte("k"))
				return ok && string(entry.Value) == "v-http"
			})
		})
	}
	assertNoAsyncRaftErrors(t, cluster.errCh)
}

func TestEtcdRaftThreeNodeHTTPFollowerWriteReturnsLeaderHint(t *testing.T) {
	cluster := newThreeNodeHTTPRaftCluster(t)
	if err := cluster.stores["node-a"].Put([]byte("seed"), []byte("v")); err != nil {
		t.Fatalf("seed leader: %v", err)
	}
	eventually(t, 2*time.Second, func() bool {
		entry, ok := cluster.stores["node-b"].Get([]byte("seed"))
		return ok && string(entry.Value) == "v"
	})
	respStatus, err := http.Get(cluster.urls["node-b"] + "/cluster/status")
	if err != nil {
		t.Fatalf("get follower status: %v", err)
	}
	defer respStatus.Body.Close()
	if respStatus.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", respStatus.StatusCode)
	}
	var followerStatus lsm.ClusterStatus
	if err := json.NewDecoder(respStatus.Body).Decode(&followerStatus); err != nil {
		t.Fatalf("decode follower status: %v", err)
	}
	if followerStatus.CommitLogRuntime.Health != "follower" {
		t.Fatalf("expected follower commit-log health, got %+v", followerStatus.CommitLogRuntime)
	}
	if followerStatus.CommitLogRuntime.WriteAvailable {
		t.Fatalf("expected follower write_available=false, got %+v", followerStatus.CommitLogRuntime)
	}
	if !followerStatus.CommitLogRuntime.LeaderKnown {
		t.Fatalf("expected follower to know raft leader, got %+v", followerStatus.CommitLogRuntime)
	}

	body := bytes.NewBufferString(`{"key_base64":"` + base64.StdEncoding.EncodeToString([]byte("m")) + `","value_base64":"` + base64.StdEncoding.EncodeToString([]byte("from-follower")) + `","consistency":"local_committed"}`)
	resp, err := http.Post(cluster.urls["node-b"]+"/kv/put", "application/json", body)
	if err != nil {
		t.Fatalf("post follower write: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected status 409, got %d", resp.StatusCode)
	}
	var out struct {
		Code      string `json:"code"`
		Retryable bool   `json:"retryable"`
		Route     *struct {
			ShardID string `json:"shard_id"`
			Leader  string `json:"leader"`
		} `json:"route"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode follower write error: %v", err)
	}
	if out.Code != "not_leader" {
		t.Fatalf("expected not_leader code, got %s", out.Code)
	}
	if !out.Retryable {
		t.Fatalf("expected retryable=true")
	}
	if out.Route == nil || out.Route.ShardID != "users" || out.Route.Leader != "node-a" {
		t.Fatalf("unexpected route hint: %+v", out.Route)
	}
}

func TestEtcdRaftThreeNodeHTTPGatewayRoutesWriteToLeader(t *testing.T) {
	cluster := newThreeNodeHTTPRaftCluster(t)
	gateway, err := lsmserver.NewGateway(lsmserver.GatewayOptions{
		BootstrapURL: cluster.urls["node-b"],
		NodeEndpoints: map[string]string{
			"node-a": cluster.urls["node-a"],
			"node-b": cluster.urls["node-b"],
			"node-c": cluster.urls["node-c"],
		},
		MaxWriteAttempts: 3,
	})
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}

	status, err := gateway.Put(context.Background(), []byte("gw"), []byte("routed"), lsm.WriteConsistencyLocalCommitted)
	if err != nil {
		t.Fatalf("gateway put: %v", err)
	}
	if status.State != lsm.WriteRequestCommitted {
		t.Fatalf("expected committed status, got %+v", status)
	}
	for _, nodeID := range cluster.peers {
		nodeID := nodeID
		t.Run(nodeID, func(t *testing.T) {
			eventually(t, 2*time.Second, func() bool {
				entry, ok := cluster.stores[nodeID].Get([]byte("gw"))
				return ok && string(entry.Value) == "routed"
			})
		})
	}
	assertNoAsyncRaftErrors(t, cluster.errCh)
}

type threeNodeHTTPRaftCluster struct {
	peers  []string
	urls   map[string]string
	stores map[string]*lsm.LSM
	errCh  chan error
}

func newThreeNodeHTTPRaftCluster(t *testing.T) threeNodeHTTPRaftCluster {
	t.Helper()
	peers := []string{"node-a", "node-b", "node-c"}
	listeners := make(map[string]net.Listener, len(peers))
	peerURLs := make(map[uint64]string, len(peers))
	urls := make(map[string]string, len(peers))
	for _, nodeID := range peers {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen %s: %v", nodeID, err)
		}
		listeners[nodeID] = listener
		urls[nodeID] = "http://" + listener.Addr().String()
		peerURLs[lsm.RaftPeerID(nodeID)] = urls[nodeID]
	}

	errCh := make(chan error, 32)
	stores := make(map[string]*lsm.LSM, len(peers))
	servers := make(map[string]*http.Server, len(peers))
	for _, nodeID := range peers {
		transport, err := lsmserver.NewRaftHTTPTransport(lsmserver.RaftHTTPTransportOptions{
			PeerURLs: peerURLs,
			OnError: func(err error) {
				select {
				case errCh <- err:
				default:
				}
			},
		})
		if err != nil {
			t.Fatalf("new http transport %s: %v", nodeID, err)
		}
		store, err := lsm.New(lsm.Options{
			DataDir:   t.TempDir(),
			NodeID:    nodeID,
			ClusterID: "cluster-a",
			CommitLog: &lsm.CommitLogOptions{
				Provider:  lsm.CommitLogProviderEtcdRaft,
				Transport: transport,
			},
			Raft: &lsm.RaftOptions{
				Peers: peers,
			},
			ShardMap: []lsm.ShardConfig{
				{
					ID:       "users",
					StartKey: []byte("a"),
					EndKey:   []byte("z"),
					Replicas: peers,
					Leader:   "node-a",
				},
			},
		})
		if err != nil {
			t.Fatalf("new %s: %v", nodeID, err)
		}
		stores[nodeID] = store
		server := &http.Server{Handler: lsmserver.NewHandler(store)}
		servers[nodeID] = server
		listener := listeners[nodeID]
		go func(nodeID string) {
			if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
				select {
				case errCh <- fmt.Errorf("%s serve: %w", nodeID, err):
				default:
				}
			}
		}(nodeID)
	}
	t.Cleanup(func() {
		for _, nodeID := range peers {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = servers[nodeID].Shutdown(ctx)
			cancel()
			if err := stores[nodeID].Close(); err != nil {
				t.Fatalf("close %s: %v", nodeID, err)
			}
		}
	})
	return threeNodeHTTPRaftCluster{
		peers:  peers,
		urls:   urls,
		stores: stores,
		errCh:  errCh,
	}
}

func assertNoAsyncRaftErrors(t *testing.T, errCh <-chan error) {
	t.Helper()
	select {
	case err := <-errCh:
		t.Fatalf("async raft error: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
}

func eventually(t *testing.T, timeout time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ok() {
		t.Fatalf("condition not met within %s", timeout)
	}
}
