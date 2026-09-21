package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"lsmengine/pkg/lsm"
)

func TestRaftHTTPTransportRealProvidersRoundTrip(t *testing.T) {
	a := httptest.NewUnstartedServer(nil)
	b := httptest.NewUnstartedServer(nil)
	defer a.Close()
	defer b.Close()
	urls := map[uint64]string{
		lsm.RaftPeerID("node-a"): "http://" + a.Listener.Addr().String(),
		lsm.RaftPeerID("node-b"): "http://" + b.Listener.Addr().String(),
	}
	stores := make([]*lsm.LSM, 0, 2)
	for i, node := range []string{"node-a", "node-b"} {
		transport, err := NewRaftHTTPTransport(RaftHTTPTransportOptions{PeerURLs: urls})
		if err != nil {
			t.Fatal(err)
		}
		store, err := lsm.New(lsm.Options{
			DataDir: t.TempDir(), NodeID: node,
			CommitLog: &lsm.CommitLogOptions{Provider: lsm.CommitLogProviderEtcdRaft, Transport: transport},
			Raft:      &lsm.RaftOptions{Peers: []string{"node-a", "node-b"}},
			ShardMap:  []lsm.ShardConfig{{ID: "shared", Leader: "node-a", Replicas: []string{"node-a", "node-b"}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		stores = append(stores, store)
		server := []*httptest.Server{a, b}[i]
		server.Config.Handler = NewHandler(store)
		server.Start()
	}
	if err := stores[0].Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("real-provider HTTP election/commit round trip: %v", err)
	}
	if !stores[0].ClusterStatus().CommitLogRuntime.Leader {
		t.Fatal("node-a failed to elect")
	}
	leader, ok := stores[0].Get([]byte("key"))
	if !ok {
		t.Fatal("leader did not apply committed value")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		follower, ok := stores[1].Get([]byte("key"))
		if ok && string(follower.Value) == "value" && follower.Seq == leader.Seq {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower did not apply committed entry: found=%v value=%q seq=%d; leader seq=%d, follower commit=%d",
				ok, follower.Value, follower.Seq, leader.Seq, stores[1].ClusterStatus().CommitLogRuntime.Index)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRaftHTTPTransportRejectsRedirectsAndFailures(t *testing.T) {
	for _, status := range []int{301, 302, 303, 307, 308, 400, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			redirected := make(chan struct{}, 1)
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				redirected <- struct{}{}
				w.WriteHeader(http.StatusOK)
			}))
			defer target.Close()
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", target.URL)
				w.WriteHeader(status)
			}))
			defer peer.Close()
			for _, client := range []*http.Client{nil, {Timeout: time.Second}} {
				transport, err := NewRaftHTTPTransport(RaftHTTPTransportOptions{PeerURLs: map[uint64]string{2: peer.URL}, HTTPClient: client})
				if err != nil {
					t.Fatal(err)
				}
				if err := transport.Send(context.Background(), []lsm.CommitLogPeerMessage{{From: 1, To: 2}}); err == nil {
					t.Fatal("unsuccessful response acknowledged")
				}
				if client != nil && client.CheckRedirect != nil {
					t.Fatal("mutated caller client")
				}
			}
			select {
			case <-redirected:
				t.Fatal("followed redirect")
			default:
			}
		})
	}
}

func TestRaftHTTPTransportIsolatesSlowFailingDestination(t *testing.T) {
	healthyReceived := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-healthyReceived:
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer slow.Close()
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(healthyReceived)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer healthy.Close()
	transport, err := NewRaftHTTPTransport(RaftHTTPTransportOptions{PeerURLs: map[uint64]string{2: slow.URL, 3: healthy.URL}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = transport.Send(ctx, []lsm.CommitLogPeerMessage{{From: 1, To: 2}, {From: 1, To: 3}})
	if err == nil {
		t.Fatal("failed peer was not reported")
	}
	select {
	case <-healthyReceived:
	default:
		t.Fatal("healthy peer did not receive message")
	}
	if ctx.Err() != nil {
		t.Fatal("slow peer blocked healthy delivery until deadline")
	}
}

func TestRaftHTTPTransportPostsMessagesToConfiguredPeer(t *testing.T) {
	peerID := lsm.RaftPeerID("node-b")
	var got raftPeerMessagesRequest
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != RaftPeerMessagesPath {
			t.Fatalf("expected path %s, got %s", RaftPeerMessagesPath, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		writeJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
	}))
	defer peer.Close()
	transport, err := NewRaftHTTPTransport(RaftHTTPTransportOptions{
		PeerURLs: map[uint64]string{peerID: peer.URL},
	})
	if err != nil {
		t.Fatalf("new raft http transport: %v", err)
	}

	err = transport.Send(context.Background(), []lsm.CommitLogPeerMessage{
		{From: lsm.RaftPeerID("node-a"), To: peerID, Payload: []byte{1, 2, 3}},
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if len(got.Messages) != 1 {
		t.Fatalf("expected one message, got %d", len(got.Messages))
	}
	if got.Messages[0].To != peerID {
		t.Fatalf("expected target %d, got %d", peerID, got.Messages[0].To)
	}
	if string(got.Messages[0].Payload) != string([]byte{1, 2, 3}) {
		t.Fatalf("unexpected payload: %v", got.Messages[0].Payload)
	}
}

func TestRaftHTTPTransportRejectsUnknownPeer(t *testing.T) {
	transport, err := NewRaftHTTPTransport(RaftHTTPTransportOptions{
		PeerURLs: map[uint64]string{lsm.RaftPeerID("node-b"): "http://127.0.0.1:1"},
	})
	if err != nil {
		t.Fatalf("new raft http transport: %v", err)
	}
	err = transport.Send(context.Background(), []lsm.CommitLogPeerMessage{
		{From: 1, To: lsm.RaftPeerID("node-c")},
	})
	if err == nil {
		t.Fatalf("expected missing peer url error")
	}
}
