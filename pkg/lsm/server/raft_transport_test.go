package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"lsmengine/pkg/lsm"
)

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

	err = transport.Send(context.Background(), []lsm.RaftPeerMessage{
		{From: lsm.RaftPeerID("node-a"), To: peerID, Term: 4, Type: "MsgApp", Payload: []byte{1, 2, 3}},
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
	err = transport.Send(context.Background(), []lsm.RaftPeerMessage{
		{From: 1, To: lsm.RaftPeerID("node-c"), Type: "MsgApp"},
	})
	if err == nil {
		t.Fatalf("expected missing peer url error")
	}
}
