package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"lsmengine/pkg/lsm"
)

const RaftPeerMessagesPath = "/cluster/raft/messages"

// RaftHTTPTransportOptions configures outbound raft peer delivery over HTTP.
type RaftHTTPTransportOptions struct {
	PeerURLs   map[uint64]string
	HTTPClient *http.Client
	OnError    func(error)
}

// RaftHTTPTransport sends LSM-owned raft peer messages to peer server endpoints.
type RaftHTTPTransport struct {
	peerURLs map[uint64]string
	client   *http.Client
	onError  func(error)
}

// NewRaftHTTPTransport returns an outbound HTTP transport for raft peer messages.
func NewRaftHTTPTransport(opts RaftHTTPTransportOptions) (*RaftHTTPTransport, error) {
	if len(opts.PeerURLs) == 0 {
		return nil, fmt.Errorf("raft peer urls required")
	}
	peerURLs := make(map[uint64]string, len(opts.PeerURLs))
	for id, rawURL := range opts.PeerURLs {
		endpoint := strings.TrimSuffix(strings.TrimSpace(rawURL), "/")
		if id == 0 || endpoint == "" {
			return nil, fmt.Errorf("invalid raft peer url mapping")
		}
		peerURLs[id] = endpoint
	}
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}
	return &RaftHTTPTransport{
		peerURLs: peerURLs,
		client:   client,
		onError:  opts.OnError,
	}, nil
}

// Send groups messages by target raft id and dispatches them to peer endpoints.
func (t *RaftHTTPTransport) Send(ctx context.Context, messages []lsm.RaftPeerMessage) error {
	if len(messages) == 0 {
		return nil
	}
	if t == nil || t.client == nil {
		return fmt.Errorf("raft http transport is unavailable")
	}
	grouped := make(map[uint64][]lsm.RaftPeerMessage)
	for _, message := range messages {
		if message.To == 0 {
			return fmt.Errorf("raft peer message missing target")
		}
		if _, ok := t.peerURLs[message.To]; !ok {
			return fmt.Errorf("raft peer url not configured for node id %d", message.To)
		}
		grouped[message.To] = append(grouped[message.To], message)
	}
	for peerID, peerMessages := range grouped {
		cloned := cloneRaftPeerMessages(peerMessages)
		go func(peerID uint64, messages []lsm.RaftPeerMessage) {
			sendCtx, cancel := detachedRaftSendContext(ctx, 3*time.Second)
			defer cancel()
			if err := t.sendToPeer(sendCtx, peerID, messages); err != nil {
				t.reportError(err)
			}
		}(peerID, cloned)
	}
	return nil
}

func (t *RaftHTTPTransport) reportError(err error) {
	if err == nil || t == nil || t.onError == nil {
		return
	}
	t.onError(err)
}

func detachedRaftSendContext(parent context.Context, fallback time.Duration) (context.Context, context.CancelFunc) {
	if parent != nil {
		if deadline, ok := parent.Deadline(); ok {
			return context.WithDeadline(context.Background(), deadline)
		}
	}
	return context.WithTimeout(context.Background(), fallback)
}

func cloneRaftPeerMessages(messages []lsm.RaftPeerMessage) []lsm.RaftPeerMessage {
	if len(messages) == 0 {
		return nil
	}
	out := make([]lsm.RaftPeerMessage, len(messages))
	for i := range messages {
		out[i] = messages[i]
		out[i].Payload = append([]byte(nil), messages[i].Payload...)
	}
	return out
}

func (t *RaftHTTPTransport) sendToPeer(
	ctx context.Context,
	peerID uint64,
	messages []lsm.RaftPeerMessage,
) error {
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(raftPeerMessagesRequest{Messages: messages}); err != nil {
		return fmt.Errorf("marshal raft peer messages: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.peerURLs[peerID]+RaftPeerMessagesPath, &body)
	if err != nil {
		return fmt.Errorf("build raft peer request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("post raft peer messages: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("post raft peer messages to %d failed: http %d: %s", peerID, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}
