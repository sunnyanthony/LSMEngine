package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
}

// RaftHTTPTransport sends LSM-owned raft peer messages to peer server endpoints.
type RaftHTTPTransport struct {
	peerURLs map[uint64]string
	client   *http.Client
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
	// Keep caller-owned client settings intact while forbidding POST redirects.
	ownedClient := *client
	ownedClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &RaftHTTPTransport{
		peerURLs: peerURLs,
		client:   &ownedClient,
	}, nil
}

// Send groups messages by target raft id and posts them to the configured peer endpoint.
func (t *RaftHTTPTransport) Send(ctx context.Context, messages []lsm.CommitLogPeerMessage) error {
	if len(messages) == 0 {
		return nil
	}
	if t == nil || t.client == nil {
		return fmt.Errorf("raft http transport is unavailable")
	}
	grouped := make(map[uint64][]lsm.CommitLogPeerMessage)
	for _, message := range messages {
		if message.To == 0 {
			return fmt.Errorf("raft peer message missing target")
		}
		if _, ok := t.peerURLs[message.To]; !ok {
			return fmt.Errorf("raft peer url not configured for node id %d", message.To)
		}
		grouped[message.To] = append(grouped[message.To], message)
	}
	// A stalled destination must not consume another peer's delivery window.
	results := make(chan error, len(grouped))
	for peerID, peerMessages := range grouped {
		go func(id uint64, batch []lsm.CommitLogPeerMessage) {
			results <- t.sendToPeer(ctx, id, batch)
		}(peerID, peerMessages)
	}
	var failures []error
	for range grouped {
		if err := <-results; err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (t *RaftHTTPTransport) sendToPeer(
	ctx context.Context,
	peerID uint64,
	messages []lsm.CommitLogPeerMessage,
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
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("post raft peer messages to %d failed: http %d: %s", peerID, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}
