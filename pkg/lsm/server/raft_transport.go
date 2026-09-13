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
	PeerURLs     map[uint64]string
	PeerResolver RaftPeerResolver
	HTTPClient   *http.Client
	OnError      func(error)
}

// RaftHTTPTransport sends LSM-owned raft peer messages to peer server endpoints.
type RaftHTTPTransport struct {
	resolver RaftPeerResolver
	client   *http.Client
	onError  func(error)
}

// NewRaftHTTPTransport returns an outbound HTTP transport for raft peer messages.
func NewRaftHTTPTransport(opts RaftHTTPTransportOptions) (*RaftHTTPTransport, error) {
	resolver := opts.PeerResolver
	if resolver == nil {
		staticResolver, err := NewStaticRaftPeerResolver(opts.PeerURLs)
		if err != nil {
			return nil, err
		}
		resolver = staticResolver
	}
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}
	return &RaftHTTPTransport{
		resolver: resolver,
		client:   client,
		onError:  opts.OnError,
	}, nil
}

// Send groups messages by target raft id and dispatches them to peer endpoints.
func (t *RaftHTTPTransport) Send(ctx context.Context, messages []lsm.CommitLogPeerMessage) error {
	return t.send(ctx, messages, nil)
}

// SendWithResult reports each target's HTTP delivery outcome independently.
func (t *RaftHTTPTransport) SendWithResult(ctx context.Context, messages []lsm.CommitLogPeerMessage, report func(uint64, error)) error {
	if report == nil {
		return fmt.Errorf("raft delivery result callback is required")
	}
	return t.send(ctx, messages, report)
}

func (t *RaftHTTPTransport) send(ctx context.Context, messages []lsm.CommitLogPeerMessage, report func(uint64, error)) error {
	if len(messages) == 0 {
		return nil
	}
	if t == nil || t.client == nil || t.resolver == nil {
		return fmt.Errorf("raft http transport is unavailable")
	}
	grouped := make(map[uint64][]lsm.CommitLogPeerMessage)
	for _, message := range messages {
		if message.To == 0 {
			return fmt.Errorf("raft peer message missing target")
		}
		grouped[message.To] = append(grouped[message.To], message)
	}
	var firstError error
	for peerID, peerMessages := range grouped {
		endpoint, err := t.resolver.ResolveRaftPeer(ctx, peerID)
		if err != nil {
			if report != nil {
				report(peerID, err)
				t.reportError(err)
			} else if firstError == nil {
				firstError = err
			}
			continue
		}
		cloned := cloneCommitLogPeerMessages(peerMessages)
		go func(peerID uint64, endpoint string, messages []lsm.CommitLogPeerMessage) {
			sendCtx, cancel := detachedRaftSendContext(ctx, 3*time.Second)
			defer cancel()
			err := t.sendToPeer(sendCtx, peerID, endpoint, messages)
			if report != nil {
				report(peerID, err)
			}
			if err != nil {
				t.reportError(err)
			}
		}(peerID, endpoint, cloned)
	}
	return firstError
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

func cloneCommitLogPeerMessages(messages []lsm.CommitLogPeerMessage) []lsm.CommitLogPeerMessage {
	if len(messages) == 0 {
		return nil
	}
	out := make([]lsm.CommitLogPeerMessage, len(messages))
	for i := range messages {
		out[i] = messages[i]
		out[i].Payload = append([]byte(nil), messages[i].Payload...)
	}
	return out
}

func (t *RaftHTTPTransport) sendToPeer(
	ctx context.Context,
	peerID uint64,
	endpoint string,
	messages []lsm.CommitLogPeerMessage,
) error {
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(raftPeerMessagesRequest{Messages: messages}); err != nil {
		return fmt.Errorf("marshal raft peer messages: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+RaftPeerMessagesPath, &body)
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
