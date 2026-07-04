package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"lsmengine/pkg/lsm"
)

// GatewayOptions configures route-aware write forwarding.
type GatewayOptions struct {
	BootstrapURL  string
	NodeEndpoints map[string]string
	HTTPClient    *http.Client
}

// Gateway routes writes by shard metadata and retries on stale-route errors.
type Gateway struct {
	bootstrapURL  string
	nodeEndpoints map[string]string
	client        *http.Client

	mu     sync.RWMutex
	routes cachedRoutes
}

type cachedRoutes struct {
	revision uint64
	shards   []cachedRouteShard
}

type cachedRouteShard struct {
	id     string
	start  []byte
	end    []byte
	leader string
}

// WriteRequestError describes a failed write response from a node endpoint.
type WriteRequestError struct {
	Status   int
	Response writeErrorResponse
}

func (e *WriteRequestError) Error() string {
	if e == nil {
		return ""
	}
	if e.Response.Error != "" {
		return e.Response.Error
	}
	return fmt.Sprintf("write request failed with status %d", e.Status)
}

// NewGateway builds a write gateway using route metadata from bootstrapURL.
func NewGateway(opts GatewayOptions) (*Gateway, error) {
	bootstrapURL := strings.TrimSuffix(strings.TrimSpace(opts.BootstrapURL), "/")
	if bootstrapURL == "" {
		return nil, fmt.Errorf("bootstrap url required")
	}
	if len(opts.NodeEndpoints) == 0 {
		return nil, fmt.Errorf("node endpoints required")
	}
	endpoints := make(map[string]string, len(opts.NodeEndpoints))
	for nodeID, endpoint := range opts.NodeEndpoints {
		trimmedNode := strings.TrimSpace(nodeID)
		trimmedEndpoint := strings.TrimSuffix(strings.TrimSpace(endpoint), "/")
		if trimmedNode == "" || trimmedEndpoint == "" {
			return nil, fmt.Errorf("invalid node endpoint mapping")
		}
		endpoints[trimmedNode] = trimmedEndpoint
	}
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}
	return &Gateway{
		bootstrapURL:  bootstrapURL,
		nodeEndpoints: endpoints,
		client:        client,
	}, nil
}

// Put routes a key write to the current shard leader and retries once on stale routes.
func (g *Gateway) Put(
	ctx context.Context,
	key []byte,
	value []byte,
	consistency lsm.WriteConsistency,
) (lsm.WriteRequestStatus, error) {
	return g.writeWithRetry(ctx, "put", key, value, consistency)
}

// Delete routes a delete to the current shard leader and retries once on stale routes.
func (g *Gateway) Delete(
	ctx context.Context,
	key []byte,
	consistency lsm.WriteConsistency,
) (lsm.WriteRequestStatus, error) {
	return g.writeWithRetry(ctx, "delete", key, nil, consistency)
}

func (g *Gateway) writeWithRetry(
	ctx context.Context,
	operation string,
	key []byte,
	value []byte,
	consistency lsm.WriteConsistency,
) (lsm.WriteRequestStatus, error) {
	status, err := g.writeOnce(ctx, operation, key, value, consistency)
	if err == nil {
		return status, nil
	}
	var reqErr *WriteRequestError
	if !errors.As(err, &reqErr) || !reqErr.Response.Retryable {
		return lsm.WriteRequestStatus{}, err
	}
	if refreshErr := g.refreshRoutes(ctx); refreshErr != nil {
		return lsm.WriteRequestStatus{}, err
	}
	return g.writeOnce(ctx, operation, key, value, consistency)
}

func (g *Gateway) writeOnce(
	ctx context.Context,
	operation string,
	key []byte,
	value []byte,
	consistency lsm.WriteConsistency,
) (lsm.WriteRequestStatus, error) {
	leader, err := g.leaderForKey(ctx, key)
	if err != nil {
		return lsm.WriteRequestStatus{}, err
	}
	endpoint, ok := g.nodeEndpoints[leader]
	if !ok {
		return lsm.WriteRequestStatus{}, fmt.Errorf("node endpoint missing for leader %q", leader)
	}
	path := "/kv/put"
	var payload any
	switch operation {
	case "put":
		payload = putRequest{
			KeyBase64:   base64.StdEncoding.EncodeToString(key),
			ValueBase64: base64.StdEncoding.EncodeToString(value),
			Consistency: consistency,
		}
	case "delete":
		path = "/kv/delete"
		payload = deleteRequest{
			KeyBase64:   base64.StdEncoding.EncodeToString(key),
			Consistency: consistency,
		}
	default:
		return lsm.WriteRequestStatus{}, fmt.Errorf("unsupported operation %q", operation)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return lsm.WriteRequestStatus{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+path, bytes.NewReader(body))
	if err != nil {
		return lsm.WriteRequestStatus{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := g.client.Do(req)
	if err != nil {
		return lsm.WriteRequestStatus{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
		var out lsm.WriteRequestStatus
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return lsm.WriteRequestStatus{}, err
		}
		return out, nil
	}
	var writeErr writeErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&writeErr); err != nil {
		writeErr = writeErrorResponse{
			Error: fmt.Sprintf("write request failed with status %d", resp.StatusCode),
			Code:  "unknown",
		}
	}
	return lsm.WriteRequestStatus{}, &WriteRequestError{
		Status:   resp.StatusCode,
		Response: writeErr,
	}
}

func (g *Gateway) leaderForKey(ctx context.Context, key []byte) (string, error) {
	if leader, ok := g.lookupLeader(key); ok {
		return leader, nil
	}
	if err := g.refreshRoutes(ctx); err != nil {
		return "", err
	}
	if leader, ok := g.lookupLeader(key); ok {
		return leader, nil
	}
	return "", fmt.Errorf("route not found for key")
}

func (g *Gateway) lookupLeader(key []byte) (string, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	for _, shard := range g.routes.shards {
		if len(shard.start) > 0 && bytes.Compare(key, shard.start) < 0 {
			continue
		}
		if len(shard.end) > 0 && bytes.Compare(key, shard.end) >= 0 {
			continue
		}
		if shard.leader == "" {
			return "", false
		}
		return shard.leader, true
	}
	return "", false
}

func (g *Gateway) refreshRoutes(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.bootstrapURL+"/cluster/routes", nil)
	if err != nil {
		return err
	}
	resp, err := g.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("route refresh status %d", resp.StatusCode)
	}
	var table routingResponse
	if err := json.NewDecoder(resp.Body).Decode(&table); err != nil {
		return err
	}
	shards := make([]cachedRouteShard, 0, len(table.Shards))
	for _, shard := range table.Shards {
		start, err := base64.StdEncoding.DecodeString(shard.StartKeyBase64)
		if err != nil {
			return fmt.Errorf("decode start key for shard %q: %w", shard.ID, err)
		}
		end, err := base64.StdEncoding.DecodeString(shard.EndKeyBase64)
		if err != nil {
			return fmt.Errorf("decode end key for shard %q: %w", shard.ID, err)
		}
		shards = append(shards, cachedRouteShard{
			id:     shard.ID,
			start:  start,
			end:    end,
			leader: shard.Leader,
		})
	}
	g.mu.Lock()
	g.routes = cachedRoutes{
		revision: table.Revision,
		shards:   shards,
	}
	g.mu.Unlock()
	return nil
}
