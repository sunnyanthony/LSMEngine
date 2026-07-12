package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"lsmengine/pkg/lsm"
)

// GatewayOptions configures route-aware write forwarding.
type GatewayOptions struct {
	BootstrapURL         string
	NodeEndpoints        map[string]string
	NodeEndpointResolver NodeEndpointResolver
	HTTPClient           *http.Client
	MaxWriteAttempts     int
	WriteRetryBackoff    time.Duration
	AlignWriteLeader     bool
}

// Gateway routes writes by shard metadata and performs bounded retries on retryable write errors.
type Gateway struct {
	bootstrapURL     string
	endpointResolver NodeEndpointResolver
	client           *http.Client
	maxAttempts      int
	retryBackoff     time.Duration
	alignWriteLeader bool

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
	resolver := opts.NodeEndpointResolver
	if resolver == nil {
		staticResolver, err := NewStaticNodeEndpointResolver(opts.NodeEndpoints)
		if err != nil {
			if len(opts.NodeEndpoints) > 0 {
				return nil, fmt.Errorf("invalid node endpoint mapping")
			}
			return nil, err
		}
		resolver = staticResolver
	}
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}
	maxAttempts := opts.MaxWriteAttempts
	if maxAttempts < 0 {
		return nil, fmt.Errorf("max write attempts must be non-negative")
	}
	if maxAttempts == 0 {
		maxAttempts = 2
	}
	return &Gateway{
		bootstrapURL:     bootstrapURL,
		endpointResolver: resolver,
		client:           client,
		maxAttempts:      maxAttempts,
		retryBackoff:     opts.WriteRetryBackoff,
		alignWriteLeader: opts.AlignWriteLeader,
	}, nil
}

// Put routes a key write to the current shard leader.
func (g *Gateway) Put(
	ctx context.Context,
	key []byte,
	value []byte,
	consistency lsm.WriteConsistency,
) (lsm.WriteRequestStatus, error) {
	return g.writeWithRetry(ctx, "put", key, value, consistency)
}

// Delete routes a delete to the current shard leader.
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
	for attempt := 1; attempt <= g.maxAttempts; attempt++ {
		status, err := g.writeOnce(ctx, operation, key, value, consistency)
		if err == nil {
			return status, nil
		}
		var reqErr *WriteRequestError
		if !errors.As(err, &reqErr) || !reqErr.Response.Retryable || attempt == g.maxAttempts {
			return lsm.WriteRequestStatus{}, err
		}
		if retryErr := g.prepareWriteRetry(ctx, key, reqErr.Response.Route); retryErr != nil {
			return lsm.WriteRequestStatus{}, err
		}
		if g.retryBackoff > 0 {
			timer := time.NewTimer(g.retryBackoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return lsm.WriteRequestStatus{}, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return lsm.WriteRequestStatus{}, fmt.Errorf("write retry exhausted")
}

func (g *Gateway) prepareWriteRetry(ctx context.Context, key []byte, hint *writeRouteHint) error {
	if g.applyRouteHint(key, hint) {
		return nil
	}
	return g.refreshRoutes(ctx)
}

func (g *Gateway) applyRouteHint(key []byte, hint *writeRouteHint) bool {
	if hint == nil || hint.Leader == "" {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if hint.Revision != 0 && hint.Revision < g.routes.revision {
		return false
	}
	for i := range g.routes.shards {
		shard := &g.routes.shards[i]
		if hint.ShardID != "" {
			if shard.id != hint.ShardID {
				continue
			}
		} else if !routeContainsKey(*shard, key) {
			continue
		}
		shard.leader = hint.Leader
		if hint.Revision > g.routes.revision {
			g.routes.revision = hint.Revision
		}
		return true
	}
	return false
}

func (g *Gateway) writeOnce(
	ctx context.Context,
	operation string,
	key []byte,
	value []byte,
	consistency lsm.WriteConsistency,
) (lsm.WriteRequestStatus, error) {
	if g.alignWriteLeader {
		endpoints, err := g.endpointResolver.ResolveNodeEndpoints(ctx)
		if err != nil {
			return lsm.WriteRequestStatus{}, err
		}
		nodeID, endpoint, err := g.currentWriteLeader(ctx, endpoints)
		if err != nil {
			return lsm.WriteRequestStatus{}, err
		}
		if err := g.alignShardLeader(ctx, endpoint, key, nodeID); err != nil {
			return lsm.WriteRequestStatus{}, err
		}
		return g.postWrite(ctx, endpoint, operation, key, value, consistency)
	}
	leader, err := g.leaderForKey(ctx, key)
	if err != nil {
		return lsm.WriteRequestStatus{}, err
	}
	endpoints, err := g.endpointResolver.ResolveNodeEndpoints(ctx)
	if err != nil {
		return lsm.WriteRequestStatus{}, err
	}
	endpoint, ok := endpoints[leader]
	if !ok {
		return lsm.WriteRequestStatus{}, fmt.Errorf("node endpoint missing for leader %q", leader)
	}
	return g.postWrite(ctx, endpoint, operation, key, value, consistency)
}

func (g *Gateway) postWrite(
	ctx context.Context,
	endpoint string,
	operation string,
	key []byte,
	value []byte,
	consistency lsm.WriteConsistency,
) (lsm.WriteRequestStatus, error) {
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

func (g *Gateway) currentWriteLeader(ctx context.Context, endpoints map[string]string) (string, string, error) {
	nodeIDs := sortedNodeEndpointIDs(endpoints)
	var lastErr error
	for _, nodeID := range nodeIDs {
		endpoint := endpoints[nodeID]
		var status lsm.ClusterStatus
		if err := g.getJSON(ctx, endpoint+"/cluster/status", &status); err != nil {
			lastErr = err
			continue
		}
		if status.CommitLogRuntime.Leader && status.CommitLogRuntime.WriteAvailable {
			if strings.TrimSpace(status.NodeID) != "" {
				nodeID = status.NodeID
			}
			return nodeID, endpoint, nil
		}
	}
	if lastErr != nil {
		return "", "", lastErr
	}
	return "", "", fmt.Errorf("cluster write leader not available")
}

func (g *Gateway) alignShardLeader(ctx context.Context, endpoint string, key []byte, target string) error {
	shard, ok, err := g.shardForKey(ctx, endpoint, key)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("route not found for key")
	}
	if shard.Leader == target {
		return nil
	}
	return g.postControlAction(ctx, endpoint+"/cluster/shards/"+url.PathEscape(shard.ID)+"/transfer-leader", targetRequest{
		Target: target,
	})
}

func (g *Gateway) shardForKey(ctx context.Context, endpoint string, key []byte) (lsm.ShardStatus, bool, error) {
	var shards []lsm.ShardStatus
	if err := g.getJSON(ctx, endpoint+"/cluster/shards", &shards); err != nil {
		return lsm.ShardStatus{}, false, err
	}
	for _, shard := range shards {
		if len(shard.StartKey) > 0 && bytes.Compare(key, shard.StartKey) < 0 {
			continue
		}
		if len(shard.EndKey) > 0 && bytes.Compare(key, shard.EndKey) >= 0 {
			continue
		}
		return shard, true, nil
	}
	return lsm.ShardStatus{}, false, nil
}

func (g *Gateway) postControlAction(ctx context.Context, rawURL string, payload any) error {
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(payload); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := g.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("control action status %d", resp.StatusCode)
	}
	return nil
}

func (g *Gateway) getJSON(ctx context.Context, rawURL string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	resp, err := g.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("get %s status %d", rawURL, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
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
		if !routeContainsKey(shard, key) {
			continue
		}
		if shard.leader == "" {
			return "", false
		}
		return shard.leader, true
	}
	return "", false
}

func routeContainsKey(shard cachedRouteShard, key []byte) bool {
	if len(shard.start) > 0 && bytes.Compare(key, shard.start) < 0 {
		return false
	}
	if len(shard.end) > 0 && bytes.Compare(key, shard.end) >= 0 {
		return false
	}
	return true
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
