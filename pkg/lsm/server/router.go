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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"lsmengine/pkg/lsm"
)

// GatewayReadMode controls how the gateway selects read backends.
type GatewayReadMode string

const (
	GatewayReadModeAny    GatewayReadMode = "any"
	GatewayReadModeLeader GatewayReadMode = "leader"
)

// GatewayReadBalancePolicy controls how healthy read backends are ordered outside leader-only reads.
type GatewayReadBalancePolicy string

const (
	GatewayReadBalanceRoundRobin GatewayReadBalancePolicy = "round_robin"
	GatewayReadBalanceOrdered    GatewayReadBalancePolicy = "ordered"
	GatewayReadBalanceFreshest   GatewayReadBalancePolicy = "freshest"
	GatewayReadBalanceAdaptive   GatewayReadBalancePolicy = "adaptive"
)

// GatewayOptions configures route-aware write forwarding.
type GatewayOptions struct {
	BootstrapURL            string
	NodeEndpoints           map[string]string
	NodeEndpointResolver    NodeEndpointResolver
	HTTPClient              *http.Client
	ReadMode                GatewayReadMode
	ReadBalancePolicy       GatewayReadBalancePolicy
	MaxReadApplyLag         *uint64
	MaxWriteAttempts        int
	WriteRetryBackoff       time.Duration
	AlignWriteLeader        bool
	EndpointFailureCooldown time.Duration
}

// Gateway routes writes by shard metadata and performs bounded retries on retryable write errors.
type Gateway struct {
	bootstrapURL            string
	endpointResolver        NodeEndpointResolver
	client                  *http.Client
	readMode                GatewayReadMode
	readBalancePolicy       GatewayReadBalancePolicy
	maxReadApplyLag         *uint64
	maxAttempts             int
	retryBackoff            time.Duration
	alignWriteLeader        bool
	endpointFailureCooldown time.Duration

	mu       sync.RWMutex
	routes   cachedRoutes
	endpoint endpointPolicy
	routing  gatewayRoutingCounters
}

// GatewayClusterStatus summarizes backend node status as seen through a gateway.
type GatewayClusterStatus struct {
	Ready               bool                       `json:"ready"`
	Reason              string                     `json:"reason,omitempty"`
	NodeCount           int                        `json:"node_count"`
	ReachableNodes      int                        `json:"reachable_nodes"`
	ReadMode            string                     `json:"read_mode"`
	ReadBalancePolicy   string                     `json:"read_balance_policy"`
	MaxReadApplyLag     *uint64                    `json:"max_read_apply_lag,omitempty"`
	WriteLeader         string                     `json:"write_leader,omitempty"`
	WriteLeaderEndpoint string                     `json:"write_leader_endpoint,omitempty"`
	Routing             GatewayRoutingStats        `json:"routing"`
	Nodes               []GatewayClusterNodeStatus `json:"nodes"`
}

// GatewayClusterNodeStatus is one backend node status sample.
type GatewayClusterNodeStatus struct {
	Node          string              `json:"node"`
	Endpoint      string              `json:"endpoint"`
	OK            bool                `json:"ok"`
	Degraded      bool                `json:"degraded"`
	DegradedUntil string              `json:"degraded_until,omitempty"`
	Routing       GatewayBackendStats `json:"routing"`
	Error         string              `json:"error,omitempty"`
	Status        *lsm.ClusterStatus  `json:"status,omitempty"`
}

// GatewayBackendStats describes process-local routing activity for one backend endpoint.
type GatewayBackendStats struct {
	ReadAttempts         uint64 `json:"read_attempts"`
	ReadSuccesses        uint64 `json:"read_successes"`
	ReadFailures         uint64 `json:"read_failures"`
	WriteAttempts        uint64 `json:"write_attempts"`
	WriteSuccesses       uint64 `json:"write_successes"`
	WriteFailures        uint64 `json:"write_failures"`
	StatusProbeAttempts  uint64 `json:"status_probe_attempts"`
	StatusProbeSuccesses uint64 `json:"status_probe_successes"`
	StatusProbeFailures  uint64 `json:"status_probe_failures"`
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

type endpointPolicy struct {
	mu            sync.Mutex
	failedUntil   map[string]time.Time
	stats         map[string]*gatewayBackendCounters
	nextReadStart int
}

type gatewayBackendCounters struct {
	readAttempts         uint64
	readSuccesses        uint64
	readFailures         uint64
	writeAttempts        uint64
	writeSuccesses       uint64
	writeFailures        uint64
	statusProbeAttempts  uint64
	statusProbeSuccesses uint64
	statusProbeFailures  uint64
}

type gatewayRoutingCounters struct {
	writeAttempts        atomic.Uint64
	writeRetries         atomic.Uint64
	writeFailures        atomic.Uint64
	readAttempts         atomic.Uint64
	readFallbacks        atomic.Uint64
	readFailures         atomic.Uint64
	routeRefreshes       atomic.Uint64
	routeRefreshFailures atomic.Uint64
	routeHintUpdates     atomic.Uint64
}

// GatewayRoutingStats describes process-local gateway routing activity.
type GatewayRoutingStats struct {
	WriteAttempts        uint64 `json:"write_attempts"`
	WriteRetries         uint64 `json:"write_retries"`
	WriteFailures        uint64 `json:"write_failures"`
	ReadAttempts         uint64 `json:"read_attempts"`
	ReadFallbacks        uint64 `json:"read_fallbacks"`
	ReadFailures         uint64 `json:"read_failures"`
	RouteRefreshes       uint64 `json:"route_refreshes"`
	RouteRefreshFailures uint64 `json:"route_refresh_failures"`
	RouteHintUpdates     uint64 `json:"route_hint_updates"`
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
	if opts.WriteRetryBackoff < 0 {
		return nil, fmt.Errorf("write retry backoff must be non-negative")
	}
	readMode := opts.ReadMode
	if readMode == "" {
		readMode = GatewayReadModeAny
	}
	switch readMode {
	case GatewayReadModeAny, GatewayReadModeLeader:
	default:
		return nil, fmt.Errorf("invalid gateway read mode %q", readMode)
	}
	readBalancePolicy := opts.ReadBalancePolicy
	if readBalancePolicy == "" {
		readBalancePolicy = GatewayReadBalanceRoundRobin
	}
	switch readBalancePolicy {
	case GatewayReadBalanceRoundRobin, GatewayReadBalanceOrdered, GatewayReadBalanceFreshest, GatewayReadBalanceAdaptive:
	default:
		return nil, fmt.Errorf("invalid gateway read balance policy %q", readBalancePolicy)
	}
	endpointFailureCooldown := opts.EndpointFailureCooldown
	if endpointFailureCooldown < 0 {
		return nil, fmt.Errorf("endpoint failure cooldown must be non-negative")
	}
	if endpointFailureCooldown == 0 {
		endpointFailureCooldown = 5 * time.Second
	}
	return &Gateway{
		bootstrapURL:            bootstrapURL,
		endpointResolver:        resolver,
		client:                  client,
		readMode:                readMode,
		readBalancePolicy:       readBalancePolicy,
		maxReadApplyLag:         cloneUint64Ptr(opts.MaxReadApplyLag),
		maxAttempts:             maxAttempts,
		retryBackoff:            opts.WriteRetryBackoff,
		alignWriteLeader:        opts.AlignWriteLeader,
		endpointFailureCooldown: endpointFailureCooldown,
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

// ClusterStatus polls configured node endpoints and returns the gateway's
// current view of backend readiness.
func (g *Gateway) ClusterStatus(ctx context.Context) (GatewayClusterStatus, error) {
	if g == nil {
		return GatewayClusterStatus{
			Ready:  false,
			Reason: "gateway_unavailable",
		}, fmt.Errorf("gateway unavailable")
	}
	endpoints, err := g.endpointResolver.ResolveNodeEndpoints(ctx)
	if err != nil {
		return GatewayClusterStatus{
			Ready:  false,
			Reason: err.Error(),
		}, err
	}
	nodeIDs := sortedUniqueNodeEndpointIDs(endpoints)
	result := GatewayClusterStatus{
		NodeCount:         len(nodeIDs),
		ReadMode:          string(g.readMode),
		ReadBalancePolicy: string(g.readBalancePolicy),
		MaxReadApplyLag:   cloneUint64Ptr(g.maxReadApplyLag),
		Routing:           g.RoutingStats(),
		Nodes:             make([]GatewayClusterNodeStatus, 0, len(nodeIDs)),
	}
	var lastErr error
	for _, nodeID := range nodeIDs {
		endpoint := endpoints[nodeID]
		node := GatewayClusterNodeStatus{
			Node:     nodeID,
			Endpoint: endpoint,
		}
		var status lsm.ClusterStatus
		if err := g.getJSON(ctx, endpoint+"/cluster/status", &status); err != nil {
			g.recordEndpointStatusProbe(endpoint, false)
			g.markEndpointFailure(endpoint)
			node.Degraded, node.DegradedUntil = g.endpointHealth(endpoint)
			node.Routing = g.endpointRoutingStats(endpoint)
			node.Error = err.Error()
			lastErr = err
		} else {
			g.recordEndpointStatusProbe(endpoint, true)
			g.markEndpointSuccess(endpoint)
			node.Degraded, node.DegradedUntil = g.endpointHealth(endpoint)
			node.Routing = g.endpointRoutingStats(endpoint)
			node.OK = true
			node.Status = &status
			result.ReachableNodes++
			if strings.TrimSpace(status.NodeID) != "" {
				node.Node = status.NodeID
			}
			if status.CommitLogRuntime.Leader && status.CommitLogRuntime.WriteAvailable {
				result.WriteLeader = node.Node
				result.WriteLeaderEndpoint = endpoint
			}
		}
		result.Nodes = append(result.Nodes, node)
	}
	result.Ready = result.ReachableNodes > 0
	if !result.Ready {
		switch {
		case lastErr != nil:
			result.Reason = lastErr.Error()
		case result.NodeCount == 0:
			result.Reason = "no node endpoints available"
		default:
			result.Reason = "no reachable node endpoints"
		}
		return result, errors.New(result.Reason)
	}
	if result.ReachableNodes < result.NodeCount {
		result.Reason = "partial_node_unavailable"
	}
	return result, nil
}

// RoutingStats returns process-local gateway routing counters.
func (g *Gateway) RoutingStats() GatewayRoutingStats {
	if g == nil {
		return GatewayRoutingStats{}
	}
	return GatewayRoutingStats{
		WriteAttempts:        g.routing.writeAttempts.Load(),
		WriteRetries:         g.routing.writeRetries.Load(),
		WriteFailures:        g.routing.writeFailures.Load(),
		ReadAttempts:         g.routing.readAttempts.Load(),
		ReadFallbacks:        g.routing.readFallbacks.Load(),
		ReadFailures:         g.routing.readFailures.Load(),
		RouteRefreshes:       g.routing.routeRefreshes.Load(),
		RouteRefreshFailures: g.routing.routeRefreshFailures.Load(),
		RouteHintUpdates:     g.routing.routeHintUpdates.Load(),
	}
}

func sortedUniqueNodeEndpointIDs(endpoints map[string]string) []string {
	nodeIDs := sortedNodeEndpointIDs(endpoints)
	unique := make([]string, 0, len(nodeIDs))
	seen := make(map[string]struct{}, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		endpoint := NormalizeHTTPBaseURL(endpoints[nodeID])
		if _, ok := seen[endpoint]; ok {
			continue
		}
		seen[endpoint] = struct{}{}
		unique = append(unique, nodeID)
	}
	return unique
}

func (g *Gateway) writeWithRetry(
	ctx context.Context,
	operation string,
	key []byte,
	value []byte,
	consistency lsm.WriteConsistency,
) (lsm.WriteRequestStatus, error) {
	for attempt := 1; attempt <= g.maxAttempts; attempt++ {
		g.routing.writeAttempts.Add(1)
		status, err := g.writeOnce(ctx, operation, key, value, consistency)
		if err == nil {
			return status, nil
		}
		var reqErr *WriteRequestError
		if !errors.As(err, &reqErr) || !reqErr.Response.Retryable || attempt == g.maxAttempts {
			g.routing.writeFailures.Add(1)
			return lsm.WriteRequestStatus{}, err
		}
		if retryErr := g.prepareWriteRetry(ctx, key, reqErr.Response.Route); retryErr != nil {
			g.routing.writeFailures.Add(1)
			return lsm.WriteRequestStatus{}, err
		}
		g.routing.writeRetries.Add(1)
		if g.retryBackoff > 0 {
			timer := time.NewTimer(g.retryBackoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				g.routing.writeFailures.Add(1)
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
		g.routing.routeHintUpdates.Add(1)
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
	writeSucceeded := err == nil && (resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted)
	g.recordEndpointWriteAttempt(endpoint, writeSucceeded)
	if err != nil {
		g.markEndpointFailure(endpoint)
		return lsm.WriteRequestStatus{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
		g.markEndpointSuccess(endpoint)
		var out lsm.WriteRequestStatus
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return lsm.WriteRequestStatus{}, err
		}
		return out, nil
	}
	if resp.StatusCode >= http.StatusInternalServerError {
		g.markEndpointFailure(endpoint)
	} else {
		g.markEndpointSuccess(endpoint)
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
	nodeIDs := g.nodeEndpointIDs(endpoints, false)
	var lastErr error
	for _, nodeID := range nodeIDs {
		endpoint := endpoints[nodeID]
		var status lsm.ClusterStatus
		if err := g.getJSON(ctx, endpoint+"/cluster/status", &status); err != nil {
			g.recordEndpointStatusProbe(endpoint, false)
			g.markEndpointFailure(endpoint)
			lastErr = err
			continue
		}
		g.recordEndpointStatusProbe(endpoint, true)
		g.markEndpointSuccess(endpoint)
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

func (g *Gateway) readNodeEndpointIDs(endpoints map[string]string) []string {
	switch g.readBalancePolicy {
	case GatewayReadBalanceRoundRobin:
		return g.nodeEndpointIDs(endpoints, true)
	case GatewayReadBalanceAdaptive:
		return g.adaptiveReadNodeEndpointIDs(endpoints)
	default:
		return g.nodeEndpointIDs(endpoints, false)
	}
}

type gatewayReadTarget struct {
	nodeID   string
	endpoint string
}

type gatewayAdaptiveReadCandidate struct {
	nodeID              string
	degraded            bool
	totalFailures       uint64
	readFailures        uint64
	statusProbeFailures uint64
	writeFailures       uint64
	readAttempts        uint64
	order               int
}

type gatewayReadStatusTarget struct {
	gatewayReadTarget
	applyLag uint64
	order    int
}

func (g *Gateway) readTargets(ctx context.Context, endpoints map[string]string, kvRead bool) ([]gatewayReadTarget, error) {
	if g == nil {
		return nil, fmt.Errorf("gateway unavailable")
	}
	if kvRead && g.readMode == GatewayReadModeLeader {
		nodeID, endpoint, err := g.currentWriteLeader(ctx, endpoints)
		if err != nil {
			return nil, err
		}
		return []gatewayReadTarget{{nodeID: nodeID, endpoint: endpoint}}, nil
	}
	nodeIDs := g.readNodeEndpointIDs(endpoints)
	if kvRead && (g.maxReadApplyLag != nil || g.readBalancePolicy == GatewayReadBalanceFreshest) {
		return g.statusFilteredReadTargets(ctx, endpoints, nodeIDs)
	}
	out := make([]gatewayReadTarget, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		endpoint, ok := endpoints[nodeID]
		if !ok {
			continue
		}
		out = append(out, gatewayReadTarget{nodeID: nodeID, endpoint: endpoint})
	}
	return out, nil
}

func (g *Gateway) statusFilteredReadTargets(ctx context.Context, endpoints map[string]string, nodeIDs []string) ([]gatewayReadTarget, error) {
	candidates := make([]gatewayReadStatusTarget, 0, len(nodeIDs))
	var lastErr error
	for _, nodeID := range nodeIDs {
		endpoint, ok := endpoints[nodeID]
		if !ok {
			continue
		}
		status, err := g.backendStatus(ctx, endpoint)
		if err != nil {
			lastErr = err
			continue
		}
		applyLag := status.CommitLogRuntime.ApplyLag
		if g.maxReadApplyLag != nil && applyLag > *g.maxReadApplyLag {
			lastErr = fmt.Errorf("node %q apply lag %d exceeds max read apply lag %d", nodeID, applyLag, *g.maxReadApplyLag)
			continue
		}
		if strings.TrimSpace(status.NodeID) != "" {
			nodeID = status.NodeID
		}
		candidates = append(candidates, gatewayReadStatusTarget{
			gatewayReadTarget: gatewayReadTarget{nodeID: nodeID, endpoint: endpoint},
			applyLag:          applyLag,
			order:             len(candidates),
		})
	}
	if len(candidates) == 0 && lastErr != nil {
		return nil, lastErr
	}
	if g.readBalancePolicy == GatewayReadBalanceFreshest {
		sort.SliceStable(candidates, func(i, j int) bool {
			if candidates[i].applyLag != candidates[j].applyLag {
				return candidates[i].applyLag < candidates[j].applyLag
			}
			return candidates[i].order < candidates[j].order
		})
	}
	out := make([]gatewayReadTarget, 0, len(candidates))
	for _, candidate := range candidates {
		out = append(out, candidate.gatewayReadTarget)
	}
	return out, nil
}

func (g *Gateway) backendStatus(ctx context.Context, endpoint string) (lsm.ClusterStatus, error) {
	var status lsm.ClusterStatus
	if err := g.getJSON(ctx, endpoint+"/cluster/status", &status); err != nil {
		g.recordEndpointStatusProbe(endpoint, false)
		g.markEndpointFailure(endpoint)
		return lsm.ClusterStatus{}, err
	}
	g.recordEndpointStatusProbe(endpoint, true)
	g.markEndpointSuccess(endpoint)
	return status, nil
}

func (g *Gateway) nodeEndpointIDs(endpoints map[string]string, rotateHealthy bool) []string {
	nodeIDs := sortedNodeEndpointIDs(endpoints)
	if g == nil || len(nodeIDs) <= 1 {
		return nodeIDs
	}
	now := time.Now()
	healthy := make([]string, 0, len(nodeIDs))
	degraded := make([]string, 0, len(nodeIDs))
	g.endpoint.mu.Lock()
	for _, nodeID := range nodeIDs {
		endpoint := NormalizeHTTPBaseURL(endpoints[nodeID])
		until, failed := g.endpoint.failedUntil[endpoint]
		if failed && now.Before(until) {
			degraded = append(degraded, nodeID)
			continue
		}
		if failed {
			delete(g.endpoint.failedUntil, endpoint)
		}
		healthy = append(healthy, nodeID)
	}
	if rotateHealthy && len(healthy) > 1 {
		start := g.endpoint.nextReadStart % len(healthy)
		g.endpoint.nextReadStart++
		healthy = append(append([]string(nil), healthy[start:]...), healthy[:start]...)
	}
	g.endpoint.mu.Unlock()
	return append(healthy, degraded...)
}

func (g *Gateway) adaptiveReadNodeEndpointIDs(endpoints map[string]string) []string {
	nodeIDs := sortedNodeEndpointIDs(endpoints)
	if g == nil || len(nodeIDs) <= 1 {
		return nodeIDs
	}
	candidates := make([]gatewayAdaptiveReadCandidate, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		endpoint := endpoints[nodeID]
		stats := g.endpointRoutingStats(endpoint)
		degraded, _ := g.endpointHealth(endpoint)
		candidates = append(candidates, gatewayAdaptiveReadCandidate{
			nodeID:              nodeID,
			degraded:            degraded,
			totalFailures:       stats.ReadFailures + stats.StatusProbeFailures + stats.WriteFailures,
			readFailures:        stats.ReadFailures,
			statusProbeFailures: stats.StatusProbeFailures,
			writeFailures:       stats.WriteFailures,
			readAttempts:        stats.ReadAttempts,
			order:               len(candidates),
		})
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].degraded != candidates[j].degraded {
			return !candidates[i].degraded
		}
		if candidates[i].totalFailures != candidates[j].totalFailures {
			return candidates[i].totalFailures < candidates[j].totalFailures
		}
		if candidates[i].readFailures != candidates[j].readFailures {
			return candidates[i].readFailures < candidates[j].readFailures
		}
		if candidates[i].statusProbeFailures != candidates[j].statusProbeFailures {
			return candidates[i].statusProbeFailures < candidates[j].statusProbeFailures
		}
		if candidates[i].writeFailures != candidates[j].writeFailures {
			return candidates[i].writeFailures < candidates[j].writeFailures
		}
		if candidates[i].readAttempts != candidates[j].readAttempts {
			return candidates[i].readAttempts < candidates[j].readAttempts
		}
		return candidates[i].order < candidates[j].order
	})
	out := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		out = append(out, candidate.nodeID)
	}
	return out
}

func (g *Gateway) markEndpointFailure(endpoint string) {
	if g == nil || g.endpointFailureCooldown <= 0 {
		return
	}
	endpoint = NormalizeHTTPBaseURL(endpoint)
	g.endpoint.mu.Lock()
	if g.endpoint.failedUntil == nil {
		g.endpoint.failedUntil = make(map[string]time.Time)
	}
	g.endpoint.failedUntil[endpoint] = time.Now().Add(g.endpointFailureCooldown)
	g.endpoint.mu.Unlock()
}

func (g *Gateway) markEndpointSuccess(endpoint string) {
	if g == nil {
		return
	}
	endpoint = NormalizeHTTPBaseURL(endpoint)
	g.endpoint.mu.Lock()
	delete(g.endpoint.failedUntil, endpoint)
	g.endpoint.mu.Unlock()
}

func (g *Gateway) endpointHealth(endpoint string) (bool, string) {
	if g == nil {
		return false, ""
	}
	endpoint = NormalizeHTTPBaseURL(endpoint)
	now := time.Now()
	g.endpoint.mu.Lock()
	defer g.endpoint.mu.Unlock()
	until, failed := g.endpoint.failedUntil[endpoint]
	if !failed {
		return false, ""
	}
	if !now.Before(until) {
		delete(g.endpoint.failedUntil, endpoint)
		return false, ""
	}
	return true, until.UTC().Format(time.RFC3339Nano)
}

func (g *Gateway) recordEndpointReadAttempt(endpoint string, success bool) {
	if g == nil {
		return
	}
	g.updateEndpointCounters(endpoint, func(stats *gatewayBackendCounters) {
		stats.readAttempts++
		if success {
			stats.readSuccesses++
		} else {
			stats.readFailures++
		}
	})
}

func (g *Gateway) recordEndpointWriteAttempt(endpoint string, success bool) {
	if g == nil {
		return
	}
	g.updateEndpointCounters(endpoint, func(stats *gatewayBackendCounters) {
		stats.writeAttempts++
		if success {
			stats.writeSuccesses++
		} else {
			stats.writeFailures++
		}
	})
}

func (g *Gateway) recordEndpointStatusProbe(endpoint string, success bool) {
	if g == nil {
		return
	}
	g.updateEndpointCounters(endpoint, func(stats *gatewayBackendCounters) {
		stats.statusProbeAttempts++
		if success {
			stats.statusProbeSuccesses++
		} else {
			stats.statusProbeFailures++
		}
	})
}

func (g *Gateway) endpointRoutingStats(endpoint string) GatewayBackendStats {
	if g == nil {
		return GatewayBackendStats{}
	}
	endpoint = NormalizeHTTPBaseURL(endpoint)
	g.endpoint.mu.Lock()
	defer g.endpoint.mu.Unlock()
	stats := g.endpoint.stats[endpoint]
	if stats == nil {
		return GatewayBackendStats{}
	}
	return GatewayBackendStats{
		ReadAttempts:         stats.readAttempts,
		ReadSuccesses:        stats.readSuccesses,
		ReadFailures:         stats.readFailures,
		WriteAttempts:        stats.writeAttempts,
		WriteSuccesses:       stats.writeSuccesses,
		WriteFailures:        stats.writeFailures,
		StatusProbeAttempts:  stats.statusProbeAttempts,
		StatusProbeSuccesses: stats.statusProbeSuccesses,
		StatusProbeFailures:  stats.statusProbeFailures,
	}
}

func (g *Gateway) updateEndpointCounters(endpoint string, update func(*gatewayBackendCounters)) {
	endpoint = NormalizeHTTPBaseURL(endpoint)
	g.endpoint.mu.Lock()
	defer g.endpoint.mu.Unlock()
	if g.endpoint.stats == nil {
		g.endpoint.stats = make(map[string]*gatewayBackendCounters)
	}
	stats := g.endpoint.stats[endpoint]
	if stats == nil {
		stats = &gatewayBackendCounters{}
		g.endpoint.stats[endpoint] = stats
	}
	update(stats)
}

func cloneUint64Ptr(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	out := *value
	return &out
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

func (g *Gateway) refreshRoutes(ctx context.Context) (err error) {
	if g != nil {
		g.routing.routeRefreshes.Add(1)
		defer func() {
			if err != nil {
				g.routing.routeRefreshFailures.Add(1)
			}
		}()
	}
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
