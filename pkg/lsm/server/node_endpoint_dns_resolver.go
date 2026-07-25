package server

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
)

// LookupSRVFunc resolves DNS SRV records.
type LookupSRVFunc func(ctx context.Context, service string, proto string, name string) (string, []*net.SRV, error)

// NodeEndpointDNSResolverOptions configures DNS SRV-backed node endpoint
// resolution.
type NodeEndpointDNSResolverOptions struct {
	Service               string
	Proto                 string
	Name                  string
	Scheme                string
	FallbackNodeEndpoints map[string]string
	LookupSRV             LookupSRVFunc
}

// NodeEndpointDNSResolver resolves node HTTP endpoints from DNS SRV records.
// The first DNS target label is used as the LSM node id, matching Kubernetes
// StatefulSet pod DNS names such as:
// lsm-cluster-0.lsm-cluster.lsm-cluster.svc.cluster.local.
type NodeEndpointDNSResolver struct {
	service  string
	proto    string
	name     string
	scheme   string
	fallback map[string]string
	lookup   LookupSRVFunc

	mu       sync.Mutex
	lastGood map[string]string
}

// NewNodeEndpointDNSResolver returns a resolver backed by DNS SRV lookup.
func NewNodeEndpointDNSResolver(opts NodeEndpointDNSResolverOptions) (*NodeEndpointDNSResolver, error) {
	service := normalizeDNSPart(opts.Service, "http")
	proto := normalizeDNSPart(opts.Proto, "tcp")
	name := strings.Trim(strings.TrimSpace(opts.Name), ".")
	if name == "" {
		return nil, fmt.Errorf("node endpoint DNS name required")
	}
	scheme := strings.TrimSpace(opts.Scheme)
	if scheme == "" {
		scheme = "http"
	}
	lookup := opts.LookupSRV
	if lookup == nil {
		lookup = net.DefaultResolver.LookupSRV
	}
	fallback := make(map[string]string)
	mergeNodeEndpoints(fallback, opts.FallbackNodeEndpoints, false)
	return &NodeEndpointDNSResolver{
		service:  service,
		proto:    proto,
		name:     name,
		scheme:   scheme,
		fallback: fallback,
		lookup:   lookup,
	}, nil
}

// ResolveNodeEndpoints returns the latest DNS SRV endpoint map. If a lookup
// fails after a successful lookup, the resolver returns the last good map plus
// fallback endpoints.
func (r *NodeEndpointDNSResolver) ResolveNodeEndpoints(ctx context.Context) (map[string]string, error) {
	if r == nil {
		return nil, fmt.Errorf("node endpoint resolver is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	endpoints, lookupErr := r.load(ctx)
	if endpoints == nil {
		endpoints = make(map[string]string)
	}
	mergeNodeEndpoints(endpoints, r.fallback, true)
	if len(endpoints) > 0 {
		return endpoints, nil
	}
	if lookupErr != nil {
		return nil, lookupErr
	}
	return nil, fmt.Errorf("node endpoints required")
}

func (r *NodeEndpointDNSResolver) load(ctx context.Context) (map[string]string, error) {
	if err := ctx.Err(); err != nil {
		return r.cached(), err
	}
	_, records, err := r.lookup(ctx, r.service, r.proto, r.name)
	if err != nil {
		return r.cached(), fmt.Errorf("lookup node endpoint SRV records: %w", err)
	}
	endpoints, err := nodeEndpointsFromSRVRecords(r.scheme, records)
	if err != nil {
		return r.cached(), err
	}
	r.mu.Lock()
	r.lastGood = endpoints
	r.mu.Unlock()
	return cloneNodeEndpointMap(endpoints), nil
}

func (r *NodeEndpointDNSResolver) cached() map[string]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneNodeEndpointMap(r.lastGood)
}

func nodeEndpointsFromSRVRecords(scheme string, records []*net.SRV) (map[string]string, error) {
	scheme = strings.TrimSpace(scheme)
	if scheme == "" {
		scheme = "http"
	}
	records = append([]*net.SRV(nil), records...)
	sort.Slice(records, func(i, j int) bool {
		if records[i] == nil {
			return false
		}
		if records[j] == nil {
			return true
		}
		left := strings.TrimRight(records[i].Target, ".")
		right := strings.TrimRight(records[j].Target, ".")
		if left == right {
			return records[i].Port < records[j].Port
		}
		return left < right
	})
	endpoints := make(map[string]string, len(records))
	for _, record := range records {
		if record == nil {
			continue
		}
		target := strings.TrimRight(strings.TrimSpace(record.Target), ".")
		nodeID := nodeIDFromDNSTarget(target)
		if nodeID == "" || record.Port == 0 || target == "" {
			continue
		}
		endpoints[nodeID] = NormalizeHTTPBaseURL(fmt.Sprintf("%s://%s:%d", scheme, target, record.Port))
	}
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("node endpoint SRV lookup returned no endpoints")
	}
	return endpoints, nil
}

func nodeIDFromDNSTarget(target string) string {
	target = strings.Trim(strings.TrimSpace(target), ".")
	if target == "" {
		return ""
	}
	nodeID, _, _ := strings.Cut(target, ".")
	return strings.TrimSpace(nodeID)
}

func normalizeDNSPart(value string, fallback string) string {
	value = strings.Trim(strings.TrimSpace(value), "_")
	if value == "" {
		return fallback
	}
	return value
}
