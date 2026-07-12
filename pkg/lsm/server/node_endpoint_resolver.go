package server

import (
	"context"
	"fmt"
	"strings"

	serverconfig "lsmengine/pkg/lsm/server/config"
)

// NodeEndpointResolver resolves node ids to HTTP base URLs for client and
// operator cluster requests.
type NodeEndpointResolver interface {
	ResolveNodeEndpoints(ctx context.Context) (map[string]string, error)
}

// StaticNodeEndpointResolver resolves node endpoints from a fixed map.
type StaticNodeEndpointResolver struct {
	endpoints map[string]string
}

// NewStaticNodeEndpointResolver returns a resolver backed by a copied endpoint
// map.
func NewStaticNodeEndpointResolver(endpoints map[string]string) (*StaticNodeEndpointResolver, error) {
	resolved := make(map[string]string)
	mergeNodeEndpoints(resolved, endpoints, false)
	if len(resolved) == 0 {
		return nil, fmt.Errorf("node endpoints required")
	}
	return &StaticNodeEndpointResolver{endpoints: resolved}, nil
}

// ResolveNodeEndpoints returns the configured endpoint map.
func (r *StaticNodeEndpointResolver) ResolveNodeEndpoints(ctx context.Context) (map[string]string, error) {
	if r == nil {
		return nil, fmt.Errorf("node endpoint resolver is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return cloneNodeEndpointMap(r.endpoints), nil
}

// NodeEndpointConfigResolverOptions configures endpoint resolution from server
// config sources plus command-level overrides.
type NodeEndpointConfigResolverOptions struct {
	PeerURLFile  string
	PeerURLs     map[string]string
	JoinPeerURLs map[string]string
	Addr         string
	AddrNodeID   string
	Overrides    map[string]string

	LoadPeerURLFile func(path string) (map[string]string, error)
}

// NodeEndpointConfigResolver merges operator endpoint sources in stable
// precedence order: peer_url_file, peer_urls, join_peer_urls, --addr, then
// explicit node-endpoint overrides.
type NodeEndpointConfigResolver struct {
	peerURLFile  string
	peerURLs     map[string]string
	joinPeerURLs map[string]string
	addr         string
	addrNodeID   string
	overrides    map[string]string
	loadFile     func(path string) (map[string]string, error)
}

// NewNodeEndpointConfigResolver returns a resolver backed by config and
// command-level endpoint sources.
func NewNodeEndpointConfigResolver(opts NodeEndpointConfigResolverOptions) *NodeEndpointConfigResolver {
	loadFile := opts.LoadPeerURLFile
	if loadFile == nil {
		loadFile = serverconfig.LoadPeerURLFile
	}
	return &NodeEndpointConfigResolver{
		peerURLFile:  strings.TrimSpace(opts.PeerURLFile),
		peerURLs:     cloneNodeEndpointMap(opts.PeerURLs),
		joinPeerURLs: cloneNodeEndpointMap(opts.JoinPeerURLs),
		addr:         strings.TrimSpace(opts.Addr),
		addrNodeID:   strings.TrimSpace(opts.AddrNodeID),
		overrides:    cloneNodeEndpointMap(opts.Overrides),
		loadFile:     loadFile,
	}
}

// NewNodeEndpointConfigResolverFromConfig returns a resolver backed by server
// config and command-level overrides.
func NewNodeEndpointConfigResolverFromConfig(
	cfg serverconfig.Config,
	addr string,
	overrides map[string]string,
) *NodeEndpointConfigResolver {
	return NewNodeEndpointConfigResolver(NodeEndpointConfigResolverOptions{
		PeerURLFile:  cfg.Raft.PeerURLFile,
		PeerURLs:     cfg.Raft.PeerURLs,
		JoinPeerURLs: cfg.Raft.JoinPeerURLs,
		Addr:         addr,
		AddrNodeID:   cfg.NodeID,
		Overrides:    overrides,
	})
}

// ResolveNodeEndpoints loads and merges configured endpoint sources.
func (r *NodeEndpointConfigResolver) ResolveNodeEndpoints(ctx context.Context) (map[string]string, error) {
	if r == nil {
		return nil, fmt.Errorf("node endpoint resolver is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	endpoints := make(map[string]string)
	if r.peerURLFile != "" {
		loaded, err := r.loadFile(r.peerURLFile)
		if err != nil {
			return nil, err
		}
		mergeNodeEndpoints(endpoints, loaded, false)
	}
	mergeNodeEndpoints(endpoints, r.peerURLs, true)
	mergeNodeEndpoints(endpoints, r.joinPeerURLs, true)
	if r.addr != "" {
		nodeID := r.addrNodeID
		if nodeID == "" {
			nodeID = "addr"
		}
		endpoints[nodeID] = NormalizeHTTPBaseURL(r.addr)
	}
	mergeNodeEndpoints(endpoints, r.overrides, false)
	return endpoints, nil
}

// NormalizeHTTPBaseURL normalizes a host or absolute URL to an HTTP base URL.
func NormalizeHTTPBaseURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		return strings.TrimRight(raw, "/")
	}
	return "http://" + strings.TrimRight(raw, "/")
}

func mergeNodeEndpoints(dst map[string]string, src map[string]string, keepExisting bool) {
	for nodeID, endpoint := range src {
		nodeID = strings.TrimSpace(nodeID)
		endpoint = strings.TrimSpace(endpoint)
		if nodeID == "" || endpoint == "" {
			continue
		}
		if keepExisting {
			if _, exists := dst[nodeID]; exists {
				continue
			}
		}
		dst[nodeID] = NormalizeHTTPBaseURL(endpoint)
	}
}

func cloneNodeEndpointMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for nodeID, endpoint := range in {
		out[nodeID] = endpoint
	}
	return out
}
