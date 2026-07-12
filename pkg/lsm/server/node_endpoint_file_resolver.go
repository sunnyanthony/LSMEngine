package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

// NodeEndpointFileResolverOptions configures a file-backed node endpoint
// resolver.
type NodeEndpointFileResolverOptions struct {
	Path                  string
	FallbackNodeEndpoints map[string]string
}

// NodeEndpointFileResolver resolves node HTTP endpoints from a YAML/JSON
// node-to-URL file. The file is reloaded on each resolve so a long-running
// gateway or supervisor can observe operator-managed endpoint updates.
type NodeEndpointFileResolver struct {
	path     string
	fallback map[string]string

	mu       sync.Mutex
	lastGood map[string]string
}

// NewNodeEndpointFileResolver returns a resolver backed by an absolute endpoint
// file. Fallback endpoints are copied and used when the latest valid file does
// not contain a node.
func NewNodeEndpointFileResolver(opts NodeEndpointFileResolverOptions) (*NodeEndpointFileResolver, error) {
	path := strings.TrimSpace(opts.Path)
	if path == "" {
		return nil, fmt.Errorf("node endpoint file required")
	}
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("node endpoint file must be an absolute path")
	}
	fallback := make(map[string]string)
	mergeNodeEndpoints(fallback, opts.FallbackNodeEndpoints, false)
	return &NodeEndpointFileResolver{
		path:     path,
		fallback: fallback,
	}, nil
}

// ResolveNodeEndpoints returns the latest valid node endpoint map. If reloading
// fails after a successful load, the resolver returns the last good map plus
// fallback endpoints.
func (r *NodeEndpointFileResolver) ResolveNodeEndpoints(ctx context.Context) (map[string]string, error) {
	if r == nil {
		return nil, fmt.Errorf("node endpoint resolver is unavailable")
	}
	endpoints, loadErr := r.load(ctx)
	mergeNodeEndpoints(endpoints, r.fallback, true)
	if len(endpoints) > 0 {
		return endpoints, nil
	}
	if loadErr != nil {
		return nil, loadErr
	}
	return nil, fmt.Errorf("node endpoints required")
}

func (r *NodeEndpointFileResolver) load(ctx context.Context) (map[string]string, error) {
	if err := ctx.Err(); err != nil {
		return r.cached(), err
	}
	data, err := os.ReadFile(r.path)
	if err != nil {
		return r.cached(), fmt.Errorf("read node endpoint file: %w", err)
	}
	loaded, err := parseNodeEndpointFile(data)
	if err != nil {
		return r.cached(), err
	}
	r.mu.Lock()
	r.lastGood = loaded
	r.mu.Unlock()
	return cloneNodeEndpointMap(loaded), nil
}

func (r *NodeEndpointFileResolver) cached() map[string]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneNodeEndpointMap(r.lastGood)
}

func parseNodeEndpointFile(data []byte) (map[string]string, error) {
	var raw map[string]string
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse node endpoint file: %w", err)
	}
	endpoints := make(map[string]string, len(raw))
	mergeNodeEndpoints(endpoints, raw, false)
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("node endpoint file contains no endpoints")
	}
	return endpoints, nil
}
