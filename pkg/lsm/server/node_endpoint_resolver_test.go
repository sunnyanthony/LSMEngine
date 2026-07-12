package server

import (
	"context"
	"fmt"
	"reflect"
	"testing"
)

func TestNodeEndpointConfigResolverMergesSourcesByPrecedence(t *testing.T) {
	resolver := NewNodeEndpointConfigResolver(NodeEndpointConfigResolverOptions{
		PeerURLFile: "peers.yaml",
		PeerURLs: map[string]string{
			"node-a": "http://static-a:8080",
			"node-b": "http://static-b:8080/",
		},
		JoinPeerURLs: map[string]string{
			"node-d": "static-d:8080",
		},
		Addr:       "127.0.0.1:8080/",
		AddrNodeID: "node-a",
		Overrides: map[string]string{
			"node-c": "127.0.0.1:8082",
		},
		LoadPeerURLFile: func(path string) (map[string]string, error) {
			if path != "peers.yaml" {
				return nil, fmt.Errorf("unexpected path %q", path)
			}
			return map[string]string{
				"node-a": "http://file-a:8080",
				"node-c": "http://file-c:8080",
			}, nil
		},
	})

	got, err := resolver.ResolveNodeEndpoints(context.Background())
	if err != nil {
		t.Fatalf("resolve endpoints: %v", err)
	}
	want := map[string]string{
		"node-a": "http://127.0.0.1:8080",
		"node-b": "http://static-b:8080",
		"node-c": "http://127.0.0.1:8082",
		"node-d": "http://static-d:8080",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected endpoints:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestStaticNodeEndpointResolverReturnsCopy(t *testing.T) {
	resolver, err := NewStaticNodeEndpointResolver(map[string]string{
		"node-a": "http://node-a:8080/",
	})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	got, err := resolver.ResolveNodeEndpoints(context.Background())
	if err != nil {
		t.Fatalf("resolve endpoints: %v", err)
	}
	got["node-a"] = "http://mutated"

	again, err := resolver.ResolveNodeEndpoints(context.Background())
	if err != nil {
		t.Fatalf("resolve endpoints again: %v", err)
	}
	if again["node-a"] != "http://node-a:8080" {
		t.Fatalf("resolver returned mutable state: %+v", again)
	}
}

func TestStaticNodeEndpointResolverHonorsContextCancellation(t *testing.T) {
	resolver, err := NewStaticNodeEndpointResolver(map[string]string{
		"node-a": "http://node-a:8080",
	})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.ResolveNodeEndpoints(ctx); err == nil {
		t.Fatalf("expected canceled context error")
	}
}
