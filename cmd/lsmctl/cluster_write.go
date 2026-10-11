package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"lsmengine/pkg/lsm"
	"lsmengine/pkg/lsm/server"
)

func runClusterWrite(ctx context.Context, operation string, args []string, stdout, stderr io.Writer) error {
	if operation != "put" && operation != "delete" {
		return fmt.Errorf("unsupported operation %q", operation)
	}
	fs := flag.NewFlagSet("cluster-"+operation, flag.ContinueOnError)
	fs.SetOutput(stderr)
	bootstrap := fs.String("bootstrap-url", "", "preferred node HTTP URL for route discovery")
	nodes := fs.String("node-endpoints", "", "JSON object mapping node IDs to HTTP URLs")
	keyText := fs.String("key", "", "key as text (exclusive with --key-base64)")
	keyEncoded := fs.String("key-base64", "", "base64-encoded key")
	var valueText, valueEncoded string
	if operation == "put" {
		fs.StringVar(&valueText, "value", "", "value as text (exclusive with --value-base64)")
		fs.StringVar(&valueEncoded, "value-base64", "", "base64-encoded value")
	}
	consistency := fs.String("consistency", string(lsm.WriteConsistencyLocalCommitted), "local_committed or accepted (accepted is not a commit acknowledgment)")
	timeout := fs.Duration("timeout", 10*time.Second, "overall request deadline")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	if *timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	mode, err := parseWriteConsistencyDefault(*consistency)
	if err != nil || *consistency == "" {
		return fmt.Errorf("consistency must be local_committed or accepted")
	}
	present := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { present[f.Name] = true })
	key, err := clusterWriteBytes("key", *keyText, *keyEncoded, present)
	if err != nil {
		return err
	}
	var value []byte
	if operation == "put" {
		value, err = clusterWriteBytes("value", valueText, valueEncoded, present)
		if err != nil {
			return err
		}
	}
	bootstrapURL, err := clusterEndpoint(*bootstrap)
	if err != nil {
		return fmt.Errorf("bootstrap-url: %w", err)
	}
	var endpoints map[string]string
	if err := json.Unmarshal([]byte(*nodes), &endpoints); err != nil {
		return fmt.Errorf("node-endpoints: %w", err)
	}
	if len(endpoints) == 0 {
		return fmt.Errorf("node-endpoints must not be empty")
	}
	cleaned := make(map[string]string, len(endpoints))
	for node, endpoint := range endpoints {
		id := strings.TrimSpace(node)
		if id == "" {
			return fmt.Errorf("node ID must not be empty")
		}
		if _, exists := cleaned[id]; exists {
			return fmt.Errorf("duplicate normalized node ID %q", id)
		}
		cleaned[id], err = clusterEndpoint(endpoint)
		if err != nil {
			return fmt.Errorf("node %q: %w", id, err)
		}
	}
	gateway, err := server.NewGateway(server.GatewayOptions{BootstrapURL: bootstrapURL, NodeEndpoints: cleaned, RequestTimeout: *timeout})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	var status lsm.WriteRequestStatus
	if operation == "put" {
		status, err = gateway.Put(ctx, key, value, mode)
	} else {
		status, err = gateway.Delete(ctx, key, mode)
	}
	if err != nil {
		return fmt.Errorf("cluster %s failed (outcome may be unknown; do not blindly retry): %w", operation, err)
	}
	if status.RequestID == "" || status.Operation != operation || status.Consistency != mode {
		return fmt.Errorf("server returned an inconsistent write acknowledgment")
	}
	if mode == lsm.WriteConsistencyLocalCommitted && status.State != lsm.WriteRequestCommitted {
		return fmt.Errorf("server did not confirm commitment: state=%q", status.State)
	}
	if status.State != lsm.WriteRequestCommitted && status.State != lsm.WriteRequestPending {
		return fmt.Errorf("unexpected write result: state=%q", status.State)
	}
	return json.NewEncoder(stdout).Encode(status)
}

func clusterWriteBytes(name, text, encoded string, present map[string]bool) ([]byte, error) {
	if present[name] == present[name+"-base64"] {
		return nil, fmt.Errorf("provide exactly one of --%s or --%s-base64", name, name)
	}
	data := []byte(text)
	if present[name+"-base64"] {
		var err error
		data, err = base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, fmt.Errorf("%s-base64: %w", name, err)
		}
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("%s must not be empty", name)
	}
	return data, nil
}

func clusterEndpoint(raw string) (string, error) {
	endpoint := strings.TrimSuffix(strings.TrimSpace(raw), "/")
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.ForceQuery || strings.Contains(endpoint, "#") {
		return "", fmt.Errorf("expected an HTTP(S) URL without query or fragment")
	}
	return endpoint, nil
}
