package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type keyReadResult struct {
	KeyBase64   string `json:"key_base64"`
	ValueBase64 string `json:"value_base64"`
	Found       *bool  `json:"found"`
	Sequence    uint64 `json:"sequence"`
	Consistency string `json:"consistency"`
}

func runGet(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	fs.SetOutput(stderr)
	endpoint := fs.String("url", "", "specific node HTTP URL (local_applied read; may be stale)")
	keyText := fs.String("key", "", "text key, exclusive with --key-base64")
	keyEncoded := fs.String("key-base64", "", "base64-encoded key")
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
	present := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { present[f.Name] = true })
	key, err := clusterWriteBytes("key", *keyText, *keyEncoded, present)
	if err != nil {
		return err
	}
	base, err := clusterEndpoint(*endpoint)
	if err != nil {
		return err
	}
	encoded := base64.StdEncoding.EncodeToString(key)
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/kv/get?"+url.Values{"key_base64": {encoded}}.Encode(), nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: *timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("node read: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusNotFound {
		return fmt.Errorf("node read returned HTTP %d", res.StatusCode)
	}
	var result keyReadResult
	decoder := json.NewDecoder(res.Body)
	if err := decoder.Decode(&result); err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("expected a single read response")
	}
	if result.Found == nil || result.KeyBase64 != encoded || result.Consistency != "local_applied" || *result.Found != (res.StatusCode == http.StatusOK) {
		return fmt.Errorf("inconsistent node read response")
	}
	if _, err := base64.StdEncoding.Strict().DecodeString(result.ValueBase64); err != nil {
		return fmt.Errorf("invalid response value: %w", err)
	}
	if !*result.Found && (result.ValueBase64 != "" || result.Sequence != 0) {
		return fmt.Errorf("missing key response contains data")
	}
	return json.NewEncoder(stdout).Encode(result)
}
