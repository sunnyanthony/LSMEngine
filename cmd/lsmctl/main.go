// Command-line helper for stats, health, and server mode.

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"lsmengine/pkg/lsm"
	"lsmengine/pkg/lsm/server"
	serverconfig "lsmengine/pkg/lsm/server/config"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "serve":
		serveCmd(os.Args[2:])
	case "get":
		getCmd(os.Args[2:])
	case "range":
		rangeCmd(os.Args[2:])
	case "put":
		putCmd(os.Args[2:])
	case "delete":
		deleteCmd(os.Args[2:])
	case "write-status":
		writeStatusCmd(os.Args[2:])
	case "stats":
		statsCmd(os.Args[2:])
	case "health":
		healthCmd(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: lsmctl <serve|get|range|put|delete|write-status|stats|health> [options]")
}

func serveCmd(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	configPath := fs.String("config", "", "config file path")
	dataDir := fs.String("data-dir", "", "data directory")
	addr := fs.String("addr", "", "listen address")
	ioBackend := fs.String("io-backend", "", "io backend (os|async|io_uring)")
	ioBackendStrict := fs.Bool("io-backend-strict", false, "fail if io backend is unavailable")
	ioAsyncMax := fs.Int("io-async-max-in-flight", 0, "async io max in-flight ops")
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}

	cfg := loadConfigOrExit(*configPath)
	if *dataDir == "" {
		*dataDir = cfg.DataDir
	}
	if *dataDir == "" {
		log.Fatal("serve requires --data-dir or config")
	}
	if *addr == "" {
		if cfg.Addr != "" {
			*addr = cfg.Addr
		} else {
			*addr = ":8080"
		}
	}

	if *ioBackend == "" {
		*ioBackend = cfg.IOBackend
	}
	if *ioAsyncMax == 0 {
		*ioAsyncMax = cfg.IOAsyncMaxInFlight
	}
	if !*ioBackendStrict {
		*ioBackendStrict = cfg.IOBackendStrict
	}

	commitLogOpts, err := toCommitLogOptions(cfg.CommitLog, cfg.Raft)
	if err != nil {
		log.Fatalf("commitlog config: %v", err)
	}
	store, err := lsm.New(lsm.Options{
		DataDir:            *dataDir,
		NodeID:             cfg.NodeID,
		ClusterID:          cfg.ClusterID,
		StorageMode:        cfg.StorageMode,
		ControlStatePath:   cfg.ControlStatePath,
		CommitLog:          commitLogOpts,
		Raft:               toRaftOptions(cfg.Raft),
		ShardMap:           toShardMap(cfg.Shards),
		IOBackend:          *ioBackend,
		IOBackendStrict:    *ioBackendStrict,
		IOAsyncMaxInFlight: *ioAsyncMax,
	})
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Printf("close: %v", err)
		}
	}()

	server := &http.Server{
		Addr: *addr,
		Handler: server.NewHandlerWithOptions(store, server.HandlerOptions{
			WriteConsistencyDefault: mustWriteConsistencyDefault(cfg.WriteConsistencyDefault),
		}),
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown: %v", err)
		}
	}()

	log.Printf("listening on %s", *addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %v", err)
	}
}

func statsCmd(args []string) {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	configPath := fs.String("config", "", "config file path")
	dataDir := fs.String("data-dir", "", "data directory")
	addr := fs.String("addr", "", "http address for server mode")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}

	cfg := loadConfigOrExit(*configPath)
	if *addr == "" {
		*addr = cfg.Addr
	}
	if *dataDir == "" {
		*dataDir = cfg.DataDir
	}
	stats, err := readStats(*addr, *dataDir)
	if err != nil {
		log.Fatal(err)
	}
	if *jsonOut {
		writeJSON(os.Stdout, stats)
		return
	}
	fmt.Printf("memtable_bytes=%d\n", stats.MemtableBytes)
	fmt.Printf("memtable_entries=%d\n", stats.MemtableEntries)
	fmt.Printf("immutables=%d\n", stats.ImmutableCount)
	fmt.Printf("immutable_bytes=%d\n", stats.ImmutableBytes)
	fmt.Printf("flush_queue_depth=%d\n", stats.FlushQueueDepth)
	fmt.Printf("pinned=%d\n", stats.PinnedCount)
	fmt.Printf("tables=%d\n", stats.TableCount)
	fmt.Printf("seq=%d\n", stats.Seq)
	fmt.Printf("closing=%v closed=%v\n", stats.Closing, stats.Closed)
	fmt.Printf("flush_blocked=%v compaction_enabled=%v\n", stats.FlushBlocked, stats.CompactionEnabled)
}

func healthCmd(args []string) {
	fs := flag.NewFlagSet("health", flag.ExitOnError)
	configPath := fs.String("config", "", "config file path")
	dataDir := fs.String("data-dir", "", "data directory")
	addr := fs.String("addr", "", "http address for server mode")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}

	cfg := loadConfigOrExit(*configPath)
	if *addr == "" {
		*addr = cfg.Addr
	}
	if *dataDir == "" {
		*dataDir = cfg.DataDir
	}
	health, err := readHealth(*addr, *dataDir)
	if err != nil {
		log.Fatal(err)
	}
	if *jsonOut {
		writeJSON(os.Stdout, health)
		return
	}
	fmt.Printf("ready=%v reason=%s\n", health.Ready, health.Reason)
}

type kvGetResult struct {
	Found       bool   `json:"found"`
	KeyBase64   string `json:"key_base64"`
	ValueBase64 string `json:"value_base64"`
	Tombstone   bool   `json:"tombstone,omitempty"`
	Seq         uint64 `json:"seq"`
}

type kvRangeResult struct {
	Entries   []kvRangeEntry `json:"entries"`
	Limit     int            `json:"limit"`
	Truncated bool           `json:"truncated"`
}

type kvRangeEntry struct {
	KeyBase64   string `json:"key_base64"`
	ValueBase64 string `json:"value_base64"`
	Tombstone   bool   `json:"tombstone"`
	Seq         uint64 `json:"seq"`
}

type kvWriteRequest struct {
	KeyBase64   string               `json:"key_base64"`
	ValueBase64 string               `json:"value_base64,omitempty"`
	Consistency lsm.WriteConsistency `json:"consistency,omitempty"`
}

func getCmd(args []string) {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	configPath := fs.String("config", "", "config file path")
	dataDir := fs.String("data-dir", "", "data directory")
	addr := fs.String("addr", "", "http address for server mode")
	key := fs.String("key", "", "key as UTF-8 text")
	keyBase64 := fs.String("key-base64", "", "key as base64")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}
	cfg := loadConfigOrExit(*configPath)
	if *addr == "" {
		*addr = cfg.Addr
	}
	if *dataDir == "" {
		*dataDir = cfg.DataDir
	}
	keyBytes, err := parseKeyFlag(*key, *keyBase64)
	if err != nil {
		log.Fatal(err)
	}
	result, err := readKV(*addr, *dataDir, keyBytes)
	if err != nil {
		log.Fatal(err)
	}
	if *jsonOut {
		writeJSON(os.Stdout, result)
		return
	}
	if !result.Found {
		fmt.Println("found=false")
		return
	}
	value, err := base64.StdEncoding.DecodeString(result.ValueBase64)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("found=true\n")
	fmt.Printf("value=%s\n", string(value))
	fmt.Printf("seq=%d\n", result.Seq)
}

func rangeCmd(args []string) {
	fs := flag.NewFlagSet("range", flag.ExitOnError)
	configPath := fs.String("config", "", "config file path")
	dataDir := fs.String("data-dir", "", "data directory")
	addr := fs.String("addr", "", "http address for server mode")
	start := fs.String("start", "", "start key as UTF-8 text")
	startBase64 := fs.String("start-base64", "", "start key as base64")
	end := fs.String("end", "", "end key as UTF-8 text")
	endBase64 := fs.String("end-base64", "", "end key as base64")
	limit := fs.Int("limit", 100, "maximum entries to return")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}
	cfg := loadConfigOrExit(*configPath)
	if *addr == "" {
		*addr = cfg.Addr
	}
	if *dataDir == "" {
		*dataDir = cfg.DataDir
	}
	startBytes, err := parseOptionalBytesFlag("start", *start, *startBase64)
	if err != nil {
		log.Fatal(err)
	}
	endBytes, err := parseOptionalBytesFlag("end", *end, *endBase64)
	if err != nil {
		log.Fatal(err)
	}
	result, err := readKVRange(*addr, *dataDir, startBytes, endBytes, *limit)
	if err != nil {
		log.Fatal(err)
	}
	if *jsonOut {
		writeJSON(os.Stdout, result)
		return
	}
	writeKVRange(os.Stdout, result)
}

func putCmd(args []string) {
	fs := flag.NewFlagSet("put", flag.ExitOnError)
	configPath := fs.String("config", "", "config file path")
	dataDir := fs.String("data-dir", "", "data directory")
	addr := fs.String("addr", "", "http address for server mode")
	key := fs.String("key", "", "key as UTF-8 text")
	keyBase64 := fs.String("key-base64", "", "key as base64")
	value := fs.String("value", "", "value as UTF-8 text")
	valueBase64 := fs.String("value-base64", "", "value as base64")
	consistency := fs.String("consistency", string(lsm.WriteConsistencyLocalCommitted), "write consistency (accepted|local_committed)")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}
	cfg := loadConfigOrExit(*configPath)
	if *addr == "" {
		*addr = cfg.Addr
	}
	if *dataDir == "" {
		*dataDir = cfg.DataDir
	}
	keyBytes, err := parseKeyFlag(*key, *keyBase64)
	if err != nil {
		log.Fatal(err)
	}
	valueBytes, err := parseValueFlag(*value, *valueBase64)
	if err != nil {
		log.Fatal(err)
	}
	mode, err := parseWriteConsistencyDefault(*consistency)
	if err != nil {
		log.Fatalf("invalid consistency: %v", err)
	}
	status, err := writeKVPut(*addr, *dataDir, keyBytes, valueBytes, mode)
	if err != nil {
		log.Fatal(err)
	}
	writeKVStatus(os.Stdout, status, *jsonOut)
}

func deleteCmd(args []string) {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	configPath := fs.String("config", "", "config file path")
	dataDir := fs.String("data-dir", "", "data directory")
	addr := fs.String("addr", "", "http address for server mode")
	key := fs.String("key", "", "key as UTF-8 text")
	keyBase64 := fs.String("key-base64", "", "key as base64")
	consistency := fs.String("consistency", string(lsm.WriteConsistencyLocalCommitted), "write consistency (accepted|local_committed)")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}
	cfg := loadConfigOrExit(*configPath)
	if *addr == "" {
		*addr = cfg.Addr
	}
	if *dataDir == "" {
		*dataDir = cfg.DataDir
	}
	keyBytes, err := parseKeyFlag(*key, *keyBase64)
	if err != nil {
		log.Fatal(err)
	}
	mode, err := parseWriteConsistencyDefault(*consistency)
	if err != nil {
		log.Fatalf("invalid consistency: %v", err)
	}
	status, err := writeKVDelete(*addr, *dataDir, keyBytes, mode)
	if err != nil {
		log.Fatal(err)
	}
	writeKVStatus(os.Stdout, status, *jsonOut)
}

func writeStatusCmd(args []string) {
	fs := flag.NewFlagSet("write-status", flag.ExitOnError)
	configPath := fs.String("config", "", "config file path")
	addr := fs.String("addr", "", "http address for server mode")
	requestID := fs.String("request-id", "", "write request id")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}
	if *requestID == "" && fs.NArg() > 0 {
		*requestID = fs.Arg(0)
	}
	if *requestID == "" {
		log.Fatal("--request-id required")
	}
	cfg := loadConfigOrExit(*configPath)
	if *addr == "" {
		*addr = cfg.Addr
	}
	status, err := readWriteStatus(*addr, *requestID)
	if err != nil {
		log.Fatal(err)
	}
	writeKVStatus(os.Stdout, status, *jsonOut)
}

func readStats(addr, dataDir string) (lsm.Stats, error) {
	if addr != "" {
		var stats lsm.Stats
		if err := getJSON(addr+"/stats", &stats); err != nil {
			return lsm.Stats{}, err
		}
		return stats, nil
	}
	store, err := openLocal(dataDir)
	if err != nil {
		return lsm.Stats{}, err
	}
	defer store.Close()
	return store.Stats(), nil
}

func readHealth(addr, dataDir string) (lsm.Health, error) {
	if addr != "" {
		var health lsm.Health
		if err := getJSON(addr+"/healthz", &health); err != nil {
			return lsm.Health{}, err
		}
		return health, nil
	}
	store, err := openLocal(dataDir)
	if err != nil {
		return lsm.Health{}, err
	}
	defer store.Close()
	return store.Health(), nil
}

func readKV(addr, dataDir string, key []byte) (kvGetResult, error) {
	if addr != "" {
		var result kvGetResult
		err := getJSON(
			normalizeHTTPBaseURL(addr)+"/kv/get?key_base64="+url.QueryEscape(base64.StdEncoding.EncodeToString(key)),
			&result,
		)
		if err != nil {
			var statusErr *httpStatusError
			if errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusNotFound {
				return kvGetResult{
					Found:     false,
					KeyBase64: base64.StdEncoding.EncodeToString(key),
				}, nil
			}
			return kvGetResult{}, err
		}
		return result, nil
	}
	store, err := openLocal(dataDir)
	if err != nil {
		return kvGetResult{}, err
	}
	defer store.Close()
	entry, ok := store.Get(key)
	if !ok || entry.Tombstone {
		return kvGetResult{
			Found:     false,
			KeyBase64: base64.StdEncoding.EncodeToString(key),
			Tombstone: entry.Tombstone,
			Seq:       entry.Seq,
		}, nil
	}
	return kvGetResult{
		Found:       true,
		KeyBase64:   base64.StdEncoding.EncodeToString(key),
		ValueBase64: base64.StdEncoding.EncodeToString(entry.Value),
		Seq:         entry.Seq,
	}, nil
}

func readKVRange(addr, dataDir string, start []byte, end []byte, limit int) (kvRangeResult, error) {
	if limit <= 0 {
		return kvRangeResult{}, fmt.Errorf("limit must be positive")
	}
	if addr != "" {
		query := url.Values{}
		if start != nil {
			query.Set("start_key_base64", base64.StdEncoding.EncodeToString(start))
		}
		if end != nil {
			query.Set("end_key_base64", base64.StdEncoding.EncodeToString(end))
		}
		query.Set("limit", strconv.Itoa(limit))
		var result kvRangeResult
		err := getJSON(normalizeHTTPBaseURL(addr)+"/kv/range?"+query.Encode(), &result)
		if err != nil {
			return kvRangeResult{}, err
		}
		return result, nil
	}
	store, err := openLocal(dataDir)
	if err != nil {
		return kvRangeResult{}, err
	}
	defer store.Close()
	snap := store.Snapshot()
	if snap == nil {
		return kvRangeResult{}, fmt.Errorf("snapshot unavailable")
	}
	defer snap.Close()
	iter := snap.Range(start, end)
	entries := make([]kvRangeEntry, 0, limit)
	truncated := false
	for iter.Next() {
		entry := iter.Entry()
		if len(entries) >= limit {
			truncated = true
			break
		}
		entries = append(entries, kvRangeEntry{
			KeyBase64:   base64.StdEncoding.EncodeToString(entry.Key),
			ValueBase64: base64.StdEncoding.EncodeToString(entry.Value),
			Tombstone:   entry.Tombstone,
			Seq:         entry.Seq,
		})
	}
	if err := iter.Err(); err != nil {
		return kvRangeResult{}, err
	}
	return kvRangeResult{
		Entries:   entries,
		Limit:     limit,
		Truncated: truncated,
	}, nil
}

func writeKVPut(
	addr string,
	dataDir string,
	key []byte,
	value []byte,
	consistency lsm.WriteConsistency,
) (lsm.WriteRequestStatus, error) {
	if addr != "" {
		req := kvWriteRequest{
			KeyBase64:   base64.StdEncoding.EncodeToString(key),
			ValueBase64: base64.StdEncoding.EncodeToString(value),
			Consistency: consistency,
		}
		return postKVWrite(normalizeHTTPBaseURL(addr)+"/kv/put", req)
	}
	store, err := openLocal(dataDir)
	if err != nil {
		return lsm.WriteRequestStatus{}, err
	}
	defer store.Close()
	if err := store.Put(key, value); err != nil {
		return lsm.WriteRequestStatus{}, err
	}
	return localWriteStatus("put", consistency), nil
}

func writeKVDelete(
	addr string,
	dataDir string,
	key []byte,
	consistency lsm.WriteConsistency,
) (lsm.WriteRequestStatus, error) {
	if addr != "" {
		req := kvWriteRequest{
			KeyBase64:   base64.StdEncoding.EncodeToString(key),
			Consistency: consistency,
		}
		return postKVWrite(normalizeHTTPBaseURL(addr)+"/kv/delete", req)
	}
	store, err := openLocal(dataDir)
	if err != nil {
		return lsm.WriteRequestStatus{}, err
	}
	defer store.Close()
	if err := store.Delete(key); err != nil {
		return lsm.WriteRequestStatus{}, err
	}
	return localWriteStatus("delete", consistency), nil
}

func readWriteStatus(addr string, requestID string) (lsm.WriteRequestStatus, error) {
	if addr == "" {
		return lsm.WriteRequestStatus{}, fmt.Errorf("write-status requires --addr or config addr")
	}
	if requestID == "" {
		return lsm.WriteRequestStatus{}, fmt.Errorf("request id required")
	}
	var status lsm.WriteRequestStatus
	err := getJSON(
		normalizeHTTPBaseURL(addr)+"/kv/write-status/"+url.PathEscape(requestID),
		&status,
	)
	if err != nil {
		return lsm.WriteRequestStatus{}, err
	}
	return status, nil
}

func openLocal(dataDir string) (*lsm.LSM, error) {
	if dataDir == "" {
		return nil, fmt.Errorf("requires --addr or --data-dir")
	}
	return lsm.New(lsm.Options{
		DataDir:               dataDir,
		CompactionL0Threshold: 0,
	})
}

func getJSON(url string, out any) error {
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(normalizeHTTPBaseURL(url))
	if err != nil {
		return err
	}
	return decodeHTTPJSON(resp, out)
}

func postKVWrite(url string, payload kvWriteRequest) (lsm.WriteRequestStatus, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(payload); err != nil {
		return lsm.WriteRequestStatus{}, err
	}
	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(url, "application/json", &buf)
	if err != nil {
		return lsm.WriteRequestStatus{}, err
	}
	var status lsm.WriteRequestStatus
	if err := decodeHTTPJSON(resp, &status); err != nil {
		return lsm.WriteRequestStatus{}, err
	}
	return status, nil
}

func decodeHTTPJSON(resp *http.Response, out any) error {
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return &httpStatusError{StatusCode: resp.StatusCode, Body: string(body)}
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

type httpStatusError struct {
	StatusCode int
	Body       string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("http %d: %s", e.StatusCode, e.Body)
}

func parseKeyFlag(key string, keyBase64 string) ([]byte, error) {
	return parseBytesFlag("key", key, keyBase64)
}

func parseValueFlag(value string, valueBase64 string) ([]byte, error) {
	return parseBytesFlag("value", value, valueBase64)
}

func parseOptionalBytesFlag(name string, text string, encoded string) ([]byte, error) {
	if text != "" && encoded != "" {
		return nil, fmt.Errorf("use --%s or --%s-base64, not both", name, name)
	}
	if encoded != "" {
		out, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, fmt.Errorf("invalid %s-base64: %w", name, err)
		}
		return out, nil
	}
	if text == "" {
		return nil, nil
	}
	return []byte(text), nil
}

func parseBytesFlag(name string, text string, encoded string) ([]byte, error) {
	if text != "" && encoded != "" {
		return nil, fmt.Errorf("use --%s or --%s-base64, not both", name, name)
	}
	if encoded != "" {
		out, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, fmt.Errorf("invalid %s-base64: %w", name, err)
		}
		return out, nil
	}
	if text == "" {
		return nil, fmt.Errorf("--%s required", name)
	}
	return []byte(text), nil
}

func localWriteStatus(operation string, consistency lsm.WriteConsistency) lsm.WriteRequestStatus {
	now := time.Now().UTC()
	return lsm.WriteRequestStatus{
		Operation:   operation,
		Consistency: consistency,
		State:       lsm.WriteRequestCommitted,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

func writeKVStatus(w io.Writer, status lsm.WriteRequestStatus, jsonOut bool) {
	if jsonOut {
		writeJSON(w, status)
		return
	}
	if status.RequestID != "" {
		fmt.Fprintf(w, "request_id=%s\n", status.RequestID)
	}
	fmt.Fprintf(w, "operation=%s\n", status.Operation)
	fmt.Fprintf(w, "state=%s\n", status.State)
	fmt.Fprintf(w, "consistency=%s\n", status.Consistency)
	if status.Error != "" {
		fmt.Fprintf(w, "error=%s\n", status.Error)
	}
}

func writeKVRange(w io.Writer, result kvRangeResult) {
	for _, entry := range result.Entries {
		key, err := base64.StdEncoding.DecodeString(entry.KeyBase64)
		if err != nil {
			fmt.Fprintf(w, "key_base64=%s value_base64=%s seq=%d tombstone=%v\n", entry.KeyBase64, entry.ValueBase64, entry.Seq, entry.Tombstone)
			continue
		}
		value, err := base64.StdEncoding.DecodeString(entry.ValueBase64)
		if err != nil {
			fmt.Fprintf(w, "key=%s value_base64=%s seq=%d tombstone=%v\n", string(key), entry.ValueBase64, entry.Seq, entry.Tombstone)
			continue
		}
		fmt.Fprintf(w, "key=%s value=%s seq=%d tombstone=%v\n", string(key), string(value), entry.Seq, entry.Tombstone)
	}
	if result.Truncated {
		fmt.Fprintf(w, "truncated=true limit=%d\n", result.Limit)
	}
}

func normalizeHTTPBaseURL(raw string) string {
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		return strings.TrimRight(raw, "/")
	}
	return "http://" + strings.TrimRight(raw, "/")
}

func writeJSON(w io.Writer, payload any) {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(payload); err != nil {
		log.Fatal(err)
	}
}

func loadConfigOrExit(path string) serverconfig.Config {
	if path == "" {
		return serverconfig.Config{}
	}
	cfg, err := serverconfig.Load(path)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	return cfg
}

func toRaftOptions(cfg serverconfig.RaftConfig) *lsm.RaftOptions {
	if cfg.Replicas == 0 && cfg.ElectionTimeout == 0 && cfg.HeartbeatInterval == 0 && len(cfg.Peers) == 0 {
		return nil
	}
	peers := append([]string(nil), cfg.Peers...)
	return &lsm.RaftOptions{
		Replicas:          cfg.Replicas,
		ElectionTimeout:   cfg.ElectionTimeout,
		HeartbeatInterval: cfg.HeartbeatInterval,
		Peers:             peers,
	}
}

func toCommitLogOptions(cfg serverconfig.CommitLogConfig, raftCfg serverconfig.RaftConfig) (*lsm.CommitLogOptions, error) {
	if cfg.Provider == "" {
		return nil, nil
	}
	opts := &lsm.CommitLogOptions{
		Provider: lsm.CommitLogProvider(cfg.Provider),
		SnapshotPolicy: lsm.CommitLogSnapshotPolicy{
			AppliedEntries: cfg.SnapshotPolicy.AppliedEntries,
			RetainEntries:  cfg.SnapshotPolicy.RetainEntries,
		},
	}
	if opts.Provider == lsm.CommitLogProviderEtcdRaft && len(raftCfg.PeerURLs) > 0 {
		transport, err := server.NewRaftHTTPTransport(server.RaftHTTPTransportOptions{
			PeerURLs: toRaftPeerURLMap(raftCfg.PeerURLs),
		})
		if err != nil {
			return nil, err
		}
		opts.Transport = transport
	}
	return opts, nil
}

func toRaftPeerURLMap(in map[string]string) map[uint64]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[uint64]string, len(in))
	for nodeID, endpoint := range in {
		out[lsm.RaftPeerID(nodeID)] = endpoint
	}
	return out
}

func toShardMap(in []serverconfig.ShardConfig) []lsm.ShardConfig {
	if len(in) == 0 {
		return nil
	}
	out := make([]lsm.ShardConfig, 0, len(in))
	for _, shard := range in {
		out = append(out, lsm.ShardConfig{
			ID:       shard.ID,
			StartKey: []byte(shard.StartKey),
			EndKey:   []byte(shard.EndKey),
			Replicas: append([]string(nil), shard.Replicas...),
			Leader:   shard.Leader,
		})
	}
	return out
}

func mustWriteConsistencyDefault(raw string) lsm.WriteConsistency {
	mode, err := parseWriteConsistencyDefault(raw)
	if err != nil {
		log.Fatalf("invalid write_consistency_default: %v", err)
	}
	return mode
}

func parseWriteConsistencyDefault(raw string) (lsm.WriteConsistency, error) {
	switch raw {
	case "":
		return lsm.WriteConsistencyAccepted, nil
	case string(lsm.WriteConsistencyAccepted):
		return lsm.WriteConsistencyAccepted, nil
	case string(lsm.WriteConsistencyLocalCommitted):
		return lsm.WriteConsistencyLocalCommitted, nil
	default:
		return "", fmt.Errorf("%q", raw)
	}
}
