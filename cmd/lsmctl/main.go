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
	"sort"
	"strconv"
	"strings"
	"syscall"
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
	case "gateway":
		gatewayCmd(os.Args[2:])
	case "get":
		getCmd(os.Args[2:])
	case "range":
		rangeCmd(os.Args[2:])
	case "put":
		putCmd(os.Args[2:])
	case "delete":
		deleteCmd(os.Args[2:])
	case "async-put":
		asyncPutCmd(os.Args[2:])
	case "async-delete":
		asyncDeleteCmd(os.Args[2:])
	case "write-status":
		writeStatusCmd(os.Args[2:])
	case "cluster-status":
		clusterStatusCmd(os.Args[2:])
	case "wait-cluster":
		waitClusterCmd(os.Args[2:])
	case "drain-node":
		drainNodeCmd(os.Args[2:])
	case "resume-node":
		resumeNodeCmd(os.Args[2:])
	case "raft-add-node":
		raftAddNodeCmd(os.Args[2:])
	case "raft-remove-node":
		raftRemoveNodeCmd(os.Args[2:])
	case "shard-add-replica":
		shardAddReplicaCmd(os.Args[2:])
	case "shard-remove-replica":
		shardRemoveReplicaCmd(os.Args[2:])
	case "replace-node":
		replaceNodeCmd(os.Args[2:])
	case "replacement-plan":
		replacementPlanCmd(os.Args[2:])
	case "replacement-apply":
		replacementApplyCmd(os.Args[2:])
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
	fmt.Fprintln(os.Stderr, "usage: lsmctl <serve|gateway|get|range|put|delete|async-put|async-delete|write-status|cluster-status|wait-cluster|drain-node|resume-node|raft-add-node|raft-remove-node|shard-add-replica|shard-remove-replica|replace-node|replacement-plan|replacement-apply|stats|health> [options]")
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

	ctx, stop := signal.NotifyContext(context.Background(), serveSignals()...)
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

func serveSignals() []os.Signal {
	return []os.Signal{os.Interrupt, syscall.SIGTERM}
}

func gatewayCmd(args []string) {
	fs := flag.NewFlagSet("gateway", flag.ExitOnError)
	configPath := fs.String("config", "", "config file path")
	listen := fs.String("listen", ":8090", "gateway listen address")
	bootstrapURL := fs.String("bootstrap-url", "", "server URL used to refresh cluster routes")
	endpointFile := fs.String("endpoint-file", "", "absolute YAML/JSON node endpoint file")
	writeConsistencyDefault := fs.String("write-consistency-default", "", "default write consistency (accepted|local_committed)")
	maxWriteAttempts := fs.Int("max-write-attempts", 0, "maximum route-aware write attempts; 0 uses default")
	writeRetryBackoff := fs.Duration("write-retry-backoff", 0, "delay between retryable write attempts")
	var nodeEndpoints nodeEndpointFlags
	fs.Var(&nodeEndpoints, "node-endpoint", "node endpoint mapping node=url; may be repeated")
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}

	cfg := loadConfigOrExit(*configPath)
	if *bootstrapURL == "" {
		*bootstrapURL = cfg.Addr
	}
	if strings.TrimSpace(*bootstrapURL) == "" {
		log.Fatal("gateway requires --bootstrap-url or config addr")
	}
	consistencyDefault := *writeConsistencyDefault
	if consistencyDefault == "" {
		consistencyDefault = cfg.WriteConsistencyDefault
	}
	resolver, err := gatewayNodeEndpointResolverFromConfig(cfg, *bootstrapURL, *endpointFile, nodeEndpoints)
	if err != nil {
		log.Fatalf("gateway endpoints: %v", err)
	}
	gateway, err := server.NewGateway(server.GatewayOptions{
		BootstrapURL:         normalizeHTTPBaseURL(*bootstrapURL),
		NodeEndpointResolver: resolver,
		MaxWriteAttempts:     *maxWriteAttempts,
		WriteRetryBackoff:    *writeRetryBackoff,
		AlignWriteLeader:     true,
	})
	if err != nil {
		log.Fatalf("gateway: %v", err)
	}
	srv := &http.Server{
		Addr: *listen,
		Handler: server.NewGatewayHandler(gateway, server.HandlerOptions{
			WriteConsistencyDefault: mustWriteConsistencyDefault(consistencyDefault),
		}),
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	ctx, stop := signal.NotifyContext(context.Background(), serveSignals()...)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("gateway shutdown: %v", err)
		}
	}()

	log.Printf("gateway listening on %s", *listen)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("gateway listen: %v", err)
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

type targetRequest struct {
	Target string `json:"target"`
	controlRequestOptions
}

type controlRequestOptions struct {
	OperationID      string  `json:"operation_id,omitempty"`
	ExpectedRevision *uint64 `json:"expected_revision,omitempty"`
}

type drainRequest struct {
	controlRequestOptions
}

type nodeEndpointFlags map[string]string

type stringListFlags []string

func (f *stringListFlags) String() string {
	if f == nil || len(*f) == 0 {
		return ""
	}
	return strings.Join(*f, ",")
}

func (f *stringListFlags) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("value must not be empty")
	}
	*f = append(*f, value)
	return nil
}

func (f *nodeEndpointFlags) String() string {
	if f == nil || len(*f) == 0 {
		return ""
	}
	keys := make([]string, 0, len(*f))
	for key := range *f {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+(*f)[key])
	}
	return strings.Join(parts, ",")
}

func (f *nodeEndpointFlags) Set(value string) error {
	parts := strings.SplitN(value, "=", 2)
	if len(parts) != 2 {
		return fmt.Errorf("node endpoint must be node=url")
	}
	nodeID := strings.TrimSpace(parts[0])
	endpoint := strings.TrimSpace(parts[1])
	if nodeID == "" || endpoint == "" {
		return fmt.Errorf("node endpoint must be node=url")
	}
	if *f == nil {
		*f = make(map[string]string)
	}
	(*f)[nodeID] = normalizeHTTPBaseURL(endpoint)
	return nil
}

type clusterWriteOptions struct {
	Enabled       bool
	NodeEndpoints map[string]string
}

type clusterStatusResult struct {
	Nodes []clusterStatusNodeResult `json:"nodes"`
}

type clusterStatusNodeResult struct {
	Node     string             `json:"node"`
	Endpoint string             `json:"endpoint"`
	Status   *lsm.ClusterStatus `json:"status,omitempty"`
	Error    string             `json:"error,omitempty"`
}

type clusterWaitResult struct {
	Ready               bool                `json:"ready"`
	ReadyNodes          int                 `json:"ready_nodes"`
	RequiredReadyNodes  int                 `json:"required_ready_nodes"`
	WriteLeader         string              `json:"write_leader,omitempty"`
	WriteLeaderEndpoint string              `json:"write_leader_endpoint,omitempty"`
	Statuses            clusterStatusResult `json:"statuses"`
}

type waitClusterOptions struct {
	RequiredReadyNodes int
	RequireWriteLeader bool
	Timeout            time.Duration
	Interval           time.Duration
}

type drainNodeResult struct {
	Target      string              `json:"target"`
	SubmittedTo string              `json:"submitted_to"`
	Endpoint    string              `json:"endpoint"`
	Shards      []lsm.ShardStatus   `json:"shards"`
	Statuses    clusterStatusResult `json:"statuses"`
}

type membershipActionResult struct {
	Operation   string              `json:"operation"`
	Node        string              `json:"node"`
	Shard       string              `json:"shard,omitempty"`
	SubmittedTo string              `json:"submitted_to"`
	Endpoint    string              `json:"endpoint"`
	Shards      []lsm.ShardStatus   `json:"shards,omitempty"`
	Statuses    clusterStatusResult `json:"statuses"`
}

type replaceNodeResult struct {
	OldNode   string                     `json:"old_node"`
	NewNode   string                     `json:"new_node"`
	DryRun    bool                       `json:"dry_run,omitempty"`
	Shards    []string                   `json:"shards"`
	Preflight replaceNodePreflightResult `json:"preflight"`
	Steps     []membershipActionResult   `json:"steps"`
	Drain     drainNodeResult            `json:"drain"`
	Statuses  clusterStatusResult        `json:"statuses"`
}

type replaceNodePreflightResult struct {
	OldEndpoint         string                  `json:"old_endpoint"`
	NewEndpoint         string                  `json:"new_endpoint"`
	WriteLeader         string                  `json:"write_leader"`
	WriteLeaderEndpoint string                  `json:"write_leader_endpoint"`
	Policy              replacementPolicyResult `json:"policy"`
}

type replacementPolicyResult struct {
	Shards []replacementShardPolicyResult `json:"shards"`
}

type replacementShardPolicyResult struct {
	Shard                   string   `json:"shard"`
	ReplicaCount            int      `json:"replica_count"`
	RequiredHealthy         int      `json:"required_healthy"`
	HealthyRemaining        int      `json:"healthy_remaining"`
	HealthyRemainingNodes   []string `json:"healthy_remaining_nodes"`
	UnavailableReplicaNodes []string `json:"unavailable_replica_nodes,omitempty"`
}

type replacementPlanResult struct {
	OldNode       string                     `json:"old_node"`
	NewNode       string                     `json:"new_node"`
	Reason        string                     `json:"reason"`
	Shards        []string                   `json:"shards"`
	Preflight     replaceNodePreflightResult `json:"preflight"`
	DryRunCommand []string                   `json:"dry_run_command"`
	ApplyCommand  []string                   `json:"apply_command"`
	Statuses      clusterStatusResult        `json:"statuses"`
}

type replacementApplyResult struct {
	Plan   replacementPlanResult `json:"plan"`
	Result replaceNodeResult     `json:"result"`
}

type clusterReadOptions struct {
	Enabled       bool
	NodeEndpoints map[string]string
}

type replaceNodeOptions struct {
	OldNode                 string
	NewNode                 string
	ShardIDs                []string
	OperationPrefix         string
	DryRun                  bool
	AllowUnavailableOldNode bool
	CommandEndpoints        replacementCommandEndpointSource
}

type replacementCommandEndpointSource struct {
	ConfigPath string
	Addr       string
	Overrides  nodeEndpointFlags
}

func getCmd(args []string) {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	configPath := fs.String("config", "", "config file path")
	dataDir := fs.String("data-dir", "", "data directory")
	addr := fs.String("addr", "", "http address for server mode")
	clusterMode := fs.Bool("cluster", false, "read from the first reachable cluster endpoint")
	var nodeEndpoints nodeEndpointFlags
	fs.Var(&nodeEndpoints, "node-endpoint", "node endpoint mapping node=url for --cluster; may be repeated")
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
	clusterOpts, err := clusterReadOptionsFromConfig(cfg, *addr, *clusterMode, nodeEndpoints)
	if err != nil {
		log.Fatal(err)
	}
	result, err := readKVWithCluster(*addr, *dataDir, keyBytes, clusterOpts)
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
	clusterMode := fs.Bool("cluster", false, "read from the first reachable cluster endpoint")
	var nodeEndpoints nodeEndpointFlags
	fs.Var(&nodeEndpoints, "node-endpoint", "node endpoint mapping node=url for --cluster; may be repeated")
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
	clusterOpts, err := clusterReadOptionsFromConfig(cfg, *addr, *clusterMode, nodeEndpoints)
	if err != nil {
		log.Fatal(err)
	}
	result, err := readKVRangeWithCluster(*addr, *dataDir, startBytes, endBytes, *limit, clusterOpts)
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
	clusterMode := fs.Bool("cluster", false, "route remote write through the current cluster write leader")
	var nodeEndpoints nodeEndpointFlags
	fs.Var(&nodeEndpoints, "node-endpoint", "node endpoint mapping node=url for --cluster; may be repeated")
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
	clusterOpts, err := clusterWriteOptionsFromConfig(cfg, *addr, *clusterMode, nodeEndpoints)
	if err != nil {
		log.Fatal(err)
	}
	status, err := writeKVPutWithCluster(*addr, *dataDir, keyBytes, valueBytes, mode, clusterOpts)
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
	clusterMode := fs.Bool("cluster", false, "route remote write through the current cluster write leader")
	var nodeEndpoints nodeEndpointFlags
	fs.Var(&nodeEndpoints, "node-endpoint", "node endpoint mapping node=url for --cluster; may be repeated")
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
	clusterOpts, err := clusterWriteOptionsFromConfig(cfg, *addr, *clusterMode, nodeEndpoints)
	if err != nil {
		log.Fatal(err)
	}
	status, err := writeKVDeleteWithCluster(*addr, *dataDir, keyBytes, mode, clusterOpts)
	if err != nil {
		log.Fatal(err)
	}
	writeKVStatus(os.Stdout, status, *jsonOut)
}

func asyncPutCmd(args []string) {
	fs := flag.NewFlagSet("async-put", flag.ExitOnError)
	configPath := fs.String("config", "", "config file path")
	dataDir := fs.String("data-dir", "", "data directory")
	addr := fs.String("addr", "", "http address for server mode")
	key := fs.String("key", "", "key as UTF-8 text")
	keyBase64 := fs.String("key-base64", "", "key as base64")
	value := fs.String("value", "", "value as UTF-8 text")
	valueBase64 := fs.String("value-base64", "", "value as base64")
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
	status, err := writeKVPut(*addr, *dataDir, keyBytes, valueBytes, lsm.WriteConsistencyAccepted)
	if err != nil {
		log.Fatal(err)
	}
	writeKVStatus(os.Stdout, status, *jsonOut)
}

func asyncDeleteCmd(args []string) {
	fs := flag.NewFlagSet("async-delete", flag.ExitOnError)
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
	status, err := writeKVDelete(*addr, *dataDir, keyBytes, lsm.WriteConsistencyAccepted)
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

func clusterStatusCmd(args []string) {
	fs := flag.NewFlagSet("cluster-status", flag.ExitOnError)
	configPath := fs.String("config", "", "config file path")
	addr := fs.String("addr", "", "http address for one server node")
	var nodeEndpoints nodeEndpointFlags
	fs.Var(&nodeEndpoints, "node-endpoint", "node endpoint mapping node=url; may be repeated")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}
	cfg := loadConfigOrExit(*configPath)
	if *addr == "" {
		*addr = cfg.Addr
	}
	endpoints, err := clusterNodeEndpointsFromConfig(cfg, *addr, nodeEndpoints)
	if err != nil {
		log.Fatal(err)
	}
	if len(endpoints) == 0 {
		log.Fatal("cluster-status requires raft.peer_urls, raft.peer_url_file, --addr, or --node-endpoint")
	}
	statuses, err := readClusterStatuses(endpoints)
	if err != nil {
		log.Fatal(err)
	}
	if *jsonOut {
		writeJSON(os.Stdout, statuses)
		return
	}
	writeClusterStatuses(os.Stdout, statuses)
}

func waitClusterCmd(args []string) {
	fs := flag.NewFlagSet("wait-cluster", flag.ExitOnError)
	configPath := fs.String("config", "", "config file path")
	addr := fs.String("addr", "", "http address for one server node")
	minReady := fs.Int("min-ready", 0, "minimum healthy nodes required; 0 requires all configured endpoints")
	requireWriteLeader := fs.Bool("write-leader", true, "require a node that can accept committed writes")
	timeout := fs.Duration("timeout", 60*time.Second, "maximum time to wait")
	interval := fs.Duration("interval", 200*time.Millisecond, "poll interval")
	var nodeEndpoints nodeEndpointFlags
	fs.Var(&nodeEndpoints, "node-endpoint", "node endpoint mapping node=url; may be repeated")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}
	cfg := loadConfigOrExit(*configPath)
	if *addr == "" {
		*addr = cfg.Addr
	}
	endpoints, err := clusterNodeEndpointsFromConfig(cfg, *addr, nodeEndpoints)
	if err != nil {
		log.Fatal(err)
	}
	if len(endpoints) == 0 {
		log.Fatal("wait-cluster requires raft.peer_urls, raft.peer_url_file, --addr, or --node-endpoint")
	}
	result, err := waitCluster(endpoints, waitClusterOptions{
		RequiredReadyNodes: *minReady,
		RequireWriteLeader: *requireWriteLeader,
		Timeout:            *timeout,
		Interval:           *interval,
	})
	if err != nil {
		if *jsonOut {
			writeJSON(os.Stdout, result)
		}
		log.Fatal(err)
	}
	if *jsonOut {
		writeJSON(os.Stdout, result)
		return
	}
	writeClusterWait(os.Stdout, result)
}

func drainNodeCmd(args []string) {
	fs := flag.NewFlagSet("drain-node", flag.ExitOnError)
	configPath := fs.String("config", "", "config file path")
	addr := fs.String("addr", "", "http address for one server node")
	node := fs.String("node", "", "node id to drain")
	operationID := fs.String("operation-id", "", "idempotency key for the drain control mutation")
	expectedRevision := fs.Uint64("expected-revision", 0, "expected control revision; 0 disables the check")
	var nodeEndpoints nodeEndpointFlags
	fs.Var(&nodeEndpoints, "node-endpoint", "node endpoint mapping node=url; may be repeated")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}
	if *node == "" && fs.NArg() > 0 {
		*node = fs.Arg(0)
	}
	if strings.TrimSpace(*node) == "" {
		log.Fatal("--node required")
	}
	cfg := loadConfigOrExit(*configPath)
	if *addr == "" {
		*addr = cfg.Addr
	}
	endpoints, err := clusterNodeEndpointsFromConfig(cfg, *addr, nodeEndpoints)
	if err != nil {
		log.Fatal(err)
	}
	if len(endpoints) == 0 {
		log.Fatal("drain-node requires raft.peer_urls, raft.peer_url_file, --addr, or --node-endpoint")
	}
	opts := controlRequestOptions{OperationID: strings.TrimSpace(*operationID)}
	if *expectedRevision != 0 {
		revision := *expectedRevision
		opts.ExpectedRevision = &revision
	}
	result, err := drainClusterNode(endpoints, *node, opts)
	if err != nil {
		log.Fatal(err)
	}
	if *jsonOut {
		writeJSON(os.Stdout, result)
		return
	}
	writeDrainNodeResult(os.Stdout, result)
}

func resumeNodeCmd(args []string) {
	fs := flag.NewFlagSet("resume-node", flag.ExitOnError)
	configPath := fs.String("config", "", "config file path")
	addr := fs.String("addr", "", "http address for one server node")
	node := fs.String("node", "", "node id to resume")
	operationID := fs.String("operation-id", "", "idempotency key for the resume control mutation")
	expectedRevision := fs.Uint64("expected-revision", 0, "expected control revision; 0 disables the check")
	var nodeEndpoints nodeEndpointFlags
	fs.Var(&nodeEndpoints, "node-endpoint", "node endpoint mapping node=url; may be repeated")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}
	if *node == "" && fs.NArg() > 0 {
		*node = fs.Arg(0)
	}
	if strings.TrimSpace(*node) == "" {
		log.Fatal("--node required")
	}
	cfg := loadConfigOrExit(*configPath)
	if *addr == "" {
		*addr = cfg.Addr
	}
	endpoints, err := clusterNodeEndpointsFromConfig(cfg, *addr, nodeEndpoints)
	if err != nil {
		log.Fatal(err)
	}
	if len(endpoints) == 0 {
		log.Fatal("resume-node requires raft.peer_urls, raft.peer_url_file, --addr, or --node-endpoint")
	}
	opts := controlRequestOptions{OperationID: strings.TrimSpace(*operationID)}
	if *expectedRevision != 0 {
		revision := *expectedRevision
		opts.ExpectedRevision = &revision
	}
	result, err := resumeClusterNode(endpoints, *node, opts)
	if err != nil {
		log.Fatal(err)
	}
	if *jsonOut {
		writeJSON(os.Stdout, result)
		return
	}
	writeDrainNodeResult(os.Stdout, result)
}

func raftAddNodeCmd(args []string) {
	membershipNodeCmd("raft-add-node", args, "raft-add")
}

func raftRemoveNodeCmd(args []string) {
	membershipNodeCmd("raft-remove-node", args, "raft-remove")
}

func membershipNodeCmd(command string, args []string, action string) {
	fs := flag.NewFlagSet(command, flag.ExitOnError)
	configPath := fs.String("config", "", "config file path")
	addr := fs.String("addr", "", "http address for one server node")
	node := fs.String("node", "", "node id")
	var nodeEndpoints nodeEndpointFlags
	fs.Var(&nodeEndpoints, "node-endpoint", "node endpoint mapping node=url; may be repeated")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}
	if *node == "" && fs.NArg() > 0 {
		*node = fs.Arg(0)
	}
	if strings.TrimSpace(*node) == "" {
		log.Fatal("--node required")
	}
	cfg := loadConfigOrExit(*configPath)
	if *addr == "" {
		*addr = cfg.Addr
	}
	endpoints, err := clusterNodeEndpointsFromConfig(cfg, *addr, nodeEndpoints)
	if err != nil {
		log.Fatal(err)
	}
	if len(endpoints) == 0 {
		log.Fatalf("%s requires raft.peer_urls, raft.peer_url_file, --addr, or --node-endpoint", command)
	}
	result, err := changeRaftMembership(endpoints, action, *node)
	if err != nil {
		log.Fatal(err)
	}
	if *jsonOut {
		writeJSON(os.Stdout, result)
		return
	}
	writeMembershipActionResult(os.Stdout, result)
}

func shardAddReplicaCmd(args []string) {
	shardReplicaCmd("shard-add-replica", args, "add-replica")
}

func shardRemoveReplicaCmd(args []string) {
	shardReplicaCmd("shard-remove-replica", args, "remove-replica")
}

func shardReplicaCmd(command string, args []string, action string) {
	fs := flag.NewFlagSet(command, flag.ExitOnError)
	configPath := fs.String("config", "", "config file path")
	addr := fs.String("addr", "", "http address for one server node")
	shard := fs.String("shard", "", "shard id")
	node := fs.String("node", "", "node id")
	operationID := fs.String("operation-id", "", "idempotency key for the shard membership mutation")
	expectedRevision := fs.Uint64("expected-revision", 0, "expected control revision; 0 disables the check")
	var nodeEndpoints nodeEndpointFlags
	fs.Var(&nodeEndpoints, "node-endpoint", "node endpoint mapping node=url; may be repeated")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}
	if *shard == "" && fs.NArg() > 0 {
		*shard = fs.Arg(0)
	}
	if *node == "" && fs.NArg() > 1 {
		*node = fs.Arg(1)
	}
	if strings.TrimSpace(*shard) == "" {
		log.Fatal("--shard required")
	}
	if strings.TrimSpace(*node) == "" {
		log.Fatal("--node required")
	}
	cfg := loadConfigOrExit(*configPath)
	if *addr == "" {
		*addr = cfg.Addr
	}
	endpoints, err := clusterNodeEndpointsFromConfig(cfg, *addr, nodeEndpoints)
	if err != nil {
		log.Fatal(err)
	}
	if len(endpoints) == 0 {
		log.Fatalf("%s requires raft.peer_urls, raft.peer_url_file, --addr, or --node-endpoint", command)
	}
	opts := controlRequestOptions{OperationID: strings.TrimSpace(*operationID)}
	if *expectedRevision != 0 {
		revision := *expectedRevision
		opts.ExpectedRevision = &revision
	}
	result, err := changeShardReplica(endpoints, action, *shard, *node, opts)
	if err != nil {
		log.Fatal(err)
	}
	if *jsonOut {
		writeJSON(os.Stdout, result)
		return
	}
	writeMembershipActionResult(os.Stdout, result)
}

func replaceNodeCmd(args []string) {
	fs := flag.NewFlagSet("replace-node", flag.ExitOnError)
	configPath := fs.String("config", "", "config file path")
	addr := fs.String("addr", "", "http address for one server node")
	oldNode := fs.String("old-node", "", "node id being replaced")
	newNode := fs.String("new-node", "", "replacement node id")
	operationPrefix := fs.String("operation-prefix", "", "idempotency key prefix for committed shard mutations")
	dryRun := fs.Bool("dry-run", false, "preflight and print the replacement plan without submitting mutations")
	allowUnavailableOldNode := fs.Bool("allow-unavailable-old-node", false, "complete replacement drain when old node status is unreachable after shard leadership has moved")
	var shardIDs stringListFlags
	fs.Var(&shardIDs, "shard", "shard id to migrate; may be repeated; defaults to all shards containing --old-node")
	var nodeEndpoints nodeEndpointFlags
	fs.Var(&nodeEndpoints, "node-endpoint", "node endpoint mapping node=url; may be repeated")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}
	if strings.TrimSpace(*oldNode) == "" {
		log.Fatal("--old-node required")
	}
	if strings.TrimSpace(*newNode) == "" {
		log.Fatal("--new-node required")
	}
	cfg := loadConfigOrExit(*configPath)
	if *addr == "" {
		*addr = cfg.Addr
	}
	endpoints, err := clusterNodeEndpointsFromConfig(cfg, *addr, nodeEndpoints)
	if err != nil {
		log.Fatal(err)
	}
	if len(endpoints) == 0 {
		log.Fatal("replace-node requires raft.peer_urls, raft.peer_url_file, --addr, or --node-endpoint")
	}
	result, err := replaceClusterNode(endpoints, replaceNodeOptions{
		OldNode:                 *oldNode,
		NewNode:                 *newNode,
		ShardIDs:                []string(shardIDs),
		OperationPrefix:         *operationPrefix,
		DryRun:                  *dryRun,
		AllowUnavailableOldNode: *allowUnavailableOldNode,
	})
	if err != nil {
		log.Fatal(err)
	}
	if *jsonOut {
		writeJSON(os.Stdout, result)
		return
	}
	writeReplaceNodeResult(os.Stdout, result)
}

func replacementPlanCmd(args []string) {
	fs := flag.NewFlagSet("replacement-plan", flag.ExitOnError)
	configPath := fs.String("config", "", "config file path")
	addr := fs.String("addr", "", "http address for one server node")
	oldNode := fs.String("old-node", "", "node id to replace; if omitted, exactly one unavailable node is selected")
	newNode := fs.String("new-node", "", "replacement node id")
	operationPrefix := fs.String("operation-prefix", "", "idempotency key prefix for the suggested replace-node command")
	var shardIDs stringListFlags
	fs.Var(&shardIDs, "shard", "shard id to migrate; may be repeated; defaults to all shards containing selected old node")
	var nodeEndpoints nodeEndpointFlags
	fs.Var(&nodeEndpoints, "node-endpoint", "node endpoint mapping node=url; may be repeated")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}
	if strings.TrimSpace(*newNode) == "" {
		log.Fatal("--new-node required")
	}
	cfg := loadConfigOrExit(*configPath)
	commandEndpointSource := replacementCommandEndpointSourceFromFlags(*configPath, *addr, nodeEndpoints)
	if *addr == "" {
		*addr = cfg.Addr
	}
	endpoints, err := clusterNodeEndpointsFromConfig(cfg, *addr, nodeEndpoints)
	if err != nil {
		log.Fatal(err)
	}
	if len(endpoints) == 0 {
		log.Fatal("replacement-plan requires raft.peer_urls, raft.peer_url_file, --addr, or --node-endpoint")
	}
	result, err := planReplacementNode(endpoints, replaceNodeOptions{
		OldNode:          *oldNode,
		NewNode:          *newNode,
		ShardIDs:         []string(shardIDs),
		OperationPrefix:  *operationPrefix,
		DryRun:           true,
		CommandEndpoints: commandEndpointSource,
	})
	if err != nil {
		log.Fatal(err)
	}
	if *jsonOut {
		writeJSON(os.Stdout, result)
		return
	}
	writeReplacementPlan(os.Stdout, result)
}

func replacementApplyCmd(args []string) {
	fs := flag.NewFlagSet("replacement-apply", flag.ExitOnError)
	configPath := fs.String("config", "", "config file path")
	addr := fs.String("addr", "", "http address for one server node")
	oldNode := fs.String("old-node", "", "node id to replace; if omitted, exactly one unavailable node is selected")
	newNode := fs.String("new-node", "", "replacement node id")
	operationPrefix := fs.String("operation-prefix", "", "idempotency key prefix for committed shard mutations")
	var shardIDs stringListFlags
	fs.Var(&shardIDs, "shard", "shard id to migrate; may be repeated; defaults to all shards containing selected old node")
	var nodeEndpoints nodeEndpointFlags
	fs.Var(&nodeEndpoints, "node-endpoint", "node endpoint mapping node=url; may be repeated")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}
	if strings.TrimSpace(*newNode) == "" {
		log.Fatal("--new-node required")
	}
	cfg := loadConfigOrExit(*configPath)
	commandEndpointSource := replacementCommandEndpointSourceFromFlags(*configPath, *addr, nodeEndpoints)
	if *addr == "" {
		*addr = cfg.Addr
	}
	endpoints, err := clusterNodeEndpointsFromConfig(cfg, *addr, nodeEndpoints)
	if err != nil {
		log.Fatal(err)
	}
	if len(endpoints) == 0 {
		log.Fatal("replacement-apply requires raft.peer_urls, raft.peer_url_file, --addr, or --node-endpoint")
	}
	result, err := applyPlannedReplacement(endpoints, replaceNodeOptions{
		OldNode:          *oldNode,
		NewNode:          *newNode,
		ShardIDs:         []string(shardIDs),
		OperationPrefix:  *operationPrefix,
		CommandEndpoints: commandEndpointSource,
	})
	if err != nil {
		log.Fatal(err)
	}
	if *jsonOut {
		writeJSON(os.Stdout, result)
		return
	}
	writeReplacementApply(os.Stdout, result)
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

func readKVWithCluster(addr, dataDir string, key []byte, opts clusterReadOptions) (kvGetResult, error) {
	if !opts.Enabled {
		return readKV(addr, dataDir, key)
	}
	return readKVFromCluster(opts.NodeEndpoints, key)
}

func readKVFromCluster(endpoints map[string]string, key []byte) (kvGetResult, error) {
	if len(endpoints) == 0 {
		return kvGetResult{}, fmt.Errorf("cluster read requires node endpoints")
	}
	var lastErr error
	for _, nodeID := range sortedEndpointNodes(endpoints) {
		result, err := readKV(endpoints[nodeID], "", key)
		if err == nil {
			return result, nil
		}
		lastErr = fmt.Errorf("%s: %w", nodeID, err)
	}
	if lastErr != nil {
		return kvGetResult{}, lastErr
	}
	return kvGetResult{}, fmt.Errorf("cluster read unavailable")
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

func readKVRangeWithCluster(
	addr string,
	dataDir string,
	start []byte,
	end []byte,
	limit int,
	opts clusterReadOptions,
) (kvRangeResult, error) {
	if !opts.Enabled {
		return readKVRange(addr, dataDir, start, end, limit)
	}
	return readKVRangeFromCluster(opts.NodeEndpoints, start, end, limit)
}

func readKVRangeFromCluster(endpoints map[string]string, start []byte, end []byte, limit int) (kvRangeResult, error) {
	if len(endpoints) == 0 {
		return kvRangeResult{}, fmt.Errorf("cluster range requires node endpoints")
	}
	var lastErr error
	for _, nodeID := range sortedEndpointNodes(endpoints) {
		result, err := readKVRange(endpoints[nodeID], "", start, end, limit)
		if err == nil {
			return result, nil
		}
		lastErr = fmt.Errorf("%s: %w", nodeID, err)
	}
	if lastErr != nil {
		return kvRangeResult{}, lastErr
	}
	return kvRangeResult{}, fmt.Errorf("cluster range unavailable")
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

func writeKVPutWithCluster(
	addr string,
	dataDir string,
	key []byte,
	value []byte,
	consistency lsm.WriteConsistency,
	clusterOpts clusterWriteOptions,
) (lsm.WriteRequestStatus, error) {
	if clusterOpts.Enabled {
		return writeClusterKV("put", clusterOpts.NodeEndpoints, key, value, consistency)
	}
	return writeKVPut(addr, dataDir, key, value, consistency)
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

func writeKVDeleteWithCluster(
	addr string,
	dataDir string,
	key []byte,
	consistency lsm.WriteConsistency,
	clusterOpts clusterWriteOptions,
) (lsm.WriteRequestStatus, error) {
	if clusterOpts.Enabled {
		return writeClusterKV("delete", clusterOpts.NodeEndpoints, key, nil, consistency)
	}
	return writeKVDelete(addr, dataDir, key, consistency)
}

func writeClusterKV(
	operation string,
	endpoints map[string]string,
	key []byte,
	value []byte,
	consistency lsm.WriteConsistency,
) (lsm.WriteRequestStatus, error) {
	if len(endpoints) == 0 {
		return lsm.WriteRequestStatus{}, fmt.Errorf("cluster write requires node endpoints")
	}
	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		nodeID, endpoint, err := currentClusterWriteLeader(endpoints)
		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if err := alignShardLeader(endpoint, key, nodeID); err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		var status lsm.WriteRequestStatus
		switch operation {
		case "put":
			status, err = writeKVPut(endpoint, "", key, value, consistency)
		case "delete":
			status, err = writeKVDelete(endpoint, "", key, consistency)
		default:
			return lsm.WriteRequestStatus{}, fmt.Errorf("unsupported cluster write operation %q", operation)
		}
		if err == nil {
			return status, nil
		}
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}
	if lastErr != nil {
		return lsm.WriteRequestStatus{}, lastErr
	}
	return lsm.WriteRequestStatus{}, fmt.Errorf("cluster write timed out")
}

func currentClusterWriteLeader(endpoints map[string]string) (string, string, error) {
	nodes := sortedEndpointNodes(endpoints)
	var lastErr error
	for _, nodeID := range nodes {
		endpoint := endpoints[nodeID]
		var status lsm.ClusterStatus
		if err := getJSON(endpoint+"/cluster/status", &status); err != nil {
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

func readClusterStatuses(endpoints map[string]string) (clusterStatusResult, error) {
	nodes := sortedEndpointNodes(endpoints)
	result := clusterStatusResult{
		Nodes: make([]clusterStatusNodeResult, 0, len(nodes)),
	}
	successes := 0
	var lastErr error
	for _, nodeID := range nodes {
		endpoint := endpoints[nodeID]
		var status lsm.ClusterStatus
		err := getJSON(endpoint+"/cluster/status", &status)
		node := clusterStatusNodeResult{
			Node:     nodeID,
			Endpoint: endpoint,
		}
		if err != nil {
			node.Error = err.Error()
			lastErr = err
		} else {
			successes++
			if strings.TrimSpace(status.NodeID) != "" {
				node.Node = status.NodeID
			}
			node.Status = &status
		}
		result.Nodes = append(result.Nodes, node)
	}
	if successes == 0 && lastErr != nil {
		return result, lastErr
	}
	return result, nil
}

func waitCluster(endpoints map[string]string, opts waitClusterOptions) (clusterWaitResult, error) {
	if len(endpoints) == 0 {
		return clusterWaitResult{}, fmt.Errorf("wait-cluster requires node endpoints")
	}
	if opts.RequiredReadyNodes < 0 {
		return clusterWaitResult{}, fmt.Errorf("min-ready must be non-negative")
	}
	if opts.RequiredReadyNodes > len(endpoints) {
		return clusterWaitResult{}, fmt.Errorf("min-ready cannot exceed configured endpoints")
	}
	if opts.RequiredReadyNodes == 0 {
		opts.RequiredReadyNodes = len(endpoints)
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 60 * time.Second
	}
	if opts.Interval <= 0 {
		opts.Interval = 200 * time.Millisecond
	}
	deadline := time.Now().Add(opts.Timeout)
	var last clusterWaitResult
	var lastErr error
	for {
		statuses, err := readClusterStatuses(endpoints)
		if err != nil {
			lastErr = err
		}
		last = evaluateClusterWait(statuses, opts)
		if last.Ready {
			return last, nil
		}
		if time.Now().After(deadline) {
			if lastErr != nil && len(last.Statuses.Nodes) == 0 {
				return last, lastErr
			}
			return last, fmt.Errorf(
				"wait-cluster timed out: ready nodes %d/%d write_leader=%v",
				last.ReadyNodes,
				last.RequiredReadyNodes,
				last.WriteLeader != "",
			)
		}
		time.Sleep(opts.Interval)
	}
}

func evaluateClusterWait(statuses clusterStatusResult, opts waitClusterOptions) clusterWaitResult {
	result := clusterWaitResult{
		RequiredReadyNodes: opts.RequiredReadyNodes,
		Statuses:           statuses,
	}
	for _, node := range statuses.Nodes {
		if clusterNodeReplacementHealthy(node) {
			result.ReadyNodes++
		}
		if node.Error != "" || node.Status == nil {
			continue
		}
		runtime := node.Status.CommitLogRuntime
		if runtime.Leader && runtime.WriteAvailable {
			result.WriteLeader = node.Status.NodeID
			if strings.TrimSpace(result.WriteLeader) == "" {
				result.WriteLeader = node.Node
			}
			result.WriteLeaderEndpoint = node.Endpoint
		}
	}
	result.Ready = result.ReadyNodes >= result.RequiredReadyNodes
	if opts.RequireWriteLeader {
		result.Ready = result.Ready && result.WriteLeader != ""
	}
	return result
}

func writeClusterStatuses(w io.Writer, result clusterStatusResult) {
	for _, node := range result.Nodes {
		if node.Error != "" {
			fmt.Fprintf(w, "node=%s endpoint=%s ok=false error=%q\n", node.Node, node.Endpoint, node.Error)
			continue
		}
		status := node.Status
		if status == nil {
			fmt.Fprintf(w, "node=%s endpoint=%s ok=false error=%q\n", node.Node, node.Endpoint, "missing status")
			continue
		}
		runtime := status.CommitLogRuntime
		fmt.Fprintf(
			w,
			"node=%s endpoint=%s ok=true health=%s leader=%v write_available=%v leader_known=%v term=%d index=%d revision=%d shards=%d draining=%v\n",
			status.NodeID,
			node.Endpoint,
			runtime.Health,
			runtime.Leader,
			runtime.WriteAvailable,
			runtime.LeaderKnown,
			runtime.Term,
			runtime.Index,
			status.Revision,
			status.ShardCount,
			status.Draining,
		)
	}
}

func writeClusterWait(w io.Writer, result clusterWaitResult) {
	fmt.Fprintf(
		w,
		"ready=%v ready_nodes=%d required_ready=%d write_leader=%s write_leader_endpoint=%s\n",
		result.Ready,
		result.ReadyNodes,
		result.RequiredReadyNodes,
		result.WriteLeader,
		result.WriteLeaderEndpoint,
	)
	writeClusterStatuses(w, result.Statuses)
}

func drainClusterNode(
	endpoints map[string]string,
	target string,
	opts controlRequestOptions,
) (drainNodeResult, error) {
	return drainClusterNodeWithOptions(endpoints, target, opts, false)
}

func drainClusterNodeWithOptions(
	endpoints map[string]string,
	target string,
	opts controlRequestOptions,
	allowUnavailableTarget bool,
) (drainNodeResult, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return drainNodeResult{}, fmt.Errorf("target node required")
	}
	if len(endpoints) == 0 {
		return drainNodeResult{}, fmt.Errorf("drain-node requires node endpoints")
	}
	deadline := time.Now().Add(15 * time.Second)
	var result drainNodeResult
	var lastErr error
	for time.Now().Before(deadline) {
		if result.Target == "" {
			nodeID, endpoint, err := currentClusterWriteLeader(endpoints)
			if err != nil {
				lastErr = err
				time.Sleep(200 * time.Millisecond)
				continue
			}
			if err := postControlAction(endpoint+"/cluster/nodes/"+url.PathEscape(target)+"/drain", drainRequest{
				controlRequestOptions: opts,
			}); err != nil {
				lastErr = err
				time.Sleep(200 * time.Millisecond)
				continue
			}
			result = drainNodeResult{
				Target:      target,
				SubmittedTo: nodeID,
				Endpoint:    endpoint,
			}
		}
		if completed, next, err := readDrainNodeResult(result.Endpoint, endpoints, result, allowUnavailableTarget); err == nil {
			result = next
			if completed {
				return result, nil
			}
		} else {
			lastErr = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	if result.Target != "" {
		return result, fmt.Errorf("drain-node timed out waiting for node %q to drain", target)
	}
	if lastErr != nil {
		return drainNodeResult{}, lastErr
	}
	return drainNodeResult{}, fmt.Errorf("drain-node timed out")
}

func readDrainNodeResult(
	controlEndpoint string,
	endpoints map[string]string,
	result drainNodeResult,
	allowUnavailableTarget bool,
) (bool, drainNodeResult, error) {
	var shards []lsm.ShardStatus
	if err := getJSON(controlEndpoint+"/cluster/shards", &shards); err != nil {
		return false, result, err
	}
	statuses, err := readClusterStatuses(endpoints)
	if err != nil {
		return false, result, err
	}
	result.Shards = shards
	result.Statuses = statuses
	return drainComplete(result, allowUnavailableTarget), result, nil
}

func drainComplete(result drainNodeResult, allowUnavailableTarget bool) bool {
	for _, shard := range result.Shards {
		if shard.Leader == result.Target {
			return false
		}
	}
	for _, node := range result.Statuses.Nodes {
		if node.Node != result.Target {
			continue
		}
		if allowUnavailableTarget && (node.Error != "" || node.Status == nil) {
			return true
		}
		return node.Status != nil && node.Status.Draining
	}
	return false
}

func resumeClusterNode(
	endpoints map[string]string,
	target string,
	opts controlRequestOptions,
) (drainNodeResult, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return drainNodeResult{}, fmt.Errorf("target node required")
	}
	if len(endpoints) == 0 {
		return drainNodeResult{}, fmt.Errorf("resume-node requires node endpoints")
	}
	deadline := time.Now().Add(15 * time.Second)
	var result drainNodeResult
	var lastErr error
	for time.Now().Before(deadline) {
		if result.Target == "" {
			nodeID, endpoint, err := currentClusterWriteLeader(endpoints)
			if err != nil {
				lastErr = err
				time.Sleep(200 * time.Millisecond)
				continue
			}
			if err := postControlAction(endpoint+"/cluster/nodes/"+url.PathEscape(target)+"/resume", drainRequest{
				controlRequestOptions: opts,
			}); err != nil {
				lastErr = err
				time.Sleep(200 * time.Millisecond)
				continue
			}
			result = drainNodeResult{
				Target:      target,
				SubmittedTo: nodeID,
				Endpoint:    endpoint,
			}
		}
		if _, next, err := readDrainNodeResult(result.Endpoint, endpoints, result, false); err == nil {
			result = next
			if resumeComplete(result) {
				return result, nil
			}
		} else {
			lastErr = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	if result.Target != "" {
		return result, fmt.Errorf("resume-node timed out waiting for node %q to resume", target)
	}
	if lastErr != nil {
		return drainNodeResult{}, lastErr
	}
	return drainNodeResult{}, fmt.Errorf("resume-node timed out")
}

func resumeComplete(result drainNodeResult) bool {
	for _, node := range result.Statuses.Nodes {
		if node.Node != result.Target {
			continue
		}
		return node.Status != nil && !node.Status.Draining
	}
	return false
}

func changeRaftMembership(
	endpoints map[string]string,
	action string,
	node string,
) (membershipActionResult, error) {
	node = strings.TrimSpace(node)
	if node == "" {
		return membershipActionResult{}, fmt.Errorf("node required")
	}
	nodeID, endpoint, err := currentClusterWriteLeader(endpoints)
	if err != nil {
		return membershipActionResult{}, err
	}
	if err := postControlAction(endpoint+"/cluster/nodes/"+url.PathEscape(node)+"/"+url.PathEscape(action), drainRequest{}); err != nil {
		return membershipActionResult{}, err
	}
	statuses, err := readClusterStatuses(endpoints)
	if err != nil {
		return membershipActionResult{}, err
	}
	return membershipActionResult{
		Operation:   action,
		Node:        node,
		SubmittedTo: nodeID,
		Endpoint:    endpoint,
		Statuses:    statuses,
	}, nil
}

func changeShardReplica(
	endpoints map[string]string,
	action string,
	shardID string,
	node string,
	opts controlRequestOptions,
) (membershipActionResult, error) {
	shardID = strings.TrimSpace(shardID)
	node = strings.TrimSpace(node)
	if shardID == "" {
		return membershipActionResult{}, fmt.Errorf("shard required")
	}
	if node == "" {
		return membershipActionResult{}, fmt.Errorf("node required")
	}
	deadline := time.Now().Add(15 * time.Second)
	var result membershipActionResult
	var lastErr error
	for time.Now().Before(deadline) {
		if result.Operation == "" {
			nodeID, endpoint, err := currentClusterWriteLeader(endpoints)
			if err != nil {
				lastErr = err
				time.Sleep(200 * time.Millisecond)
				continue
			}
			if err := postControlAction(
				endpoint+"/cluster/shards/"+url.PathEscape(shardID)+"/"+url.PathEscape(action),
				targetRequest{
					Target:                node,
					controlRequestOptions: opts,
				},
			); err != nil {
				lastErr = err
				time.Sleep(200 * time.Millisecond)
				continue
			}
			result = membershipActionResult{
				Operation:   action,
				Node:        node,
				Shard:       shardID,
				SubmittedTo: nodeID,
				Endpoint:    endpoint,
			}
		}
		next, err := readMembershipActionResult(result.Endpoint, endpoints, result)
		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		result = next
		if shardReplicaActionComplete(result) {
			return result, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	if result.Operation != "" {
		return result, fmt.Errorf("%s timed out waiting for shard %q membership", action, shardID)
	}
	if lastErr != nil {
		return membershipActionResult{}, lastErr
	}
	return membershipActionResult{}, fmt.Errorf("%s timed out", action)
}

func readMembershipActionResult(
	controlEndpoint string,
	endpoints map[string]string,
	result membershipActionResult,
) (membershipActionResult, error) {
	var shards []lsm.ShardStatus
	if err := getJSON(controlEndpoint+"/cluster/shards", &shards); err != nil {
		return result, err
	}
	statuses, err := readClusterStatuses(endpoints)
	if err != nil {
		return result, err
	}
	result.Shards = shards
	result.Statuses = statuses
	return result, nil
}

func shardReplicaActionComplete(result membershipActionResult) bool {
	for _, shard := range result.Shards {
		if shard.ID != result.Shard {
			continue
		}
		has := hasShardReplica(shard, result.Node)
		switch result.Operation {
		case "add-replica":
			return has
		case "remove-replica":
			return !has
		default:
			return false
		}
	}
	return false
}

func hasShardReplica(shard lsm.ShardStatus, node string) bool {
	for _, replica := range shard.Replicas {
		if replica.NodeID == node {
			return true
		}
	}
	return false
}

func replaceClusterNode(endpoints map[string]string, opts replaceNodeOptions) (replaceNodeResult, error) {
	plan, err := preflightReplaceClusterNode(endpoints, opts)
	if err != nil {
		return replaceNodeResult{}, err
	}
	result := replaceNodeResult{
		OldNode:   plan.oldNode,
		NewNode:   plan.newNode,
		DryRun:    opts.DryRun,
		Shards:    plan.shardIDs,
		Preflight: plan.preflight,
		Statuses:  plan.statuses,
	}
	if opts.DryRun {
		return result, nil
	}
	prefix := strings.TrimSpace(opts.OperationPrefix)
	if prefix == "" {
		prefix = "replace-" + plan.oldNode + "-with-" + plan.newNode
	}
	raftAdd, err := changeRaftMembership(endpoints, "raft-add", plan.newNode)
	if err != nil {
		return result, err
	}
	result.Steps = append(result.Steps, raftAdd)
	for _, shardID := range plan.shardIDs {
		step, err := changeShardReplica(endpoints, "add-replica", shardID, plan.newNode, controlRequestOptions{
			OperationID: prefix + "-add-" + shardID + "-" + plan.newNode,
		})
		if err != nil {
			return result, err
		}
		result.Steps = append(result.Steps, step)
	}
	drain, err := drainClusterNodeWithOptions(
		endpoints,
		plan.oldNode,
		controlRequestOptions{OperationID: prefix + "-drain-" + plan.oldNode},
		opts.AllowUnavailableOldNode,
	)
	if err != nil {
		return result, err
	}
	result.Drain = drain
	for _, shardID := range plan.shardIDs {
		step, err := changeShardReplica(endpoints, "remove-replica", shardID, plan.oldNode, controlRequestOptions{
			OperationID: prefix + "-remove-" + shardID + "-" + plan.oldNode,
		})
		if err != nil {
			return result, err
		}
		result.Steps = append(result.Steps, step)
	}
	raftRemove, err := changeRaftMembership(endpoints, "raft-remove", plan.oldNode)
	if err != nil {
		return result, err
	}
	result.Steps = append(result.Steps, raftRemove)
	statuses, err := readClusterStatuses(endpoints)
	if err != nil {
		return result, err
	}
	result.Statuses = statuses
	if result.Drain.SubmittedTo == "" {
		result.Drain.SubmittedTo = plan.preflight.WriteLeader
		result.Drain.Endpoint = plan.preflight.WriteLeaderEndpoint
	}
	return result, nil
}

type replaceNodePlan struct {
	oldNode   string
	newNode   string
	shardIDs  []string
	preflight replaceNodePreflightResult
	statuses  clusterStatusResult
}

func preflightReplaceClusterNode(endpoints map[string]string, opts replaceNodeOptions) (replaceNodePlan, error) {
	oldNode := strings.TrimSpace(opts.OldNode)
	newNode := strings.TrimSpace(opts.NewNode)
	if oldNode == "" {
		return replaceNodePlan{}, fmt.Errorf("old node required")
	}
	if newNode == "" {
		return replaceNodePlan{}, fmt.Errorf("new node required")
	}
	if oldNode == newNode {
		return replaceNodePlan{}, fmt.Errorf("old node and new node must differ")
	}
	if len(endpoints) == 0 {
		return replaceNodePlan{}, fmt.Errorf("replace-node requires node endpoints")
	}
	oldEndpoint, ok := endpointForNode(endpoints, oldNode)
	if !ok {
		return replaceNodePlan{}, fmt.Errorf("old node %q endpoint required", oldNode)
	}
	newEndpoint, ok := endpointForNode(endpoints, newNode)
	if !ok {
		return replaceNodePlan{}, fmt.Errorf("new node %q endpoint required", newNode)
	}
	nodeID, endpoint, err := currentClusterWriteLeader(endpoints)
	if err != nil {
		return replaceNodePlan{}, err
	}
	shards, err := readShards(endpoint)
	if err != nil {
		return replaceNodePlan{}, err
	}
	shardIDs, err := replacementShardIDs(shards, oldNode, opts.ShardIDs)
	if err != nil {
		return replaceNodePlan{}, err
	}
	statuses, err := readClusterStatuses(endpoints)
	if err != nil {
		return replaceNodePlan{}, err
	}
	if err := validateReplacementEndpointIdentity(statuses, oldNode, oldEndpoint, false); err != nil {
		return replaceNodePlan{}, err
	}
	if err := validateReplacementEndpointIdentity(statuses, newNode, newEndpoint, true); err != nil {
		return replaceNodePlan{}, err
	}
	policy, err := validateReplacementPolicy(shards, shardIDs, statuses, oldNode)
	if err != nil {
		return replaceNodePlan{}, err
	}
	return replaceNodePlan{
		oldNode:  oldNode,
		newNode:  newNode,
		shardIDs: shardIDs,
		preflight: replaceNodePreflightResult{
			OldEndpoint:         oldEndpoint,
			NewEndpoint:         newEndpoint,
			WriteLeader:         nodeID,
			WriteLeaderEndpoint: endpoint,
			Policy:              policy,
		},
		statuses: statuses,
	}, nil
}

func endpointForNode(endpoints map[string]string, node string) (string, bool) {
	node = strings.TrimSpace(node)
	for candidate, endpoint := range endpoints {
		if strings.TrimSpace(candidate) == node && strings.TrimSpace(endpoint) != "" {
			return normalizeHTTPBaseURL(endpoint), true
		}
	}
	return "", false
}

func validateReplacementEndpointIdentity(statuses clusterStatusResult, nodeID, endpoint string, requireReachable bool) error {
	var matched *clusterStatusNodeResult
	for i := range statuses.Nodes {
		if normalizeHTTPBaseURL(statuses.Nodes[i].Endpoint) == normalizeHTTPBaseURL(endpoint) {
			matched = &statuses.Nodes[i]
			break
		}
	}
	if matched == nil {
		if requireReachable {
			return fmt.Errorf("node %q endpoint %q was not checked", nodeID, endpoint)
		}
		return nil
	}
	if matched.Error != "" {
		if requireReachable {
			return fmt.Errorf("node %q endpoint %q is not reachable: %s", nodeID, endpoint, matched.Error)
		}
		return nil
	}
	if matched.Status != nil && strings.TrimSpace(matched.Status.NodeID) != "" && strings.TrimSpace(matched.Status.NodeID) != nodeID {
		return fmt.Errorf("endpoint for node %q reports node id %q", nodeID, matched.Status.NodeID)
	}
	return nil
}

func validateReplacementPolicy(shards []lsm.ShardStatus, shardIDs []string, statuses clusterStatusResult, oldNode string) (replacementPolicyResult, error) {
	byID := make(map[string]lsm.ShardStatus, len(shards))
	for _, shard := range shards {
		byID[shard.ID] = shard
	}
	out := replacementPolicyResult{
		Shards: make([]replacementShardPolicyResult, 0, len(shardIDs)),
	}
	for _, shardID := range shardIDs {
		shard, ok := byID[shardID]
		if !ok {
			return replacementPolicyResult{}, fmt.Errorf("unknown shard %q", shardID)
		}
		policy := replacementShardPolicy(shard, statuses, oldNode)
		if policy.HealthyRemaining < policy.RequiredHealthy {
			return replacementPolicyResult{}, fmt.Errorf(
				"replacement quorum policy failed for shard %q: healthy remaining replicas %d below required %d",
				shard.ID,
				policy.HealthyRemaining,
				policy.RequiredHealthy,
			)
		}
		out.Shards = append(out.Shards, policy)
	}
	return out, nil
}

func replacementShardPolicy(shard lsm.ShardStatus, statuses clusterStatusResult, oldNode string) replacementShardPolicyResult {
	policy := replacementShardPolicyResult{
		Shard:                 shard.ID,
		ReplicaCount:          len(shard.Replicas),
		RequiredHealthy:       len(shard.Replicas)/2 + 1,
		HealthyRemainingNodes: make([]string, 0, len(shard.Replicas)),
	}
	statusByNode := clusterStatusesByNode(statuses)
	for _, replica := range shard.Replicas {
		nodeID := strings.TrimSpace(replica.NodeID)
		if nodeID == "" || nodeID == oldNode {
			continue
		}
		if replica.Healthy && clusterNodeReplacementHealthy(statusByNode[nodeID]) {
			policy.HealthyRemaining++
			policy.HealthyRemainingNodes = append(policy.HealthyRemainingNodes, nodeID)
			continue
		}
		policy.UnavailableReplicaNodes = append(policy.UnavailableReplicaNodes, nodeID)
	}
	sort.Strings(policy.HealthyRemainingNodes)
	sort.Strings(policy.UnavailableReplicaNodes)
	return policy
}

func clusterStatusesByNode(statuses clusterStatusResult) map[string]clusterStatusNodeResult {
	out := make(map[string]clusterStatusNodeResult, len(statuses.Nodes))
	for _, node := range statuses.Nodes {
		if strings.TrimSpace(node.Node) != "" {
			out[strings.TrimSpace(node.Node)] = node
		}
		if node.Status != nil && strings.TrimSpace(node.Status.NodeID) != "" {
			out[strings.TrimSpace(node.Status.NodeID)] = node
		}
	}
	return out
}

func clusterNodeReplacementHealthy(node clusterStatusNodeResult) bool {
	if node.Error != "" || node.Status == nil {
		return false
	}
	health := strings.TrimSpace(node.Status.CommitLogRuntime.Health)
	return health == "ready" || health == "follower"
}

func planReplacementNode(endpoints map[string]string, opts replaceNodeOptions) (replacementPlanResult, error) {
	newNode := strings.TrimSpace(opts.NewNode)
	if newNode == "" {
		return replacementPlanResult{}, fmt.Errorf("new node required")
	}
	if len(endpoints) == 0 {
		return replacementPlanResult{}, fmt.Errorf("replacement-plan requires node endpoints")
	}
	statuses, err := readClusterStatuses(endpoints)
	if err != nil {
		return replacementPlanResult{}, err
	}
	oldNode := strings.TrimSpace(opts.OldNode)
	reason := ""
	if oldNode == "" {
		oldNode, reason, err = selectReplacementCandidate(statuses, newNode)
		if err != nil {
			return replacementPlanResult{}, err
		}
	} else {
		reason = replacementCandidateReason(statusNodeByName(statuses, oldNode))
		if reason == "" {
			reason = "operator-selected"
		}
	}
	plan, err := preflightReplaceClusterNode(endpoints, replaceNodeOptions{
		OldNode:         oldNode,
		NewNode:         newNode,
		ShardIDs:        opts.ShardIDs,
		OperationPrefix: opts.OperationPrefix,
		DryRun:          true,
	})
	if err != nil {
		return replacementPlanResult{}, err
	}
	dryRunArgs := replaceNodeCommandArgs(endpoints, replaceNodeOptions{
		OldNode:          oldNode,
		NewNode:          newNode,
		ShardIDs:         opts.ShardIDs,
		OperationPrefix:  opts.OperationPrefix,
		DryRun:           true,
		CommandEndpoints: opts.CommandEndpoints,
	})
	applyArgs := replaceNodeCommandArgs(endpoints, replaceNodeOptions{
		OldNode:          oldNode,
		NewNode:          newNode,
		ShardIDs:         opts.ShardIDs,
		OperationPrefix:  opts.OperationPrefix,
		CommandEndpoints: opts.CommandEndpoints,
	})
	return replacementPlanResult{
		OldNode:       oldNode,
		NewNode:       newNode,
		Reason:        reason,
		Shards:        plan.shardIDs,
		Preflight:     plan.preflight,
		DryRunCommand: dryRunArgs,
		ApplyCommand:  applyArgs,
		Statuses:      statuses,
	}, nil
}

func applyPlannedReplacement(endpoints map[string]string, opts replaceNodeOptions) (replacementApplyResult, error) {
	plan, err := planReplacementNode(endpoints, replaceNodeOptions{
		OldNode:          opts.OldNode,
		NewNode:          opts.NewNode,
		ShardIDs:         opts.ShardIDs,
		OperationPrefix:  opts.OperationPrefix,
		DryRun:           true,
		CommandEndpoints: opts.CommandEndpoints,
	})
	if err != nil {
		return replacementApplyResult{}, err
	}
	result, err := replaceClusterNode(endpoints, replaceNodeOptions{
		OldNode:                 plan.OldNode,
		NewNode:                 plan.NewNode,
		ShardIDs:                opts.ShardIDs,
		OperationPrefix:         opts.OperationPrefix,
		AllowUnavailableOldNode: true,
	})
	if err != nil {
		return replacementApplyResult{Plan: plan, Result: result}, err
	}
	return replacementApplyResult{
		Plan:   plan,
		Result: result,
	}, nil
}

func selectReplacementCandidate(statuses clusterStatusResult, newNode string) (string, string, error) {
	var candidates []clusterStatusNodeResult
	reasons := make(map[string]string)
	for _, node := range statuses.Nodes {
		nodeID := strings.TrimSpace(node.Node)
		if nodeID == "" || nodeID == strings.TrimSpace(newNode) {
			continue
		}
		reason := replacementCandidateReason(&node)
		if reason == "" {
			continue
		}
		candidates = append(candidates, node)
		reasons[nodeID] = reason
	}
	if len(candidates) == 0 {
		return "", "", fmt.Errorf("no unavailable replacement candidate found; pass --old-node to plan an operator-selected replacement")
	}
	if len(candidates) > 1 {
		nodes := make([]string, 0, len(candidates))
		for _, candidate := range candidates {
			nodes = append(nodes, candidate.Node)
		}
		sort.Strings(nodes)
		return "", "", fmt.Errorf("multiple unavailable replacement candidates found: %s; pass --old-node", strings.Join(nodes, ","))
	}
	nodeID := candidates[0].Node
	return nodeID, reasons[nodeID], nil
}

func statusNodeByName(statuses clusterStatusResult, nodeID string) *clusterStatusNodeResult {
	nodeID = strings.TrimSpace(nodeID)
	for i := range statuses.Nodes {
		if strings.TrimSpace(statuses.Nodes[i].Node) == nodeID {
			return &statuses.Nodes[i]
		}
		if statuses.Nodes[i].Status != nil && strings.TrimSpace(statuses.Nodes[i].Status.NodeID) == nodeID {
			return &statuses.Nodes[i]
		}
	}
	return nil
}

func replacementCandidateReason(node *clusterStatusNodeResult) string {
	if node == nil {
		return "status-missing"
	}
	if strings.TrimSpace(node.Error) != "" {
		return "status-error"
	}
	if node.Status == nil {
		return "status-missing"
	}
	if strings.TrimSpace(node.Status.CommitLogRuntime.Health) == "unavailable" {
		return "commit-log-unavailable"
	}
	return ""
}

func replaceNodeCommandArgs(endpoints map[string]string, opts replaceNodeOptions) []string {
	args := []string{
		"lsmctl",
		"replace-node",
		"--old-node",
		strings.TrimSpace(opts.OldNode),
		"--new-node",
		strings.TrimSpace(opts.NewNode),
	}
	if opts.DryRun {
		args = append(args, "--dry-run")
	}
	if prefix := strings.TrimSpace(opts.OperationPrefix); prefix != "" {
		args = append(args, "--operation-prefix", prefix)
	}
	args = appendReplacementCommandEndpointArgs(args, endpoints, opts.CommandEndpoints)
	for _, shardID := range opts.ShardIDs {
		shardID = strings.TrimSpace(shardID)
		if shardID != "" {
			args = append(args, "--shard", shardID)
		}
	}
	return args
}

func appendReplacementCommandEndpointArgs(args []string, endpoints map[string]string, source replacementCommandEndpointSource) []string {
	hasSource := false
	if configPath := strings.TrimSpace(source.ConfigPath); configPath != "" {
		args = append(args, "--config", configPath)
		hasSource = true
	}
	if addr := strings.TrimSpace(source.Addr); addr != "" {
		args = append(args, "--addr", addr)
		hasSource = true
	}
	for _, nodeID := range sortedEndpointNodes(source.Overrides) {
		args = append(args, "--node-endpoint", nodeID+"="+source.Overrides[nodeID])
		hasSource = true
	}
	if hasSource {
		return args
	}
	for _, nodeID := range sortedEndpointNodes(endpoints) {
		args = append(args, "--node-endpoint", nodeID+"="+endpoints[nodeID])
	}
	return args
}

func replacementCommandEndpointSourceFromFlags(configPath string, addr string, overrides nodeEndpointFlags) replacementCommandEndpointSource {
	return replacementCommandEndpointSource{
		ConfigPath: strings.TrimSpace(configPath),
		Addr:       strings.TrimSpace(addr),
		Overrides:  cloneNodeEndpointFlags(overrides),
	}
}

func cloneNodeEndpointFlags(in nodeEndpointFlags) nodeEndpointFlags {
	if len(in) == 0 {
		return nil
	}
	out := make(nodeEndpointFlags, len(in))
	for nodeID, endpoint := range in {
		out[nodeID] = endpoint
	}
	return out
}

func readShards(endpoint string) ([]lsm.ShardStatus, error) {
	var shards []lsm.ShardStatus
	if err := getJSON(endpoint+"/cluster/shards", &shards); err != nil {
		return nil, err
	}
	return shards, nil
}

func replacementShardIDs(shards []lsm.ShardStatus, oldNode string, requested []string) ([]string, error) {
	if len(requested) > 0 {
		known := make(map[string]lsm.ShardStatus, len(shards))
		for _, shard := range shards {
			known[shard.ID] = shard
		}
		out := make([]string, 0, len(requested))
		seen := make(map[string]struct{}, len(requested))
		for _, shardID := range requested {
			shardID = strings.TrimSpace(shardID)
			if shardID == "" {
				continue
			}
			if _, ok := seen[shardID]; ok {
				continue
			}
			shard, ok := known[shardID]
			if !ok {
				return nil, fmt.Errorf("unknown shard %q", shardID)
			}
			if !hasShardReplica(shard, oldNode) {
				return nil, fmt.Errorf("old node %q is not a replica of shard %q", oldNode, shardID)
			}
			seen[shardID] = struct{}{}
			out = append(out, shardID)
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("no replacement shards selected")
		}
		sort.Strings(out)
		return out, nil
	}
	out := make([]string, 0)
	for _, shard := range shards {
		if hasShardReplica(shard, oldNode) {
			out = append(out, shard.ID)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("old node %q is not a replica of any shard", oldNode)
	}
	sort.Strings(out)
	return out, nil
}

func writeDrainNodeResult(w io.Writer, result drainNodeResult) {
	fmt.Fprintf(w, "target=%s submitted_to=%s endpoint=%s\n", result.Target, result.SubmittedTo, result.Endpoint)
	for _, shard := range result.Shards {
		fmt.Fprintf(w, "shard=%s leader=%s\n", shard.ID, shard.Leader)
	}
	writeClusterStatuses(w, result.Statuses)
}

func writeReplaceNodeResult(w io.Writer, result replaceNodeResult) {
	fmt.Fprintf(w, "old_node=%s new_node=%s shards=%s\n", result.OldNode, result.NewNode, strings.Join(result.Shards, ","))
	fmt.Fprintf(
		w,
		"preflight=ok dry_run=%v write_leader=%s old_endpoint=%s new_endpoint=%s\n",
		result.DryRun,
		result.Preflight.WriteLeader,
		result.Preflight.OldEndpoint,
		result.Preflight.NewEndpoint,
	)
	writeReplacementPolicy(w, result.Preflight.Policy)
	for _, step := range result.Steps {
		if step.Shard != "" {
			fmt.Fprintf(w, "step=%s shard=%s node=%s submitted_to=%s\n", step.Operation, step.Shard, step.Node, step.SubmittedTo)
			continue
		}
		fmt.Fprintf(w, "step=%s node=%s submitted_to=%s\n", step.Operation, step.Node, step.SubmittedTo)
	}
	if result.Drain.Target != "" {
		fmt.Fprintf(w, "step=drain-node node=%s submitted_to=%s\n", result.Drain.Target, result.Drain.SubmittedTo)
	}
	writeClusterStatuses(w, result.Statuses)
}

func writeReplacementPlan(w io.Writer, result replacementPlanResult) {
	fmt.Fprintf(
		w,
		"old_node=%s new_node=%s reason=%s shards=%s\n",
		result.OldNode,
		result.NewNode,
		result.Reason,
		strings.Join(result.Shards, ","),
	)
	fmt.Fprintf(
		w,
		"preflight=ok write_leader=%s old_endpoint=%s new_endpoint=%s\n",
		result.Preflight.WriteLeader,
		result.Preflight.OldEndpoint,
		result.Preflight.NewEndpoint,
	)
	writeReplacementPolicy(w, result.Preflight.Policy)
	fmt.Fprintf(w, "dry_run_command=%s\n", strings.Join(result.DryRunCommand, " "))
	fmt.Fprintf(w, "apply_command=%s\n", strings.Join(result.ApplyCommand, " "))
	writeClusterStatuses(w, result.Statuses)
}

func writeReplacementPolicy(w io.Writer, policy replacementPolicyResult) {
	for _, shard := range policy.Shards {
		fmt.Fprintf(
			w,
			"policy=quorum shard=%s healthy_remaining=%d required=%d healthy_nodes=%s unavailable_nodes=%s\n",
			shard.Shard,
			shard.HealthyRemaining,
			shard.RequiredHealthy,
			strings.Join(shard.HealthyRemainingNodes, ","),
			strings.Join(shard.UnavailableReplicaNodes, ","),
		)
	}
}

func writeReplacementApply(w io.Writer, result replacementApplyResult) {
	fmt.Fprintf(
		w,
		"planned_old_node=%s planned_new_node=%s reason=%s\n",
		result.Plan.OldNode,
		result.Plan.NewNode,
		result.Plan.Reason,
	)
	writeReplaceNodeResult(w, result.Result)
}

func writeMembershipActionResult(w io.Writer, result membershipActionResult) {
	if result.Shard != "" {
		fmt.Fprintf(w, "operation=%s shard=%s node=%s submitted_to=%s endpoint=%s\n", result.Operation, result.Shard, result.Node, result.SubmittedTo, result.Endpoint)
	} else {
		fmt.Fprintf(w, "operation=%s node=%s submitted_to=%s endpoint=%s\n", result.Operation, result.Node, result.SubmittedTo, result.Endpoint)
	}
	for _, shard := range result.Shards {
		if result.Shard != "" && shard.ID != result.Shard {
			continue
		}
		replicas := make([]string, 0, len(shard.Replicas))
		for _, replica := range shard.Replicas {
			replicas = append(replicas, replica.NodeID+":"+replica.Role)
		}
		sort.Strings(replicas)
		fmt.Fprintf(w, "shard=%s leader=%s replicas=%s\n", shard.ID, shard.Leader, strings.Join(replicas, ","))
	}
	writeClusterStatuses(w, result.Statuses)
}

func alignShardLeader(endpoint string, key []byte, target string) error {
	shard, ok, err := shardForKey(endpoint, key)
	if err != nil || !ok {
		if err != nil {
			return err
		}
		return fmt.Errorf("route not found for key")
	}
	if shard.Leader == target {
		return nil
	}
	return postControlAction(endpoint+"/cluster/shards/"+url.PathEscape(shard.ID)+"/transfer-leader", targetRequest{
		Target: target,
	})
}

func shardForKey(endpoint string, key []byte) (lsm.ShardStatus, bool, error) {
	var shards []lsm.ShardStatus
	if err := getJSON(endpoint+"/cluster/shards", &shards); err != nil {
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

func postControlAction(rawURL string, payload any) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(payload); err != nil {
		return err
	}
	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(normalizeHTTPBaseURL(rawURL), "application/json", &buf)
	if err != nil {
		return err
	}
	var out map[string]any
	return decodeHTTPJSON(resp, &out)
}

func sortedEndpointNodes(endpoints map[string]string) []string {
	nodes := make([]string, 0, len(endpoints))
	for nodeID := range endpoints {
		nodes = append(nodes, nodeID)
	}
	sort.Strings(nodes)
	return nodes
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
	return server.NormalizeHTTPBaseURL(raw)
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
	if err := serverconfig.Validate(cfg); err != nil {
		log.Fatalf("invalid config: %v", err)
	}
	return cfg
}

func clusterWriteOptionsFromConfig(
	cfg serverconfig.Config,
	addr string,
	clusterMode bool,
	overrides nodeEndpointFlags,
) (clusterWriteOptions, error) {
	enabled := clusterMode || len(overrides) > 0
	if !enabled {
		return clusterWriteOptions{}, nil
	}
	endpoints, err := clusterNodeEndpointsFromConfig(cfg, addr, overrides)
	if err != nil {
		return clusterWriteOptions{}, err
	}
	if len(endpoints) == 0 {
		return clusterWriteOptions{}, fmt.Errorf("--cluster requires raft.peer_urls, raft.peer_url_file, --addr, or --node-endpoint")
	}
	return clusterWriteOptions{
		Enabled:       true,
		NodeEndpoints: endpoints,
	}, nil
}

func clusterReadOptionsFromConfig(
	cfg serverconfig.Config,
	addr string,
	clusterMode bool,
	overrides nodeEndpointFlags,
) (clusterReadOptions, error) {
	enabled := clusterMode || len(overrides) > 0
	if !enabled {
		return clusterReadOptions{}, nil
	}
	endpoints, err := clusterNodeEndpointsFromConfig(cfg, addr, overrides)
	if err != nil {
		return clusterReadOptions{}, err
	}
	if len(endpoints) == 0 {
		return clusterReadOptions{}, fmt.Errorf("--cluster requires raft.peer_urls, raft.peer_url_file, --addr, or --node-endpoint")
	}
	return clusterReadOptions{
		Enabled:       true,
		NodeEndpoints: endpoints,
	}, nil
}

func clusterNodeEndpointsFromConfig(
	cfg serverconfig.Config,
	addr string,
	overrides nodeEndpointFlags,
) (map[string]string, error) {
	resolver := server.NewNodeEndpointConfigResolverFromConfig(cfg, addr, overrides)
	return resolver.ResolveNodeEndpoints(context.Background())
}

func gatewayNodeEndpointResolverFromConfig(
	cfg serverconfig.Config,
	bootstrapURL string,
	endpointFile string,
	overrides nodeEndpointFlags,
) (server.NodeEndpointResolver, error) {
	endpointFile = strings.TrimSpace(endpointFile)
	if endpointFile == "" {
		endpointFile = strings.TrimSpace(cfg.Raft.PeerURLFile)
	}
	fallbackCfg := cfg
	fallbackCfg.Raft.PeerURLFile = ""
	fallback, err := clusterNodeEndpointsFromConfig(fallbackCfg, bootstrapURL, overrides)
	if err != nil {
		return nil, err
	}
	if endpointFile != "" {
		return server.NewNodeEndpointFileResolver(server.NodeEndpointFileResolverOptions{
			Path:                  endpointFile,
			FallbackNodeEndpoints: fallback,
		})
	}
	return server.NewStaticNodeEndpointResolver(fallback)
}

func toRaftOptions(cfg serverconfig.RaftConfig) *lsm.RaftOptions {
	if cfg.Replicas == 0 && cfg.ElectionTimeout == 0 && cfg.HeartbeatInterval == 0 && len(cfg.Peers) == 0 && !cfg.Join {
		return nil
	}
	peers := append([]string(nil), cfg.Peers...)
	return &lsm.RaftOptions{
		Replicas:          cfg.Replicas,
		ElectionTimeout:   cfg.ElectionTimeout,
		HeartbeatInterval: cfg.HeartbeatInterval,
		Peers:             peers,
		Join:              cfg.Join,
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
	peerURLs := toRaftPeerURLMap(raftCfg.PeerURLs)
	for id, url := range toRaftPeerURLMap(raftCfg.JoinPeerURLs) {
		peerURLs[id] = url
	}
	if opts.Provider == lsm.CommitLogProviderEtcdRaft && (len(peerURLs) > 0 || strings.TrimSpace(raftCfg.PeerURLFile) != "") {
		resolver, err := raftPeerResolverFromConfig(raftCfg, peerURLs)
		if err != nil {
			return nil, err
		}
		transport, err := server.NewRaftHTTPTransport(server.RaftHTTPTransportOptions{
			PeerResolver: resolver,
		})
		if err != nil {
			return nil, err
		}
		opts.Transport = transport
	}
	return opts, nil
}

func raftPeerResolverFromConfig(raftCfg serverconfig.RaftConfig, peerURLs map[uint64]string) (server.RaftPeerResolver, error) {
	peerURLFile := strings.TrimSpace(raftCfg.PeerURLFile)
	if peerURLFile != "" {
		return server.NewRaftPeerURLFileResolver(server.RaftPeerURLFileResolverOptions{
			Path:             peerURLFile,
			FallbackPeerURLs: peerURLs,
		})
	}
	return server.NewStaticRaftPeerResolver(peerURLs)
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
