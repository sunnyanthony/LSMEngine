// Command-line helper for stats, health, and server mode.

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
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
	fmt.Fprintln(os.Stderr, "usage: lsmctl <serve|stats|health> [options]")
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

	store, err := lsm.New(lsm.Options{
		DataDir:            *dataDir,
		NodeID:             cfg.NodeID,
		ClusterID:          cfg.ClusterID,
		StorageMode:        cfg.StorageMode,
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
		Addr:         *addr,
		Handler:      server.NewHandler(store),
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
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("http %d: %s", resp.StatusCode, string(body))
	}
	return json.NewDecoder(resp.Body).Decode(out)
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
	if cfg.Replicas == 0 && cfg.ElectionTimeout == 0 && cfg.HeartbeatInterval == 0 {
		return nil
	}
	return &lsm.RaftOptions{
		Replicas:          cfg.Replicas,
		ElectionTimeout:   cfg.ElectionTimeout,
		HeartbeatInterval: cfg.HeartbeatInterval,
	}
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
