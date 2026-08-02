// Package config loads server-mode configuration.

package config

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config captures server-mode defaults.
type Config struct {
	DataDir                           string          `yaml:"data_dir"`
	NodeID                            string          `yaml:"node_id"`
	ClusterID                         string          `yaml:"cluster_id"`
	StorageMode                       string          `yaml:"storage_mode"`
	ControlStatePath                  string          `yaml:"control_state_path"`
	CommitLog                         CommitLogConfig `yaml:"commitlog"`
	Raft                              RaftConfig      `yaml:"raft"`
	Shards                            []ShardConfig   `yaml:"shards"`
	Addr                              string          `yaml:"addr"`
	ReadTimeout                       time.Duration   `yaml:"read_timeout"`
	WriteTimeout                      time.Duration   `yaml:"write_timeout"`
	WriteConsistencyDefault           string          `yaml:"write_consistency_default"`
	GatewayReadMode                   string          `yaml:"gateway_read_mode"`
	GatewayReadBalancePolicy          string          `yaml:"gateway_read_balance_policy"`
	GatewayMaxReadApplyLag            *int64          `yaml:"gateway_max_read_apply_lag"`
	GatewayMaxWriteAttempts           int             `yaml:"gateway_max_write_attempts"`
	GatewayWriteRetryBackoff          time.Duration   `yaml:"gateway_write_retry_backoff"`
	GatewayEndpointFailureCooldown    time.Duration   `yaml:"gateway_endpoint_failure_cooldown"`
	GatewayEndpointFile               string          `yaml:"gateway_endpoint_file"`
	GatewayEndpointDNSName            string          `yaml:"gateway_endpoint_dns_name"`
	GatewayEndpointDNSService         string          `yaml:"gateway_endpoint_dns_service"`
	GatewayEndpointDNSProto           string          `yaml:"gateway_endpoint_dns_proto"`
	GatewayEndpointDNSScheme          string          `yaml:"gateway_endpoint_dns_scheme"`
	GatewayReadyMinReachable          int             `yaml:"gateway_ready_min_reachable"`
	GatewayReadyMaxReadApplyLag       *int64          `yaml:"gateway_ready_max_read_apply_lag"`
	GatewayReadyMinReadReady          int             `yaml:"gateway_ready_min_read_ready"`
	MemtableLimit                     int             `yaml:"memtable_limit"`
	WALMaxSegmentBytes                uint64          `yaml:"wal_max_segment_bytes"`
	WALRetainArchivedSegments         int             `yaml:"wal_retain_archived_segments"`
	WALReadyMaxCheckpointLag          uint64          `yaml:"wal_ready_max_checkpoint_lag"`
	WALBackpressureMaxCheckpointLag   uint64          `yaml:"wal_backpressure_max_checkpoint_lag"`
	FlushQueueSize                    int             `yaml:"flush_queue_size"`
	FlushBackpressureQueueThreshold   int             `yaml:"flush_backpressure_queue_threshold"`
	CompactionL0Threshold             int             `yaml:"compaction_l0_threshold"`
	CompactionCheckInterval           time.Duration   `yaml:"compaction_check_interval"`
	CompactionAdaptiveCheck           bool            `yaml:"compaction_adaptive_check"`
	CompactionBackpressureL0Threshold int             `yaml:"compaction_backpressure_l0_threshold"`
	IOBackend                         string          `yaml:"io_backend"`
	IOBackendStrict                   bool            `yaml:"io_backend_strict"`
	IOAsyncMaxInFlight                int             `yaml:"io_async_max_in_flight"`
}

// CommitLogConfig captures commit-log provider selection.
type CommitLogConfig struct {
	Provider       string                  `yaml:"provider"`
	SnapshotPolicy CommitLogSnapshotPolicy `yaml:"snapshot_policy"`
}

// CommitLogSnapshotPolicy controls provider-owned raft log snapshots.
type CommitLogSnapshotPolicy struct {
	AppliedEntries uint64 `yaml:"applied_entries"`
	RetainEntries  uint64 `yaml:"retain_entries"`
}

// RaftConfig captures control-plane raft settings.
type RaftConfig struct {
	Replicas          int               `yaml:"replicas"`
	ElectionTimeout   time.Duration     `yaml:"election_timeout"`
	HeartbeatInterval time.Duration     `yaml:"heartbeat_interval"`
	Peers             []string          `yaml:"peers"`
	PeerURLs          map[string]string `yaml:"peer_urls"`
	JoinPeerURLs      map[string]string `yaml:"join_peer_urls"`
	PeerURLFile       string            `yaml:"peer_url_file"`
	Join              bool              `yaml:"join"`
}

// ShardConfig describes a fixed shard range in server YAML.
type ShardConfig struct {
	ID       string   `yaml:"id"`
	StartKey string   `yaml:"start_key"`
	EndKey   string   `yaml:"end_key"`
	Replicas []string `yaml:"replicas"`
	Leader   string   `yaml:"leader"`
}

// Load reads a YAML config file from disk.
func Load(path string) (Config, error) {
	if path == "" {
		return Config{}, fmt.Errorf("config path required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// LoadPeerURLFile reads a YAML/JSON node-name to absolute URL map.
func LoadPeerURLFile(path string) (map[string]string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("raft peer_url_file required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var raw map[string]string
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	out := make(map[string]string, len(raw))
	for nodeID, endpoint := range raw {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" {
			return nil, fmt.Errorf("raft peer_url_file contains empty node name")
		}
		endpoint = strings.TrimSpace(endpoint)
		if err := validateAbsoluteURL(endpoint); err != nil {
			return nil, fmt.Errorf("raft peer_url_file[%q] must be an absolute URL", nodeID)
		}
		out[nodeID] = endpoint
	}
	return out, nil
}

// Validate checks cross-field server config invariants that YAML decoding cannot
// express.
func Validate(cfg Config) error {
	provider := strings.TrimSpace(cfg.CommitLog.Provider)
	if provider != "etcd-raft" {
		return nil
	}
	peers, err := normalizedPeers(cfg.Raft.Peers)
	if err != nil {
		return err
	}
	if len(peers) <= 1 {
		return nil
	}
	nodeID := strings.TrimSpace(cfg.NodeID)
	if nodeID == "" {
		nodeID = "node-0"
	}
	if _, ok := peers[nodeID]; !ok {
		return fmt.Errorf("raft peers must include local node %q", nodeID)
	}
	peerURLFile := strings.TrimSpace(cfg.Raft.PeerURLFile)
	if peerURLFile != "" && !filepath.IsAbs(peerURLFile) {
		return fmt.Errorf("raft peer_url_file must be an absolute path")
	}
	if peerURLFile == "" {
		for peer := range peers {
			rawURL := strings.TrimSpace(cfg.Raft.PeerURLs[peer])
			if rawURL == "" {
				return fmt.Errorf("raft peer_urls missing peer %q", peer)
			}
			if err := validateAbsoluteURL(rawURL); err != nil {
				return fmt.Errorf("raft peer_urls[%q] must be an absolute URL", peer)
			}
		}
	}
	for peer, rawURL := range cfg.Raft.PeerURLs {
		if strings.TrimSpace(peer) == "" {
			return fmt.Errorf("raft peer_urls contains empty peer name")
		}
		if _, ok := peers[peer]; !ok {
			return fmt.Errorf("raft peer_urls contains unknown peer %q", peer)
		}
		if err := validateAbsoluteURL(rawURL); err != nil {
			return fmt.Errorf("raft peer_urls[%q] must be an absolute URL", peer)
		}
	}
	for peer, rawURL := range cfg.Raft.JoinPeerURLs {
		if strings.TrimSpace(peer) == "" {
			return fmt.Errorf("raft join_peer_urls contains empty peer name")
		}
		if _, ok := peers[peer]; ok {
			return fmt.Errorf("raft join_peer_urls contains existing peer %q", peer)
		}
		if err := validateAbsoluteURL(rawURL); err != nil {
			return fmt.Errorf("raft join_peer_urls[%q] must be an absolute URL", peer)
		}
	}
	return nil
}

func validateAbsoluteURL(rawURL string) error {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("absolute URL required")
	}
	return nil
}

func normalizedPeers(in []string) (map[string]struct{}, error) {
	peers := make(map[string]struct{}, len(in))
	for _, raw := range in {
		peer := strings.TrimSpace(raw)
		if peer == "" {
			return nil, fmt.Errorf("raft peers contains empty peer")
		}
		if _, exists := peers[peer]; exists {
			return nil, fmt.Errorf("raft peers contains duplicate peer %q", peer)
		}
		peers[peer] = struct{}{}
	}
	return peers, nil
}
