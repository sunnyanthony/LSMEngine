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
	DataDir                 string          `yaml:"data_dir"`
	NodeID                  string          `yaml:"node_id"`
	ClusterID               string          `yaml:"cluster_id"`
	StorageMode             string          `yaml:"storage_mode"`
	ControlStatePath        string          `yaml:"control_state_path"`
	CommitLog               CommitLogConfig `yaml:"commitlog"`
	Raft                    RaftConfig      `yaml:"raft"`
	Shards                  []ShardConfig   `yaml:"shards"`
	Addr                    string          `yaml:"addr"`
	ReadTimeout             time.Duration   `yaml:"read_timeout"`
	WriteTimeout            time.Duration   `yaml:"write_timeout"`
	WriteConsistencyDefault string          `yaml:"write_consistency_default"`
	IOBackend               string          `yaml:"io_backend"`
	IOBackendStrict         bool            `yaml:"io_backend_strict"`
	IOAsyncMaxInFlight      int             `yaml:"io_async_max_in_flight"`
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
