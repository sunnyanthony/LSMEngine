// Package config loads server-mode configuration.

package config

import (
	"fmt"
	"os"
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
	Provider string `yaml:"provider"`
}

// RaftConfig captures control-plane raft settings.
type RaftConfig struct {
	Replicas          int           `yaml:"replicas"`
	ElectionTimeout   time.Duration `yaml:"election_timeout"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
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
