//go:build test

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	serverconfig "lsmengine/pkg/lsm/server/config"
)

func TestServerConfigExampleLoads(t *testing.T) {
	root := repoRoot(t)
	path := filepath.Join(root, "examples", "server-config.yaml")
	cfg, err := serverconfig.Load(path)
	if err != nil {
		t.Fatalf("load example: %v", err)
	}
	if cfg.DataDir == "" {
		t.Fatalf("expected data_dir in example config")
	}
	if cfg.Addr == "" {
		t.Fatalf("expected addr in example config")
	}
	if len(cfg.Raft.Peers) == 0 {
		t.Fatalf("expected raft peers in example config")
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("repo root not found")
		}
		dir = parent
	}
}
