package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	path := writeConfig(t, `
data_dir: "./data"
addr: "127.0.0.1:9090"
read_timeout: "2s"
write_timeout: "3s"
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.DataDir != "./data" {
		t.Fatalf("expected data dir, got %q", cfg.DataDir)
	}
	if cfg.Addr != "127.0.0.1:9090" {
		t.Fatalf("expected addr, got %q", cfg.Addr)
	}
	if cfg.ReadTimeout != 2*time.Second {
		t.Fatalf("expected read timeout, got %v", cfg.ReadTimeout)
	}
	if cfg.WriteTimeout != 3*time.Second {
		t.Fatalf("expected write timeout, got %v", cfg.WriteTimeout)
	}
}

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := t.TempDir() + "/cfg.yaml"
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
