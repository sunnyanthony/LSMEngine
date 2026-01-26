package logging

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewDefaultLoggerStdout(t *testing.T) {
	logger, closer, err := NewDefaultLogger(t.TempDir(), "")
	if err != nil {
		t.Fatalf("new default logger: %v", err)
	}
	if logger == nil {
		t.Fatalf("expected logger")
	}
	if closer != nil {
		t.Fatalf("expected nil closer for stdout logger")
	}
}

func TestNewDefaultLoggerCreatesFile(t *testing.T) {
	dataDir := t.TempDir()
	logger, closer, err := NewDefaultLogger(dataDir, "logs")
	if err != nil {
		t.Fatalf("new default logger: %v", err)
	}
	if logger == nil {
		t.Fatalf("expected logger")
	}
	if closer == nil {
		t.Fatalf("expected closer for file logger")
	}

	logger.Printf("hello %s", "world")
	if err := closer.Close(); err != nil {
		t.Fatalf("close logger: %v", err)
	}

	path := filepath.Join(dataDir, "logs", "lsm.log")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat log file: %v", err)
	}
	if info.Size() == 0 {
		t.Fatalf("expected log file to have content")
	}
}
