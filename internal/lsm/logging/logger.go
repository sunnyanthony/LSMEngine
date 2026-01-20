// Logger interface and default logger helpers.

package logging

import (
	"io"
	"log"
	"os"
	"path/filepath"
)

// Logger is a minimal logging contract.
type Logger interface {
	Printf(format string, v ...any)
}

// NewDefaultLogger builds a logger. If logDir is empty, logs go to stdout.
// If logDir is set, it is created (relative to dataDir when not absolute) and
// logs are written to lsm.log inside it. The returned closer should be closed
// on shutdown when non-nil.
func NewDefaultLogger(dataDir, logDir string) (Logger, io.Closer, error) {
	if logDir == "" {
		return log.New(os.Stdout, "lsm: ", log.LstdFlags), nil, nil
	}
	dir := logDir
	if !filepath.IsAbs(dir) {
		dir = filepath.Join(dataDir, logDir)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, err
	}
	path := filepath.Join(dir, "lsm.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, err
	}
	return log.New(f, "lsm: ", log.LstdFlags), f, nil
}
