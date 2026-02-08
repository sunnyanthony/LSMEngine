//go:build !linux

package iofs

import (
	"fmt"
	"runtime"
)

func newIOUringFS(base FS, cfg AsyncConfig) (FS, error) {
	return nil, fmt.Errorf("io_uring unsupported on %s", runtime.GOOS)
}
