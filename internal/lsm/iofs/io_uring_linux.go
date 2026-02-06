//go:build linux

package iofs

import "fmt"

func newIOUringFS(base FS, cfg AsyncConfig) (FS, error) {
	return nil, fmt.Errorf("io_uring backend not implemented")
}
