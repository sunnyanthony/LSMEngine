// Backend selection helpers for IOFS.

package iofs

import "fmt"

// Backend describes a selectable IO backend.
type Backend string

const (
	BackendOS      Backend = "os"
	BackendAsync   Backend = "async"
	BackendIOUring Backend = "io_uring"
)

// SelectFS returns a filesystem backend based on name and config.
func SelectFS(backend Backend, base FS, cfg AsyncConfig, strict bool) (FS, error) {
	if base == nil {
		base = OSFS{}
	}
	switch backend {
	case "", BackendOS:
		return base, nil
	case BackendAsync:
		return NewAsyncFS(base, cfg), nil
	case BackendIOUring:
		fs, err := newIOUringFS(base, cfg)
		if err == nil {
			return fs, nil
		}
		if strict {
			return nil, err
		}
		return NewAsyncFS(base, cfg), nil
	default:
		return nil, fmt.Errorf("unknown io backend %q", backend)
	}
}
