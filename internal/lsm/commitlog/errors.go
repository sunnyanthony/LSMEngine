package commitlog

import "errors"

var (
	ErrNotLeader   = errors.New("commitlog: not leader")
	ErrUnavailable = errors.New("commitlog: unavailable")
)
