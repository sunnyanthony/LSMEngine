package transport

import (
	"context"
	"errors"

	"lsmengine/pkg/lsm/types"
)

// ErrClosed is returned when the transport is closed.
var ErrClosed = errors.New("transport closed")

// Message carries replicated entries between nodes.
type Message struct {
	Source  string
	Term    uint64
	Entries []types.Entry
}

// Transport abstracts replication delivery (gRPC, NATS, Kafka, etc.).
type Transport interface {
	Publish(ctx context.Context, msg Message) error
	Subscribe(ctx context.Context, handler func(Message) error) error
	Close() error
}
