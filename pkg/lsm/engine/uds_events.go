// Unix domain socket write-event handler.

package engine

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"time"
)

// UDSWriteEventHandler sends write events to a Unix domain socket.
type UDSWriteEventHandler struct {
	Path    string
	Timeout time.Duration

	mu     sync.Mutex
	conn   net.Conn
	logger func(string, ...any)
}

// NewUDSWriteEventHandler builds a handler that emits newline-delimited JSON.
func NewUDSWriteEventHandler(path string, timeout time.Duration, logger func(string, ...any)) *UDSWriteEventHandler {
	return &UDSWriteEventHandler{
		Path:    path,
		Timeout: timeout,
		logger:  logger,
	}
}

// HandleWrite implements WriteEventSink.
func (h *UDSWriteEventHandler) HandleWrite(ctx context.Context, event WriteEvent) {
	if h == nil || h.Path == "" {
		return
	}
	payload, err := json.Marshal(webhookPayload(event))
	if err != nil {
		if h.logger != nil {
			h.logger("uds marshal: %v", err)
		}
		return
	}
	payload = append(payload, '\n')
	if err := h.write(ctx, payload); err != nil && h.logger != nil {
		h.logger("uds write: %v", err)
	}
}

func (h *UDSWriteEventHandler) write(ctx context.Context, payload []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	conn := h.conn
	if conn == nil {
		dialer := net.Dialer{}
		if h.Timeout > 0 {
			dialer.Timeout = h.Timeout
		}
		c, err := dialer.DialContext(ctx, "unix", h.Path)
		if err != nil {
			return err
		}
		conn = c
		h.conn = c
	}
	if h.Timeout > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(h.Timeout))
	}
	if _, err := conn.Write(payload); err != nil {
		_ = conn.Close()
		h.conn = nil
		return err
	}
	return nil
}
