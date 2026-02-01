package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"testing"
	"time"
)

func TestUDSWriteEventHandlerWritesJSON(t *testing.T) {
	path := fmt.Sprintf("%s/lsm-uds-%d.sock", os.TempDir(), time.Now().UnixNano())
	t.Cleanup(func() {
		_ = os.Remove(path)
	})
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	got := make(chan WebhookEvent, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return
		}
		var event WebhookEvent
		if err := json.Unmarshal(line, &event); err != nil {
			return
		}
		got <- event
	}()

	handler := NewUDSWriteEventHandler(path, 2*time.Second, nil)
	handler.HandleWrite(context.Background(), WriteEvent{
		Op:     "put",
		Key:    []byte("k"),
		Status: "committed",
		Seq:    3,
	})

	select {
	case event := <-got:
		if event.Op != "put" || event.Status != "committed" || event.Seq != 3 {
			t.Fatalf("unexpected event: %+v", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for uds event")
	}
}
