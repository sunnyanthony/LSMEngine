// Simple UDS sidecar that forwards write events to a webhook.

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"time"
)

type webhookEvent struct {
	Op     string `json:"op"`
	Key    string `json:"key"`
	Status string `json:"status"`
	Seq    uint64 `json:"seq"`
	Error  string `json:"error,omitempty"`
}

func main() {
	socketPath := getenv("LSM_EVENTS_SOCKET", "/var/run/lsm/events.sock")
	webhookURL := getenv("LSM_WEBHOOK_URL", "")
	timeout := 2 * time.Second

	_ = os.Remove(socketPath)
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("listening on %s", socketPath)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go handleConn(conn, webhookURL, timeout)
	}
}

func handleConn(conn net.Conn, webhookURL string, timeout time.Duration) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return
		}
		var event webhookEvent
		if err := json.Unmarshal(bytes.TrimSpace(line), &event); err != nil {
			log.Printf("decode: %v", err)
			continue
		}
		if webhookURL == "" {
			log.Printf("event: %+v", event)
			continue
		}
		if err := sendWebhook(webhookURL, event, timeout); err != nil {
			log.Printf("webhook: %v", err)
		}
	}
}

func sendWebhook(url string, event webhookEvent, timeout time.Duration) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

func getenv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
