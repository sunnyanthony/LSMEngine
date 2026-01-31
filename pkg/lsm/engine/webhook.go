// Webhook sink for async write notifications.

package engine

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"time"
)

// WebhookResolver chooses a destination URL for a write event.
type WebhookResolver func(event WriteEvent) string

// WebhookEvent describes the payload sent to webhook endpoints.
type WebhookEvent struct {
	Op     string `json:"op"`
	Key    string `json:"key"`
	Status string `json:"status"`
	Seq    uint64 `json:"seq"`
	Error  string `json:"error,omitempty"`
}

type webhookSink struct {
	resolve  WebhookResolver
	client   *http.Client
	logger   func(string, ...any)
	fallback string
}

func newWebhookSink(url string, timeout time.Duration, resolver WebhookResolver, logger func(string, ...any)) *webhookSink {
	return &webhookSink{
		resolve:  resolver,
		client:   &http.Client{Timeout: timeout},
		logger:   logger,
		fallback: url,
	}
}

func (s *webhookSink) HandleWrite(ctx context.Context, event WriteEvent) {
	if s == nil {
		return
	}
	target := s.fallback
	if s.resolve != nil {
		target = s.resolve(event)
	}
	if target == "" {
		return
	}
	payload, err := json.Marshal(webhookPayload(event))
	if err != nil {
		if s.logger != nil {
			s.logger("webhook marshal: %v", err)
		}
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(payload))
	if err != nil {
		if s.logger != nil {
			s.logger("webhook request: %v", err)
		}
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		if s.logger != nil {
			s.logger("webhook send: %v", err)
		}
		return
	}
	_ = resp.Body.Close()
}

func webhookPayload(event WriteEvent) WebhookEvent {
	payload := WebhookEvent{
		Op:     event.Op,
		Key:    encodeWebhookKey(event.Key),
		Status: event.Status,
		Seq:    event.Seq,
	}
	if event.Err != nil {
		payload.Error = event.Err.Error()
	}
	return payload
}

func encodeWebhookKey(key []byte) string {
	if len(key) == 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString(key)
}
