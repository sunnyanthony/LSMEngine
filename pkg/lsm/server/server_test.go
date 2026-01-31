package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"lsmengine/pkg/lsm"
)

type stubProvider struct{}

func (stubProvider) Stats() lsm.Stats {
	return lsm.Stats{MemtableBytes: 1, MemtableEntries: 2}
}

func (stubProvider) Health() lsm.Health {
	return lsm.Health{Ready: true, Reason: "ok"}
}

func TestHandlerHealth(t *testing.T) {
	handler := NewHandler(stubProvider{})
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	var health lsm.Health
	if err := json.NewDecoder(rec.Body).Decode(&health); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !health.Ready || health.Reason != "ok" {
		t.Fatalf("unexpected health: %+v", health)
	}
}

func TestHandlerStats(t *testing.T) {
	handler := NewHandler(stubProvider{})
	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	var stats lsm.Stats
	if err := json.NewDecoder(rec.Body).Decode(&stats); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if stats.MemtableBytes != 1 || stats.MemtableEntries != 2 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}
