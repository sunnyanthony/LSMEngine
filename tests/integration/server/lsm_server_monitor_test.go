//go:build test

package integration_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"lsmengine/pkg/lsm"
	"lsmengine/pkg/lsm/server"
)

func TestServerHealthAndStats(t *testing.T) {
	store, err := lsm.New(lsm.Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}()

	if err := store.Put([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("put: %v", err)
	}

	handler := server.NewHandler(store)

	health := readJSON[lsm.Health](t, handler, "/healthz")
	if !health.Ready || health.Reason != "ok" {
		t.Fatalf("expected ok health, got %+v", health)
	}

	stats := readJSON[lsm.Stats](t, handler, "/stats")
	if stats.MemtableBytes == 0 {
		t.Fatalf("expected memtable bytes > 0")
	}
	if stats.MemtableEntries == 0 {
		t.Fatalf("expected memtable entries > 0")
	}
	if stats.Closing || stats.Closed {
		t.Fatalf("expected open state, got closing=%v closed=%v", stats.Closing, stats.Closed)
	}
}

func TestServerStatsIncludesSSTablePressure(t *testing.T) {
	store, err := lsm.New(lsm.Options{
		DataDir:       t.TempDir(),
		MemtableLimit: 1,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}()

	if err := store.Put([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("put: %v", err)
	}

	handler := server.NewHandler(store)
	var stats lsm.Stats
	waitForMonitorStats(t, func() bool {
		stats = readJSON[lsm.Stats](t, handler, "/stats")
		return stats.SSTableCount >= 1
	})
	if stats.SSTableBytes == 0 {
		t.Fatalf("expected sstable bytes > 0")
	}
	if stats.L0TableCount == 0 {
		t.Fatalf("expected l0 sstable count > 0")
	}
	if len(stats.SSTableLevels) == 0 || stats.SSTableLevels[0].Level != 0 {
		t.Fatalf("expected l0 level stats, got %+v", stats.SSTableLevels)
	}
}

func TestServerHealthAfterClose(t *testing.T) {
	store, err := lsm.New(lsm.Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	handler := server.NewHandler(store)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", rec.Code)
	}
	var health lsm.Health
	if err := json.NewDecoder(rec.Body).Decode(&health); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if health.Reason != "closed" {
		t.Fatalf("expected closed health, got %+v", health)
	}
}

func waitForMonitorStats(t *testing.T, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for stats condition")
}

func readJSON[T any](t *testing.T, handler http.Handler, path string) T {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	var out T
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return out
}
