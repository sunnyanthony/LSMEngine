//go:build test

package integration_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"lsmengine/pkg/lsm"
	"lsmengine/pkg/lsm/server"
)

func TestStorageStatsFlushAndRestart(t *testing.T) {
	opts := lsm.Options{DataDir: t.TempDir(), MemtableLimit: 1}
	store, err := lsm.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	var before lsm.Stats
	deadline := time.Now().Add(5 * time.Second)
	for {
		before = readJSON[lsm.Stats](t, server.NewHandler(store), "/stats")
		if before.TableCount == 1 && before.FlushQueueDepth == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("flush did not finish: %+v", before)
		}
		time.Sleep(10 * time.Millisecond)
	}
	files, err := filepath.Glob(filepath.Join(opts.DataDir, "sstables", "*.sst"))
	if err != nil {
		t.Fatal(err)
	}
	var bytes uint64
	for _, path := range files {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		bytes += uint64(info.Size())
	}
	want := []lsm.SSTableLevelStats{{Level: 0, TableCount: 1, SizeBytes: bytes}}
	if len(files) != 1 || bytes == 0 || before.SSTableBytes != bytes || !reflect.DeepEqual(before.SSTableLevels, want) {
		t.Fatalf("stats disagree with flushed files (%v, %d bytes): %+v", files, bytes, before)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := lsm.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	after := readJSON[lsm.Stats](t, server.NewHandler(reopened), "/stats")
	if after.TableCount != before.TableCount || after.SSTableBytes != bytes || !reflect.DeepEqual(after.SSTableLevels, want) {
		t.Fatalf("stats changed on restart: before=%+v after=%+v", before, after)
	}
}

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
