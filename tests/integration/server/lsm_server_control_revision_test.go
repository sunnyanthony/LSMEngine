//go:build test

package integration_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"lsmengine/pkg/lsm"
	"lsmengine/pkg/lsm/server"
)

func TestServerControlRevisionAndIdempotency(t *testing.T) {
	store, err := lsm.New(lsm.Options{
		DataDir:   t.TempDir(),
		NodeID:    "node-a",
		ClusterID: "cluster-dev",
		ShardMap: []lsm.ShardConfig{
			{
				ID:       "users",
				StartKey: []byte("a"),
				EndKey:   []byte("z"),
				Replicas: []string{"node-a", "node-b"},
				Leader:   "node-a",
			},
		},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}()

	handler := server.NewHandler(store)

	status0 := readControlJSON[lsm.ClusterStatus](t, handler, "/cluster/status")
	if status0.Revision != 0 {
		t.Fatalf("expected initial revision=0, got %d", status0.Revision)
	}

	req1 := httptest.NewRequest(
		http.MethodPost,
		"/cluster/shards/users/transfer-leader",
		bytes.NewBufferString(`{"target":"node-b","operation_id":"op-1","expected_revision":0}`),
	)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("expected first status 200, got %d", rec1.Code)
	}

	reqRetry := httptest.NewRequest(
		http.MethodPost,
		"/cluster/shards/users/transfer-leader",
		bytes.NewBufferString(`{"target":"node-b","operation_id":"op-1","expected_revision":0}`),
	)
	recRetry := httptest.NewRecorder()
	handler.ServeHTTP(recRetry, reqRetry)
	if recRetry.Code != http.StatusOK {
		t.Fatalf("expected retry status 200, got %d", recRetry.Code)
	}

	status1 := readControlJSON[lsm.ClusterStatus](t, handler, "/cluster/status")
	if status1.Revision != 1 {
		t.Fatalf("expected revision=1 after idempotent retry, got %d", status1.Revision)
	}

	reqConflict := httptest.NewRequest(
		http.MethodPost,
		"/cluster/shards/users/transfer-leader",
		bytes.NewBufferString(`{"target":"node-a","operation_id":"op-2","expected_revision":0}`),
	)
	recConflict := httptest.NewRecorder()
	handler.ServeHTTP(recConflict, reqConflict)
	if recConflict.Code != http.StatusConflict {
		t.Fatalf("expected conflict status 409, got %d", recConflict.Code)
	}
}

func TestServerControlRejectsUnknownFieldsAndTrailingJSON(t *testing.T) {
	store, err := lsm.New(lsm.Options{
		DataDir:   t.TempDir(),
		NodeID:    "node-a",
		ClusterID: "cluster-dev",
		ShardMap: []lsm.ShardConfig{
			{
				ID:       "users",
				StartKey: []byte("a"),
				EndKey:   []byte("z"),
				Replicas: []string{"node-a", "node-b"},
				Leader:   "node-a",
			},
		},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}()

	handler := server.NewHandler(store)

	reqUnknown := httptest.NewRequest(
		http.MethodPost,
		"/cluster/shards/users/transfer-leader",
		bytes.NewBufferString(`{"target":"node-b","unexpected":true}`),
	)
	recUnknown := httptest.NewRecorder()
	handler.ServeHTTP(recUnknown, reqUnknown)
	if recUnknown.Code != http.StatusBadRequest {
		t.Fatalf("expected unknown-field status 400, got %d", recUnknown.Code)
	}

	reqTrailing := httptest.NewRequest(
		http.MethodPost,
		"/cluster/shards/users/transfer-leader",
		bytes.NewBufferString(`{"target":"node-b"}{"target":"node-a"}`),
	)
	recTrailing := httptest.NewRecorder()
	handler.ServeHTTP(recTrailing, reqTrailing)
	if recTrailing.Code != http.StatusBadRequest {
		t.Fatalf("expected trailing-json status 400, got %d", recTrailing.Code)
	}
}

func readControlJSON[T any](t *testing.T, handler http.Handler, path string) T {
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
