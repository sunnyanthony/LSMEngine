//go:build test

package integration_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"lsmengine/pkg/lsm"
	"lsmengine/pkg/lsm/server"
)

func TestServerRoutingEndpoint(t *testing.T) {
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
	req := httptest.NewRequest(http.MethodGet, "/cluster/routes", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d (%s)", rec.Code, rec.Body.String())
	}
	var out struct {
		Revision uint64 `json:"revision"`
		Shards   []struct {
			ID             string `json:"id"`
			StartKeyBase64 string `json:"start_key_base64"`
			EndKeyBase64   string `json:"end_key_base64"`
			Leader         string `json:"leader"`
		} `json:"shards"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Revision != 0 {
		t.Fatalf("expected revision 0, got %d", out.Revision)
	}
	if len(out.Shards) != 1 {
		t.Fatalf("expected one shard route, got %d", len(out.Shards))
	}
	if out.Shards[0].Leader != "node-a" {
		t.Fatalf("expected leader node-a, got %s", out.Shards[0].Leader)
	}
}

func TestServerWriteNotLeaderReturnsRouteHint(t *testing.T) {
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
				Leader:   "node-b",
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
	body := bytes.NewBufferString(`{"key_base64":"` + base64.StdEncoding.EncodeToString([]byte("a")) + `","value_base64":"` + base64.StdEncoding.EncodeToString([]byte("1")) + `","consistency":"local_committed"}`)
	req := httptest.NewRequest(http.MethodPost, "/kv/put", body)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected status 409, got %d (%s)", rec.Code, rec.Body.String())
	}
	var out struct {
		Error     string `json:"error"`
		Code      string `json:"code"`
		Retryable bool   `json:"retryable"`
		Route     *struct {
			Revision uint64 `json:"revision"`
			ShardID  string `json:"shard_id"`
			Leader   string `json:"leader"`
		} `json:"route"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Code != "not_leader" {
		t.Fatalf("expected not_leader code, got %s", out.Code)
	}
	if !out.Retryable {
		t.Fatalf("expected retryable=true")
	}
	if out.Route == nil {
		t.Fatalf("expected route hint")
	}
	if out.Route.ShardID != "users" {
		t.Fatalf("expected shard users, got %s", out.Route.ShardID)
	}
	if out.Route.Leader != "node-b" {
		t.Fatalf("expected leader node-b, got %s", out.Route.Leader)
	}
}
