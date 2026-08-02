//go:build test

package integration_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"lsmengine/pkg/lsm"
	"lsmengine/pkg/lsm/server"
)

type cdcReadResponse struct {
	ShardID       string             `json:"shard_id"`
	FromOffset    uint64             `json:"from_offset"`
	NextOffset    uint64             `json:"next_offset"`
	OldestOffset  uint64             `json:"oldest_offset"`
	DroppedBefore bool               `json:"dropped_before"`
	Events        []cdcEventResponse `json:"events"`
}

type cdcEventResponse struct {
	Offset      uint64 `json:"offset"`
	Operation   string `json:"operation"`
	KeyBase64   string `json:"key_base64,omitempty"`
	ValueBase64 string `json:"value_base64,omitempty"`
	Tombstone   bool   `json:"tombstone,omitempty"`
}

type cdcStatusResponse struct {
	Durable           bool                 `json:"durable"`
	Source            string               `json:"source"`
	ReplayOnRestart   bool                 `json:"replay_on_restart"`
	MaxEventsPerShard int                  `json:"max_events_per_shard"`
	Shards            []lsm.CDCShardStatus `json:"shards"`
}

func TestServerCDCEventsReturnsEmptyForKnownShardWithNoEvents(t *testing.T) {
	store, err := lsm.New(lsm.Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}()

	handler := server.NewHandler(store)
	req := httptest.NewRequest(http.MethodGet, "/cdc/events?shard=default&offset=7&limit=10", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected cdc status 200, got %d (%s)", rec.Code, rec.Body.String())
	}
	var out cdcReadResponse
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode cdc response: %v", err)
	}
	if out.ShardID != "default" || out.FromOffset != 7 || out.NextOffset != 7 {
		t.Fatalf("unexpected empty cdc response metadata: %+v", out)
	}
	if len(out.Events) != 0 {
		t.Fatalf("expected no cdc events, got %+v", out.Events)
	}
}

func TestServerCDCStatusReportsMemoryRetention(t *testing.T) {
	store, err := lsm.New(lsm.Options{
		DataDir:              t.TempDir(),
		CDCMaxEventsPerShard: 1,
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
	for _, key := range []string{"a", "b"} {
		putReqBody := bytes.NewBufferString(`{"key_base64":"` + base64.StdEncoding.EncodeToString([]byte(key)) + `","value_base64":"` + base64.StdEncoding.EncodeToString([]byte("v")) + `","consistency":"local_committed"}`)
		putReq := httptest.NewRequest(http.MethodPost, "/kv/put", putReqBody)
		putRec := httptest.NewRecorder()
		handler.ServeHTTP(putRec, putReq)
		if putRec.Code != http.StatusOK {
			t.Fatalf("expected put status 200, got %d (%s)", putRec.Code, putRec.Body.String())
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/cdc/status", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected cdc status 200, got %d (%s)", rec.Code, rec.Body.String())
	}
	var out cdcStatusResponse
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode cdc status: %v", err)
	}
	if out.Durable || out.ReplayOnRestart || out.Source != lsm.CDCSourceMemory {
		t.Fatalf("expected memory non-durable cdc status, got %+v", out)
	}
	if out.MaxEventsPerShard != 1 {
		t.Fatalf("expected retention 1, got %+v", out)
	}
	if len(out.Shards) != 1 || out.Shards[0].ShardID != "default" || out.Shards[0].RetainedEvents != 1 {
		t.Fatalf("unexpected shard status: %+v", out.Shards)
	}
	if out.Shards[0].OldestOffset == 0 || out.Shards[0].NextOffset != out.Shards[0].OldestOffset {
		t.Fatalf("expected retained single-event cursor window, got %+v", out.Shards[0])
	}

	eventsReq := httptest.NewRequest(http.MethodGet, "/cdc/events?shard=default&offset="+strconv.FormatUint(out.Shards[0].NextOffset, 10)+"&limit=10", nil)
	eventsRec := httptest.NewRecorder()
	handler.ServeHTTP(eventsRec, eventsReq)
	if eventsRec.Code != http.StatusOK {
		t.Fatalf("expected cdc events status 200, got %d (%s)", eventsRec.Code, eventsRec.Body.String())
	}
	var events cdcReadResponse
	if err := json.NewDecoder(eventsRec.Body).Decode(&events); err != nil {
		t.Fatalf("decode cdc events: %v", err)
	}
	if len(events.Events) != 0 || events.NextOffset != out.Shards[0].NextOffset {
		t.Fatalf("expected status cursor to read no duplicate events, got %+v", events)
	}
}

func TestServerCDCEventsRecentRead(t *testing.T) {
	store, err := lsm.New(lsm.Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}()

	handler := server.NewHandler(store)

	putReqBody := bytes.NewBufferString(`{"key_base64":"` + base64.StdEncoding.EncodeToString([]byte("k")) + `","value_base64":"` + base64.StdEncoding.EncodeToString([]byte("v")) + `","consistency":"local_committed"}`)
	putReq := httptest.NewRequest(http.MethodPost, "/kv/put", putReqBody)
	putRec := httptest.NewRecorder()
	handler.ServeHTTP(putRec, putReq)
	if putRec.Code != http.StatusOK {
		t.Fatalf("expected put status 200, got %d (%s)", putRec.Code, putRec.Body.String())
	}

	delReqBody := bytes.NewBufferString(`{"key_base64":"` + base64.StdEncoding.EncodeToString([]byte("k")) + `","consistency":"local_committed"}`)
	delReq := httptest.NewRequest(http.MethodPost, "/kv/delete", delReqBody)
	delRec := httptest.NewRecorder()
	handler.ServeHTTP(delRec, delReq)
	if delRec.Code != http.StatusOK {
		t.Fatalf("expected delete status 200, got %d (%s)", delRec.Code, delRec.Body.String())
	}

	cdcReq := httptest.NewRequest(http.MethodGet, "/cdc/events?shard=default&offset=0&limit=10", nil)
	cdcRec := httptest.NewRecorder()
	handler.ServeHTTP(cdcRec, cdcReq)
	if cdcRec.Code != http.StatusOK {
		t.Fatalf("expected cdc status 200, got %d (%s)", cdcRec.Code, cdcRec.Body.String())
	}
	var out cdcReadResponse
	if err := json.NewDecoder(cdcRec.Body).Decode(&out); err != nil {
		t.Fatalf("decode cdc response: %v", err)
	}
	if out.ShardID != "default" {
		t.Fatalf("expected default shard, got %q", out.ShardID)
	}
	if len(out.Events) != 2 {
		t.Fatalf("expected 2 cdc events, got %d", len(out.Events))
	}
	if out.Events[0].Operation != "put" || out.Events[1].Operation != "delete" {
		t.Fatalf("unexpected cdc operations: %+v", out.Events)
	}
	if out.Events[0].KeyBase64 != base64.StdEncoding.EncodeToString([]byte("k")) {
		t.Fatalf("unexpected put key: %q", out.Events[0].KeyBase64)
	}
	if out.Events[0].ValueBase64 != base64.StdEncoding.EncodeToString([]byte("v")) {
		t.Fatalf("unexpected put value: %q", out.Events[0].ValueBase64)
	}
	if !out.Events[1].Tombstone {
		t.Fatalf("expected delete tombstone event")
	}
	if out.Events[0].Offset >= out.Events[1].Offset {
		t.Fatalf("expected monotonic offsets, got %d then %d", out.Events[0].Offset, out.Events[1].Offset)
	}

	replayReq := httptest.NewRequest(http.MethodGet, "/cdc/events?shard=default&offset="+strconv.FormatUint(out.Events[0].Offset, 10)+"&limit=10", nil)
	replayRec := httptest.NewRecorder()
	handler.ServeHTTP(replayRec, replayReq)
	if replayRec.Code != http.StatusOK {
		t.Fatalf("expected replay status 200, got %d (%s)", replayRec.Code, replayRec.Body.String())
	}
	var replay cdcReadResponse
	if err := json.NewDecoder(replayRec.Body).Decode(&replay); err != nil {
		t.Fatalf("decode replay response: %v", err)
	}
	if len(replay.Events) != 1 {
		t.Fatalf("expected one replay event, got %d", len(replay.Events))
	}
	if replay.Events[0].Offset != out.Events[1].Offset {
		t.Fatalf("expected replay offset %d, got %d", out.Events[1].Offset, replay.Events[0].Offset)
	}
}
