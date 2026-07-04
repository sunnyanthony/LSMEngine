//go:build test

package integration_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"lsmengine/pkg/lsm"
	"lsmengine/pkg/lsm/server"
)

func TestServerWriteConsistencyEndpoints(t *testing.T) {
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

	putReqBody := bytes.NewBufferString(`{"key_base64":"` + base64.StdEncoding.EncodeToString([]byte("k")) + `","value_base64":"` + base64.StdEncoding.EncodeToString([]byte("v")) + `"}`)
	putReq := httptest.NewRequest(http.MethodPost, "/kv/put", putReqBody)
	putRec := httptest.NewRecorder()
	handler.ServeHTTP(putRec, putReq)
	if putRec.Code != http.StatusAccepted {
		t.Fatalf("expected put accepted status 202, got %d (%s)", putRec.Code, putRec.Body.String())
	}

	var accepted lsm.WriteRequestStatus
	if err := json.NewDecoder(putRec.Body).Decode(&accepted); err != nil {
		t.Fatalf("decode accepted response: %v", err)
	}
	if accepted.State != lsm.WriteRequestPending {
		t.Fatalf("expected pending accepted state, got %s", accepted.State)
	}

	final := pollWriteStatus(t, handler, accepted.RequestID, 2*time.Second)
	if final.State != lsm.WriteRequestCommitted {
		t.Fatalf("expected committed state, got %s", final.State)
	}
	if got, ok := store.Get([]byte("k")); !ok || string(got.Value) != "v" {
		t.Fatalf("expected value v after accepted write, got %q found=%v", string(got.Value), ok)
	}

	deleteReqBody := bytes.NewBufferString(`{"key_base64":"` + base64.StdEncoding.EncodeToString([]byte("k")) + `","consistency":"local_committed"}`)
	deleteReq := httptest.NewRequest(http.MethodPost, "/kv/delete", deleteReqBody)
	deleteRec := httptest.NewRecorder()
	handler.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("expected delete status 200, got %d (%s)", deleteRec.Code, deleteRec.Body.String())
	}

	var deleted lsm.WriteRequestStatus
	if err := json.NewDecoder(deleteRec.Body).Decode(&deleted); err != nil {
		t.Fatalf("decode delete response: %v", err)
	}
	if deleted.State != lsm.WriteRequestCommitted {
		t.Fatalf("expected committed delete state, got %s", deleted.State)
	}
	if _, ok := store.Get([]byte("k")); ok {
		t.Fatalf("expected key to be deleted")
	}
}

func pollWriteStatus(
	t *testing.T,
	handler http.Handler,
	requestID string,
	timeout time.Duration,
) lsm.WriteRequestStatus {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var status lsm.WriteRequestStatus
	for {
		req := httptest.NewRequest(http.MethodGet, "/kv/write-status/"+requestID, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d (%s)", rec.Code, rec.Body.String())
		}
		if err := json.NewDecoder(rec.Body).Decode(&status); err != nil {
			t.Fatalf("decode status: %v", err)
		}
		if status.State == lsm.WriteRequestCommitted || status.State == lsm.WriteRequestRejected {
			return status
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for write status for %s; last=%+v", requestID, status)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
