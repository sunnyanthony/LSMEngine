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

func TestServerEtcdRaftCommitLogWriteAndControl(t *testing.T) {
	store, err := lsm.New(lsm.Options{
		DataDir: t.TempDir(),
		NodeID:  "node-a",
		CommitLog: &lsm.CommitLogOptions{
			Provider: lsm.CommitLogProviderEtcdRaft,
		},
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

	status := readControlJSON[lsm.ClusterStatus](t, handler, "/cluster/status")
	if status.CommitLog != string(lsm.CommitLogProviderEtcdRaft) {
		t.Fatalf("expected commit log etcd-raft, got %q", status.CommitLog)
	}

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
	final := pollWriteStatus(t, handler, accepted.RequestID, 2*time.Second)
	if final.State != lsm.WriteRequestCommitted {
		t.Fatalf("expected committed state, got %s", final.State)
	}

	transferReq := httptest.NewRequest(
		http.MethodPost,
		"/cluster/shards/users/transfer-leader",
		bytes.NewBufferString(`{"target":"node-b","operation_id":"op-1","expected_revision":0}`),
	)
	transferRec := httptest.NewRecorder()
	handler.ServeHTTP(transferRec, transferReq)
	if transferRec.Code != http.StatusOK {
		t.Fatalf("expected transfer status 200, got %d (%s)", transferRec.Code, transferRec.Body.String())
	}

	localReqBody := bytes.NewBufferString(`{"key_base64":"` + base64.StdEncoding.EncodeToString([]byte("k2")) + `","value_base64":"` + base64.StdEncoding.EncodeToString([]byte("v2")) + `","consistency":"local_committed"}`)
	localReq := httptest.NewRequest(http.MethodPost, "/kv/put", localReqBody)
	localRec := httptest.NewRecorder()
	handler.ServeHTTP(localRec, localReq)
	if localRec.Code != http.StatusConflict {
		t.Fatalf("expected not-leader status 409 after transfer, got %d (%s)", localRec.Code, localRec.Body.String())
	}
}
