package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"lsmengine/pkg/lsm"
	"lsmengine/pkg/lsm/server"
)

func TestClusterWriteCLIRealHTTP(t *testing.T) {
	store, err := lsm.New(lsm.Options{DataDir: t.TempDir(), NodeID: "node-a"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	endpoint := httptest.NewServer(server.NewHandler(store))
	defer endpoint.Close()
	nodes, _ := json.Marshal(map[string]string{"node-a": endpoint.URL})
	args := []string{"--bootstrap-url", endpoint.URL, "--node-endpoints", string(nodes), "--key-base64", "AP8="}
	var output bytes.Buffer
	if err := runClusterWrite(context.Background(), "put", append(append([]string(nil), args...), "--value-base64", "dmFsdWUA"), &output, io.Discard); err != nil {
		t.Fatal(err)
	}
	var status lsm.WriteRequestStatus
	if err := json.Unmarshal(output.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.State != lsm.WriteRequestCommitted || status.Consistency != lsm.WriteConsistencyLocalCommitted {
		t.Fatalf("default acknowledgment: %+v", status)
	}
	entry, ok := store.Get([]byte{0, 255})
	if !ok || !bytes.Equal(entry.Value, []byte("value\x00")) {
		t.Fatal("binary value not stored")
	}
	output.Reset()
	if err := runClusterWrite(context.Background(), "delete", args, &output, io.Discard); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.Get([]byte{0, 255}); ok {
		t.Fatal("delete not applied")
	}
	if err := json.Unmarshal(output.Bytes(), &status); err != nil || status.Operation != "delete" || status.State != lsm.WriteRequestCommitted {
		t.Fatalf("delete acknowledgment: %+v err=%v", status, err)
	}
	output.Reset()
	if err := runClusterWrite(context.Background(), "put", []string{"--bootstrap-url", endpoint.URL, "--node-endpoints", string(nodes), "--key", "async", "--value", "value", "--consistency", "accepted"}, &output, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(output.Bytes(), &status); err != nil || status.State != lsm.WriteRequestPending || status.Consistency != lsm.WriteConsistencyAccepted {
		t.Fatalf("accepted acknowledgment: %+v err=%v", status, err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		if _, ok := store.Get([]byte("async")); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("accepted write did not finish")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestClusterWriteCLIValidation(t *testing.T) {
	base := []string{"--bootstrap-url", "http://localhost:1", "--node-endpoints", `{"a":"http://localhost:1"}`, "--key", "key", "--value", "value"}
	for _, extra := range [][]string{
		{"--key-base64", "aw=="}, {"--value-base64", "dg=="}, {"--key", ""},
		{"--consistency", "linearizable"}, {"--consistency", ""}, {"--timeout", "0"},
		{"--bootstrap-url", "ftp://host"}, {"--bootstrap-url", "http://host?x=1"},
		{"--bootstrap-url", "http://host?"}, {"--bootstrap-url", "http://host#"},
		{"--node-endpoints", "null"}, {"--node-endpoints", `{"a":"invalid"}`},
		{"--node-endpoints", `{"a":"http://localhost"," a ":"http://localhost"}`}, {"extra"},
	} {
		var output bytes.Buffer
		args := append(append([]string(nil), base...), extra...)
		if err := runClusterWrite(context.Background(), "put", args, &output, io.Discard); err == nil {
			t.Fatalf("accepted invalid flags: %v", extra)
		}
		if output.Len() != 0 {
			t.Fatal("invalid input emitted success output")
		}
	}
	for _, args := range [][]string{base[:len(base)-2], append(append([]string(nil), base[:len(base)-2]...), "--value-base64", "!")} {
		if err := runClusterWrite(context.Background(), "put", args, io.Discard, io.Discard); err == nil {
			t.Fatalf("accepted missing/invalid value: %v", args)
		}
	}
	if err := runClusterWrite(context.Background(), "delete", []string{"--help"}, io.Discard, io.Discard); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("help: %v", err)
	}
}

func TestClusterWriteCLIRejectsUnconfirmedSuccess(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cluster/routes" {
			_, _ = io.WriteString(w, `{"shards":[{"id":"default","leader":"a"}]}`)
			return
		}
		_ = json.NewEncoder(w).Encode(lsm.WriteRequestStatus{RequestID: "id", Operation: "put", Consistency: lsm.WriteConsistencyLocalCommitted, State: lsm.WriteRequestPending})
	}))
	defer endpoint.Close()
	nodes, _ := json.Marshal(map[string]string{"a": endpoint.URL})
	var output bytes.Buffer
	err := runClusterWrite(context.Background(), "put", []string{"--bootstrap-url", endpoint.URL, "--node-endpoints", string(nodes), "--key", "k", "--value", "v"}, &output, io.Discard)
	if err == nil || output.Len() != 0 {
		t.Fatalf("pending result reported as committed: %v %s", err, output.String())
	}
}

func TestClusterWriteCLIDeadline(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer endpoint.Close()
	nodes, _ := json.Marshal(map[string]string{"a": endpoint.URL})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := runClusterWrite(ctx, "delete", []string{"--bootstrap-url", endpoint.URL, "--node-endpoints", string(nodes), "--key", "k", "--timeout", "20ms"}, io.Discard, io.Discard)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline not respected: %v", err)
	}
}

func TestClusterWriteCLIWriteDeadline(t *testing.T) {
	for _, tc := range []struct {
		name, timeout string
		succeeds      bool
	}{
		{"slow-commit", "5s", true}, {"deadline", "20ms", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/cluster/routes" {
					_, _ = io.WriteString(w, `{"shards":[{"id":"default","leader":"a"}]}`)
					return
				}
				select {
				case <-time.After(3500 * time.Millisecond):
				case <-r.Context().Done():
					return
				}
				_ = json.NewEncoder(w).Encode(lsm.WriteRequestStatus{RequestID: "id", Operation: "put", Consistency: lsm.WriteConsistencyLocalCommitted, State: lsm.WriteRequestCommitted})
			}))
			defer endpoint.Close()
			nodes, _ := json.Marshal(map[string]string{"a": endpoint.URL})
			var output bytes.Buffer
			err := runClusterWrite(context.Background(), "put", []string{"--bootstrap-url", endpoint.URL, "--node-endpoints", string(nodes), "--key", "k", "--value", "v", "--timeout", tc.timeout}, &output, io.Discard)
			if tc.succeeds {
				if err != nil {
					t.Fatalf("write cut off before CLI deadline: %v", err)
				}
				var status lsm.WriteRequestStatus
				if err := json.Unmarshal(output.Bytes(), &status); err != nil || status.State != lsm.WriteRequestCommitted {
					t.Fatalf("missing committed status: %v %+v", err, status)
				}
			} else if !errors.Is(err, context.DeadlineExceeded) || output.Len() != 0 {
				t.Fatalf("write deadline: err=%v output=%s", err, output.String())
			}
		})
	}
}
