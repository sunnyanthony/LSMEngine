package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRunGet(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		wantError  bool
	}{
		{"found", `{"key_base64":"AP8=","value_base64":"/wA=","found":true,"sequence":9,"consistency":"local_applied"}`, 200, false},
		{"missing", `{"key_base64":"AP8=","value_base64":"","found":false,"sequence":0,"consistency":"local_applied"}`, 404, false},
		{"not-api", `not found`, 404, true},
		{"wrong-key", `{"key_base64":"YQ==","found":true,"consistency":"local_applied"}`, 200, true},
		{"missing-found", `{"key_base64":"AP8=","consistency":"local_applied"}`, 404, true},
		{"server-error", `failure`, 500, true},
		{"redirect", ``, 302, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.Path != "/kv/get" || r.URL.Query().Get("key_base64") != "AP8=" {
					t.Errorf("unexpected request: %s", r.URL)
				}
				w.WriteHeader(tc.status)
				_, _ = fmt.Fprint(w, tc.body)
			}))
			defer srv.Close()
			var output bytes.Buffer
			err := runGet(context.Background(), []string{"--url", srv.URL, "--key-base64", "AP8="}, &output, io.Discard)
			if (err != nil) != tc.wantError {
				t.Fatalf("err=%v output=%s", err, output.String())
			}
			if tc.wantError && output.Len() != 0 {
				t.Fatal("error emitted success output")
			}
		})
	}
}

func TestRunGetDeadlineAndRedirect(t *testing.T) {
	t.Run("body-deadline", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		}))
		defer srv.Close()
		started := time.Now()
		err := runGet(context.Background(), []string{"--url", srv.URL, "--key", "a", "--timeout", "30ms"}, io.Discard, io.Discard)
		if err == nil || time.Since(started) > time.Second {
			t.Fatalf("deadline err=%v elapsed=%s", err, time.Since(started))
		}
	})
	t.Run("no-redirect", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/kv/get" {
				t.Error("followed redirect")
			}
			http.Redirect(w, r, "/other", http.StatusFound)
		}))
		defer srv.Close()
		if err := runGet(context.Background(), []string{"--url", srv.URL, "--key", "a"}, io.Discard, io.Discard); err == nil {
			t.Fatal("accepted redirect")
		}
	})
}

func TestRunGetRejectsInvalidArguments(t *testing.T) {
	for _, args := range [][]string{
		{"--url", "http://localhost", "--key", ""},
		{"--url", "http://localhost", "--key", "a", "--key-base64", "YQ=="},
		{"--url", "http://localhost", "--key", "a", "--timeout", "0"},
		{"--url", "file:///tmp/data", "--key", "a"},
	} {
		if err := runGet(context.Background(), args, io.Discard, io.Discard); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}
