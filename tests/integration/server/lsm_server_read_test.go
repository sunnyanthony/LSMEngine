//go:build test

package integration_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"lsmengine/pkg/lsm/engine"
	"lsmengine/pkg/lsm/server"
)

func TestOnlineReadRestart(t *testing.T) {
	dir := t.TempDir()
	l, err := engine.New(engine.Options{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	key := []byte{0, 255, '+'}
	value := []byte{255, 0, 1}
	if err := l.Put(key, value); err != nil {
		t.Fatal(err)
	}
	if err := l.Put([]byte("deleted"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := l.Delete([]byte("deleted")); err != nil {
		t.Fatal(err)
	}
	check := func(key []byte, wantStatus int, wantValue []byte) {
		t.Helper()
		srv := httptest.NewServer(server.NewHandler(l))
		defer srv.Close()
		client := &http.Client{Timeout: 5 * time.Second}
		res, err := client.Get(srv.URL + "/kv/get?" + url.Values{"key_base64": {base64.StdEncoding.EncodeToString(key)}}.Encode())
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		if res.StatusCode != wantStatus {
			t.Fatalf("status=%d want=%d", res.StatusCode, wantStatus)
		}
		if wantStatus != 200 && wantStatus != 404 {
			return
		}
		var result struct {
			Found       bool   `json:"found"`
			Value       string `json:"value_base64"`
			Consistency string `json:"consistency"`
		}
		if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		if result.Found != (wantStatus == 200) || result.Value != base64.StdEncoding.EncodeToString(wantValue) || result.Consistency != "local_applied" {
			t.Fatalf("response=%+v", result)
		}
	}
	check(key, 200, value)
	check([]byte("deleted"), 404, nil)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	check(key, 503, nil)
	l, err = engine.New(engine.Options{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	check(key, 200, value)
	check([]byte("deleted"), 404, nil)
}
