package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"

	"lsmengine/pkg/lsm/errs"
	"lsmengine/pkg/lsm/types"
)

type readStub struct {
	stubProvider
	err   error
	found bool
	calls int
}

func (s *readStub) GetContext(ctx context.Context, key []byte) (types.Entry, bool, error) {
	s.calls++
	return types.Entry{Value: []byte{0, 255}, Seq: 7}, s.found, s.err
}

func TestReadHTTP(t *testing.T) {
	for _, tc := range []struct {
		name, method, query string
		found               bool
		err                 error
		status, calls       int
	}{
		{"found", "GET", "key_base64=YQ==", true, nil, 200, 1},
		{"missing", "GET", "key_base64=YQ==", false, nil, 404, 1},
		{"io", "GET", "key_base64=YQ==", false, errors.New("private path"), 500, 1},
		{"closed", "GET", "key_base64=YQ==", false, errs.ErrClosed, 503, 1},
		{"empty", "GET", "", false, nil, 400, 0},
		{"invalid", "GET", "key_base64=!!!", false, nil, 400, 0},
		{"duplicate", "GET", "key_base64=YQ==&key_base64=Yg==", false, nil, 400, 0},
		{"unsupported-consistency", "GET", "key_base64=YQ==&consistency=linearizable", false, nil, 400, 0},
		{"method", "POST", "key_base64=YQ==", false, nil, 405, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := &readStub{found: tc.found, err: tc.err}
			w := httptest.NewRecorder()
			NewHandler(provider).ServeHTTP(w, httptest.NewRequest(tc.method, "/kv/get?"+tc.query, nil))
			if w.Code != tc.status || provider.calls != tc.calls {
				t.Fatalf("status=%d calls=%d body=%s", w.Code, provider.calls, w.Body.String())
			}
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("missing cache policy")
			}
			if tc.calls == 1 && tc.err == nil {
				var result readResponse
				if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				if result.Found != tc.found || result.Consistency != "local_applied" {
					t.Fatalf("response=%+v", result)
				}
				if tc.found && (result.ValueBase64 != "AP8=" || result.Sequence != 7) {
					t.Fatalf("value=%+v", result)
				}
			}
		})
	}
}
