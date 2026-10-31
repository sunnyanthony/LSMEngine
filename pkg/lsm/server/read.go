package server

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"

	"lsmengine/pkg/lsm"
	"lsmengine/pkg/lsm/errs"
)

type readResponse struct {
	KeyBase64   string `json:"key_base64"`
	ValueBase64 string `json:"value_base64"`
	Found       bool   `json:"found"`
	Sequence    uint64 `json:"sequence"`
	Consistency string `json:"consistency"`
}

func (h *handler) handleGet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	reader, ok := h.provider.(lsm.ReadProvider)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		http.Error(w, "invalid query", http.StatusBadRequest)
		return
	}
	for name, values := range query {
		if name != "key_base64" || len(values) != 1 {
			http.Error(w, "expected one key_base64 parameter", http.StatusBadRequest)
			return
		}
	}
	key, err := base64.StdEncoding.Strict().DecodeString(query.Get("key_base64"))
	if err != nil || len(key) == 0 {
		http.Error(w, "key_base64 must encode a nonempty key", http.StatusBadRequest)
		return
	}
	entry, found, err := reader.GetContext(r.Context(), key)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errs.ErrClosed) {
			status = http.StatusServiceUnavailable
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			status = http.StatusRequestTimeout
		}
		http.Error(w, "read failed", status)
		return
	}
	response := readResponse{KeyBase64: base64.StdEncoding.EncodeToString(key), Found: found, Consistency: "local_applied"}
	status := http.StatusOK
	if found {
		response.ValueBase64 = base64.StdEncoding.EncodeToString(entry.Value)
		response.Sequence = entry.Seq
	} else {
		status = http.StatusNotFound
	}
	writeJSON(w, status, response)
}
