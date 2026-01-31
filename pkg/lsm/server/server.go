// HTTP monitoring handlers for stats and health.

package server

import (
	"encoding/json"
	"net/http"

	"lsmengine/pkg/lsm"
)

// NewHandler returns an HTTP handler that serves /healthz and /stats.
func NewHandler(provider lsm.StatsProvider) http.Handler {
	mux := http.NewServeMux()
	handler := &handler{provider: provider}
	mux.HandleFunc("/healthz", handler.handleHealth)
	mux.HandleFunc("/stats", handler.handleStats)
	return mux
}

// Serve runs an HTTP server for the given provider.
func Serve(addr string, provider lsm.StatsProvider) error {
	srv := &http.Server{
		Addr:    addr,
		Handler: NewHandler(provider),
	}
	return srv.ListenAndServe()
}

type handler struct {
	provider lsm.StatsProvider
}

func (h *handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if h.provider == nil {
		writeJSON(w, http.StatusServiceUnavailable, lsm.Health{
			Ready:  false,
			Reason: "unavailable",
		})
		return
	}
	health := h.provider.Health()
	status := http.StatusOK
	if !health.Ready {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, health)
}

func (h *handler) handleStats(w http.ResponseWriter, r *http.Request) {
	if h.provider == nil {
		writeJSON(w, http.StatusServiceUnavailable, lsm.Stats{})
		return
	}
	writeJSON(w, http.StatusOK, h.provider.Stats())
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
