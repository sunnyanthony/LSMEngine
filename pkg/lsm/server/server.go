// HTTP monitoring handlers for stats and health.

package server

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"lsmengine/pkg/lsm"
	"lsmengine/pkg/lsm/errs"
)

// NewHandler returns an HTTP handler that serves monitoring and control APIs.
func NewHandler(provider lsm.StatsProvider) http.Handler {
	mux := http.NewServeMux()
	handler := &handler{provider: provider}
	if control, ok := provider.(lsm.ControlProvider); ok {
		handler.control = control
		if advanced, ok := provider.(lsm.ControlProviderWithOptions); ok {
			handler.controlWithOptions = advanced
		}
	}
	mux.HandleFunc("/healthz", handler.handleHealth)
	mux.HandleFunc("/stats", handler.handleStats)
	mux.HandleFunc("/cluster/status", handler.handleClusterStatus)
	mux.HandleFunc("/cluster/shards", handler.handleShards)
	mux.HandleFunc("/cluster/shards/", handler.handleShardAction)
	mux.HandleFunc("/cluster/nodes/", handler.handleNodeAction)
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
	provider           lsm.StatsProvider
	control            lsm.ControlProvider
	controlWithOptions lsm.ControlProviderWithOptions
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

func (h *handler) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.control == nil {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, h.control.ClusterStatus())
}

func (h *handler) handleShards(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.control == nil {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, h.control.Shards())
}

func (h *handler) handleShardAction(w http.ResponseWriter, r *http.Request) {
	if h.control == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	shardID, action, ok := shardActionPath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	switch action {
	case "transfer-leader":
		var req targetRequest
		if !decodeJSONBody(w, r, &req) {
			return
		}
		writeActionResult(w, h.transferLeader(shardID, req))
	case "rebalance":
		var req targetRequest
		if !decodeJSONBody(w, r, &req) {
			return
		}
		writeActionResult(w, h.rebalance(shardID, req))
	case "split":
		var req splitRequest
		if !decodeJSONBody(w, r, &req) {
			return
		}
		splitKey, err := base64.StdEncoding.DecodeString(req.SplitKeyBase64)
		if err != nil {
			http.Error(w, "invalid split_key_base64", http.StatusBadRequest)
			return
		}
		writeActionResult(w, h.split(shardID, splitKey, req.controlWriteOptions()))
	default:
		http.NotFound(w, r)
	}
}

func (h *handler) handleNodeAction(w http.ResponseWriter, r *http.Request) {
	if h.control == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	nodeID, action, ok := nodeActionPath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if action != "drain" {
		http.NotFound(w, r)
		return
	}
	var req drainRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	writeActionResult(w, h.drain(nodeID, req.controlWriteOptions()))
}

type controlRequestOptions struct {
	OperationID      string  `json:"operation_id,omitempty"`
	ExpectedRevision *uint64 `json:"expected_revision,omitempty"`
}

func (o controlRequestOptions) controlWriteOptions() lsm.ControlWriteOptions {
	return lsm.ControlWriteOptions{
		OperationID:      o.OperationID,
		ExpectedRevision: o.ExpectedRevision,
	}
}

func (o controlRequestOptions) hasControlWriteOptions() bool {
	return strings.TrimSpace(o.OperationID) != "" || o.ExpectedRevision != nil
}

type targetRequest struct {
	Target string `json:"target"`
	controlRequestOptions
}

type splitRequest struct {
	SplitKeyBase64 string `json:"split_key_base64"`
	controlRequestOptions
}

type drainRequest struct {
	controlRequestOptions
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, out any) bool {
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(out); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

func writeActionResult(w http.ResponseWriter, err error) {
	if err == nil {
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
		return
	}
	if errors.Is(err, errs.ErrShardNotFound) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if errors.Is(err, errs.ErrControlRevisionConflict) || errors.Is(err, errs.ErrControlOperationConflict) {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	http.Error(w, err.Error(), http.StatusBadRequest)
}

func (h *handler) transferLeader(shardID string, req targetRequest) error {
	opts := req.controlWriteOptions()
	if h.controlWithOptions != nil {
		return h.controlWithOptions.TransferLeaderWithOptions(shardID, req.Target, opts)
	}
	if req.hasControlWriteOptions() {
		return errs.ErrControlRevisionConflict
	}
	return h.control.TransferLeader(shardID, req.Target)
}

func (h *handler) rebalance(shardID string, req targetRequest) error {
	opts := req.controlWriteOptions()
	if h.controlWithOptions != nil {
		return h.controlWithOptions.TriggerRebalanceWithOptions(shardID, req.Target, opts)
	}
	if req.hasControlWriteOptions() {
		return errs.ErrControlRevisionConflict
	}
	return h.control.TriggerRebalance(shardID, req.Target)
}

func (h *handler) split(shardID string, splitKey []byte, opts lsm.ControlWriteOptions) error {
	if h.controlWithOptions != nil {
		return h.controlWithOptions.TriggerSplitWithOptions(shardID, splitKey, opts)
	}
	if strings.TrimSpace(opts.OperationID) != "" || opts.ExpectedRevision != nil {
		return errs.ErrControlRevisionConflict
	}
	return h.control.TriggerSplit(shardID, splitKey)
}

func (h *handler) drain(nodeID string, opts lsm.ControlWriteOptions) error {
	if h.controlWithOptions != nil {
		return h.controlWithOptions.PrepareDrainWithOptions(nodeID, opts)
	}
	if strings.TrimSpace(opts.OperationID) != "" || opts.ExpectedRevision != nil {
		return errs.ErrControlRevisionConflict
	}
	return h.control.PrepareDrain(nodeID)
}

func shardActionPath(path string) (shardID string, action string, ok bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 4 {
		return "", "", false
	}
	if parts[0] != "cluster" || parts[1] != "shards" || parts[2] == "" || parts[3] == "" {
		return "", "", false
	}
	return parts[2], parts[3], true
}

func nodeActionPath(path string) (nodeID string, action string, ok bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 4 {
		return "", "", false
	}
	if parts[0] != "cluster" || parts[1] != "nodes" || parts[2] == "" || parts[3] == "" {
		return "", "", false
	}
	return parts[2], parts[3], true
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
