// HTTP monitoring handlers for stats and health.

package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"lsmengine/pkg/lsm"
	"lsmengine/pkg/lsm/errs"
)

// NewHandler returns an HTTP handler that serves monitoring and control APIs.
func NewHandler(provider lsm.StatsProvider) http.Handler {
	return NewHandlerWithOptions(provider, HandlerOptions{})
}

// HandlerOptions controls server API behavior.
type HandlerOptions struct {
	WriteConsistencyDefault lsm.WriteConsistency
	MaxWriteRequests        int
}

// NewHandlerWithOptions returns an HTTP handler with explicit behavior options.
func NewHandlerWithOptions(provider lsm.StatsProvider, opts HandlerOptions) http.Handler {
	resolved := resolveHandlerOptions(opts)
	mux := http.NewServeMux()
	handler := &handler{provider: provider}
	if control, ok := provider.(lsm.ControlProvider); ok {
		handler.control = control
		if advanced, ok := provider.(lsm.ControlProviderWithOptions); ok {
			handler.controlWithOptions = advanced
		}
	}
	if writer, ok := provider.(lsm.WriteProvider); ok {
		handler.writer = writer
		handler.requests = newWriteRequestStore(resolved.maxWriteRequests)
		handler.writeConsistencyDefault = resolved.writeConsistencyDefault
	}
	mux.HandleFunc("/healthz", handler.handleHealth)
	mux.HandleFunc("/stats", handler.handleStats)
	mux.HandleFunc("/cluster/status", handler.handleClusterStatus)
	mux.HandleFunc("/cluster/shards", handler.handleShards)
	mux.HandleFunc("/cluster/routes", handler.handleRoutes)
	mux.HandleFunc("/cluster/shards/", handler.handleShardAction)
	mux.HandleFunc("/cluster/nodes/", handler.handleNodeAction)
	mux.HandleFunc("/kv/put", handler.handlePut)
	mux.HandleFunc("/kv/delete", handler.handleDelete)
	mux.HandleFunc("/kv/write-status/", handler.handleWriteStatus)
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
	provider                lsm.StatsProvider
	control                 lsm.ControlProvider
	controlWithOptions      lsm.ControlProviderWithOptions
	writer                  lsm.WriteProvider
	requests                *writeRequestStore
	writeConsistencyDefault lsm.WriteConsistency
}

const defaultWriteRequestCapacity = 4096

type resolvedHandlerOptions struct {
	writeConsistencyDefault lsm.WriteConsistency
	maxWriteRequests        int
}

func resolveHandlerOptions(opts HandlerOptions) resolvedHandlerOptions {
	consistency := opts.WriteConsistencyDefault
	if consistency != lsm.WriteConsistencyLocalCommitted && consistency != lsm.WriteConsistencyAccepted {
		consistency = lsm.WriteConsistencyAccepted
	}
	max := opts.MaxWriteRequests
	if max <= 0 {
		max = defaultWriteRequestCapacity
	}
	return resolvedHandlerOptions{
		writeConsistencyDefault: consistency,
		maxWriteRequests:        max,
	}
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

type routingResponse struct {
	Revision uint64         `json:"revision"`
	Shards   []routingShard `json:"shards"`
}

type routingShard struct {
	ID             string `json:"id"`
	StartKeyBase64 string `json:"start_key_base64,omitempty"`
	EndKeyBase64   string `json:"end_key_base64,omitempty"`
	Leader         string `json:"leader"`
}

func (h *handler) handleRoutes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.control == nil {
		http.NotFound(w, r)
		return
	}
	status := h.control.ClusterStatus()
	shards := h.control.Shards()
	out := routingResponse{
		Revision: status.Revision,
		Shards:   make([]routingShard, 0, len(shards)),
	}
	for _, shard := range shards {
		out.Shards = append(out.Shards, routingShard{
			ID:             shard.ID,
			StartKeyBase64: base64.StdEncoding.EncodeToString(shard.StartKey),
			EndKeyBase64:   base64.StdEncoding.EncodeToString(shard.EndKey),
			Leader:         shard.Leader,
		})
	}
	writeJSON(w, http.StatusOK, out)
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

type putRequest struct {
	KeyBase64   string               `json:"key_base64"`
	ValueBase64 string               `json:"value_base64"`
	Consistency lsm.WriteConsistency `json:"consistency,omitempty"`
}

type deleteRequest struct {
	KeyBase64   string               `json:"key_base64"`
	Consistency lsm.WriteConsistency `json:"consistency,omitempty"`
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, out any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		if errors.Is(err, io.EOF) {
			return true
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return false
	}
	var trailing any
	if err := dec.Decode(&trailing); err != nil {
		if errors.Is(err, io.EOF) {
			return true
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return false
	}
	http.Error(w, "request body must contain a single JSON value", http.StatusBadRequest)
	return false
}

func (h *handler) handlePut(w http.ResponseWriter, r *http.Request) {
	if h.writer == nil || h.requests == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req putRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	key, err := base64.StdEncoding.DecodeString(req.KeyBase64)
	if err != nil {
		http.Error(w, "invalid key_base64", http.StatusBadRequest)
		return
	}
	value, err := base64.StdEncoding.DecodeString(req.ValueBase64)
	if err != nil {
		http.Error(w, "invalid value_base64", http.StatusBadRequest)
		return
	}
	consistency, err := normalizeWriteConsistency(req.Consistency, h.writeConsistencyDefault)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	h.executeWrite(w, consistency, "put", key, func() error {
		return h.writer.Put(key, value)
	})
}

func (h *handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	if h.writer == nil || h.requests == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req deleteRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	key, err := base64.StdEncoding.DecodeString(req.KeyBase64)
	if err != nil {
		http.Error(w, "invalid key_base64", http.StatusBadRequest)
		return
	}
	consistency, err := normalizeWriteConsistency(req.Consistency, h.writeConsistencyDefault)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	h.executeWrite(w, consistency, "delete", key, func() error {
		return h.writer.Delete(key)
	})
}

func (h *handler) handleWriteStatus(w http.ResponseWriter, r *http.Request) {
	if h.requests == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	requestID, ok := writeStatusPath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	status, found := h.requests.Get(requestID)
	if !found {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, status)
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
	if errors.Is(err, errs.ErrControlWriteOptionsUnsupported) {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if errors.Is(err, errs.ErrControlRevisionConflict) || errors.Is(err, errs.ErrControlOperationConflict) {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	http.Error(w, err.Error(), http.StatusBadRequest)
}

func (h *handler) executeWrite(
	w http.ResponseWriter,
	consistency lsm.WriteConsistency,
	operation string,
	key []byte,
	apply func() error,
) {
	if h.requests == nil {
		http.Error(w, "write tracker unavailable", http.StatusServiceUnavailable)
		return
	}
	status := h.requests.New(operation, consistency)
	if consistency == lsm.WriteConsistencyAccepted {
		go h.executeAccepted(status.RequestID, apply)
		writeJSON(w, http.StatusAccepted, status)
		return
	}
	if err := apply(); err != nil {
		h.requests.Reject(status.RequestID, err)
		h.writeWriteError(w, key, err)
		return
	}
	final := h.requests.Commit(status.RequestID)
	writeJSON(w, http.StatusOK, final)
}

func (h *handler) executeAccepted(requestID string, apply func() error) {
	if err := apply(); err != nil {
		h.requests.Reject(requestID, err)
		return
	}
	h.requests.Commit(requestID)
}

func (h *handler) transferLeader(shardID string, req targetRequest) error {
	opts := req.controlWriteOptions()
	if h.controlWithOptions != nil {
		return h.controlWithOptions.TransferLeaderWithOptions(shardID, req.Target, opts)
	}
	if req.hasControlWriteOptions() {
		return errs.ErrControlWriteOptionsUnsupported
	}
	return h.control.TransferLeader(shardID, req.Target)
}

func (h *handler) rebalance(shardID string, req targetRequest) error {
	opts := req.controlWriteOptions()
	if h.controlWithOptions != nil {
		return h.controlWithOptions.TriggerRebalanceWithOptions(shardID, req.Target, opts)
	}
	if req.hasControlWriteOptions() {
		return errs.ErrControlWriteOptionsUnsupported
	}
	return h.control.TriggerRebalance(shardID, req.Target)
}

func (h *handler) split(shardID string, splitKey []byte, opts lsm.ControlWriteOptions) error {
	if h.controlWithOptions != nil {
		return h.controlWithOptions.TriggerSplitWithOptions(shardID, splitKey, opts)
	}
	if strings.TrimSpace(opts.OperationID) != "" || opts.ExpectedRevision != nil {
		return errs.ErrControlWriteOptionsUnsupported
	}
	return h.control.TriggerSplit(shardID, splitKey)
}

func (h *handler) drain(nodeID string, opts lsm.ControlWriteOptions) error {
	if h.controlWithOptions != nil {
		return h.controlWithOptions.PrepareDrainWithOptions(nodeID, opts)
	}
	if strings.TrimSpace(opts.OperationID) != "" || opts.ExpectedRevision != nil {
		return errs.ErrControlWriteOptionsUnsupported
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

func writeStatusPath(path string) (requestID string, ok bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 3 {
		return "", false
	}
	if parts[0] != "kv" || parts[1] != "write-status" || parts[2] == "" {
		return "", false
	}
	return parts[2], true
}

func normalizeWriteConsistency(
	mode lsm.WriteConsistency,
	defaultConsistency lsm.WriteConsistency,
) (lsm.WriteConsistency, error) {
	trimmed := strings.TrimSpace(string(mode))
	if trimmed == "" {
		if defaultConsistency == "" {
			return lsm.WriteConsistencyAccepted, nil
		}
		return defaultConsistency, nil
	}
	switch lsm.WriteConsistency(trimmed) {
	case lsm.WriteConsistencyAccepted:
		return lsm.WriteConsistencyAccepted, nil
	case lsm.WriteConsistencyLocalCommitted:
		return lsm.WriteConsistencyLocalCommitted, nil
	default:
		return "", fmt.Errorf("invalid consistency %q", trimmed)
	}
}

func writeErrorHTTPStatus(err error) int {
	switch {
	case errors.Is(err, errs.ErrNotLeader):
		return http.StatusConflict
	case errors.Is(err, errs.ErrShardNotFound):
		return http.StatusNotFound
	case errors.Is(err, errs.ErrBackpressure):
		return http.StatusTooManyRequests
	case errors.Is(err, errs.ErrClosed):
		return http.StatusServiceUnavailable
	default:
		return http.StatusBadRequest
	}
}

type writeErrorResponse struct {
	Error     string          `json:"error"`
	Code      string          `json:"code"`
	Retryable bool            `json:"retryable"`
	Route     *writeRouteHint `json:"route,omitempty"`
}

type writeRouteHint struct {
	Revision uint64 `json:"revision"`
	ShardID  string `json:"shard_id,omitempty"`
	Leader   string `json:"leader,omitempty"`
}

func (h *handler) writeWriteError(w http.ResponseWriter, key []byte, err error) {
	status := writeErrorHTTPStatus(err)
	payload := writeErrorResponse{
		Error:     err.Error(),
		Code:      writeErrorCode(err),
		Retryable: isRetryableWriteError(err),
	}
	if payload.Retryable {
		if hint := h.routeHintForKey(key); hint != nil {
			payload.Route = hint
		}
	}
	writeJSON(w, status, payload)
}

func writeErrorCode(err error) string {
	switch {
	case errors.Is(err, errs.ErrNotLeader):
		return "not_leader"
	case errors.Is(err, errs.ErrShardNotFound):
		return "shard_not_found"
	case errors.Is(err, errs.ErrBackpressure):
		return "backpressure"
	case errors.Is(err, errs.ErrClosed):
		return "closed"
	default:
		return "bad_request"
	}
}

func isRetryableWriteError(err error) bool {
	return errors.Is(err, errs.ErrNotLeader) ||
		errors.Is(err, errs.ErrShardNotFound) ||
		errors.Is(err, errs.ErrBackpressure)
}

func (h *handler) routeHintForKey(key []byte) *writeRouteHint {
	if h.control == nil {
		return nil
	}
	status := h.control.ClusterStatus()
	hint := &writeRouteHint{Revision: status.Revision}
	shard, ok := findRouteShardByKey(h.control.Shards(), key)
	if !ok {
		return hint
	}
	hint.ShardID = shard.ID
	hint.Leader = shard.Leader
	return hint
}

func findRouteShardByKey(shards []lsm.ShardStatus, key []byte) (lsm.ShardStatus, bool) {
	for _, shard := range shards {
		if len(shard.StartKey) > 0 && bytes.Compare(key, shard.StartKey) < 0 {
			continue
		}
		if len(shard.EndKey) > 0 && bytes.Compare(key, shard.EndKey) >= 0 {
			continue
		}
		return shard, true
	}
	return lsm.ShardStatus{}, false
}

type writeRequestStore struct {
	mu        sync.Mutex
	max       int
	seq       atomic.Uint64
	order     []string
	statusMap map[string]lsm.WriteRequestStatus
}

func newWriteRequestStore(max int) *writeRequestStore {
	if max <= 0 {
		max = defaultWriteRequestCapacity
	}
	return &writeRequestStore{
		max:       max,
		order:     make([]string, 0, max),
		statusMap: make(map[string]lsm.WriteRequestStatus, max),
	}
}

func (s *writeRequestStore) New(operation string, consistency lsm.WriteConsistency) lsm.WriteRequestStatus {
	now := time.Now().UTC()
	requestID := fmt.Sprintf("wr-%d", s.seq.Add(1))
	status := lsm.WriteRequestStatus{
		RequestID:   requestID,
		Operation:   operation,
		Consistency: consistency,
		State:       lsm.WriteRequestPending,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statusMap[requestID] = status
	s.order = append(s.order, requestID)
	s.compactLocked()
	return status
}

func (s *writeRequestStore) Commit(requestID string) lsm.WriteRequestStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := s.statusMap[requestID]
	status.State = lsm.WriteRequestCommitted
	status.Error = ""
	status.UpdatedAt = time.Now().UTC()
	s.statusMap[requestID] = status
	return status
}

func (s *writeRequestStore) Reject(requestID string, err error) lsm.WriteRequestStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := s.statusMap[requestID]
	status.State = lsm.WriteRequestRejected
	status.Error = err.Error()
	status.UpdatedAt = time.Now().UTC()
	s.statusMap[requestID] = status
	return status
}

func (s *writeRequestStore) Get(requestID string) (lsm.WriteRequestStatus, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status, ok := s.statusMap[requestID]
	return status, ok
}

func (s *writeRequestStore) compactLocked() {
	if len(s.order) <= s.max {
		return
	}
	drop := len(s.order) - s.max
	kept := make([]string, 0, len(s.order))
	for _, requestID := range s.order {
		status, ok := s.statusMap[requestID]
		if !ok {
			continue
		}
		if drop > 0 && status.State != lsm.WriteRequestPending {
			delete(s.statusMap, requestID)
			drop--
			continue
		}
		kept = append(kept, requestID)
	}
	s.order = kept
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
