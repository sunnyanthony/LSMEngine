package server

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"lsmengine/pkg/lsm"
)

// NewGatewayHandler returns an HTTP handler that exposes route-aware writes and
// best-effort cluster read fallback through one gateway endpoint.
func NewGatewayHandler(gateway *Gateway, opts HandlerOptions) http.Handler {
	resolved := resolveHandlerOptions(opts)
	mux := http.NewServeMux()
	handler := &gatewayHandler{
		gateway:                 gateway,
		writeConsistencyDefault: resolved.writeConsistencyDefault,
	}
	mux.HandleFunc("/healthz", handler.handleHealth)
	mux.HandleFunc("/gateway/status", handler.handleGatewayStatus)
	mux.HandleFunc("/kv/get", handler.handleGet)
	mux.HandleFunc("/kv/range", handler.handleRange)
	mux.HandleFunc("/kv/write-status/", handler.handleWriteStatus)
	mux.HandleFunc("/kv/put", handler.handlePut)
	mux.HandleFunc("/kv/delete", handler.handleDelete)
	return mux
}

type gatewayHandler struct {
	gateway                 *Gateway
	writeConsistencyDefault lsm.WriteConsistency
}

func (h *gatewayHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.gateway == nil {
		writeJSON(w, http.StatusServiceUnavailable, lsm.Health{
			Ready:  false,
			Reason: "gateway_unavailable",
		})
		return
	}
	writeJSON(w, http.StatusOK, lsm.Health{Ready: true})
}

func (h *gatewayHandler) handleGatewayStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.gateway == nil {
		writeJSON(w, http.StatusServiceUnavailable, GatewayClusterStatus{
			Ready:  false,
			Reason: "gateway_unavailable",
		})
		return
	}
	status, err := h.gateway.ClusterStatus(r.Context())
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, status)
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (h *gatewayHandler) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	h.proxyClusterRead(w, r)
}

func (h *gatewayHandler) handleRange(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	h.proxyClusterRead(w, r)
}

func (h *gatewayHandler) handleWriteStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	h.proxyClusterRead(w, r)
}

func (h *gatewayHandler) handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.gateway == nil {
		writeGatewayUnavailable(w, "gateway unavailable")
		return
	}
	var req putRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, writeErrorResponse{
			Error: "invalid put request",
			Code:  "bad_request",
		})
		return
	}
	if req.Consistency == "" {
		req.Consistency = h.writeConsistencyDefault
	}
	consistency, err := normalizeWriteConsistency(req.Consistency, h.writeConsistencyDefault)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, writeErrorResponse{Error: err.Error(), Code: "bad_request"})
		return
	}
	key, err := base64.StdEncoding.DecodeString(req.KeyBase64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, writeErrorResponse{Error: "invalid key_base64", Code: "bad_request"})
		return
	}
	value, err := base64.StdEncoding.DecodeString(req.ValueBase64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, writeErrorResponse{Error: "invalid value_base64", Code: "bad_request"})
		return
	}
	status, err := h.gateway.Put(r.Context(), key, value, consistency)
	h.writeGatewayResult(w, status, err)
}

func (h *gatewayHandler) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.gateway == nil {
		writeGatewayUnavailable(w, "gateway unavailable")
		return
	}
	var req deleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, writeErrorResponse{
			Error: "invalid delete request",
			Code:  "bad_request",
		})
		return
	}
	if req.Consistency == "" {
		req.Consistency = h.writeConsistencyDefault
	}
	consistency, err := normalizeWriteConsistency(req.Consistency, h.writeConsistencyDefault)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, writeErrorResponse{Error: err.Error(), Code: "bad_request"})
		return
	}
	key, err := base64.StdEncoding.DecodeString(req.KeyBase64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, writeErrorResponse{Error: "invalid key_base64", Code: "bad_request"})
		return
	}
	status, err := h.gateway.Delete(r.Context(), key, consistency)
	h.writeGatewayResult(w, status, err)
}

func (h *gatewayHandler) writeGatewayResult(w http.ResponseWriter, status lsm.WriteRequestStatus, err error) {
	if err == nil {
		httpStatus := http.StatusOK
		if status.Consistency == lsm.WriteConsistencyAccepted {
			httpStatus = http.StatusAccepted
		}
		writeJSON(w, httpStatus, status)
		return
	}
	var reqErr *WriteRequestError
	if errors.As(err, &reqErr) {
		writeJSON(w, reqErr.Status, reqErr.Response)
		return
	}
	writeJSON(w, http.StatusServiceUnavailable, writeErrorResponse{
		Error:     err.Error(),
		Code:      "gateway_unavailable",
		Retryable: true,
	})
}

func (h *gatewayHandler) proxyClusterRead(w http.ResponseWriter, r *http.Request) {
	if h.gateway == nil {
		writeGatewayUnavailable(w, "gateway unavailable")
		return
	}
	endpoints, err := h.gateway.endpointResolver.ResolveNodeEndpoints(r.Context())
	if err != nil {
		writeGatewayUnavailable(w, err.Error())
		return
	}
	nodeIDs := sortedNodeEndpointIDs(endpoints)
	if r.URL.Path == "/kv/get" {
		h.proxyClusterGet(w, r, endpoints, nodeIDs)
		return
	}
	writeStatusLookup := isWriteStatusRequest(r)
	var firstNotFoundHeader http.Header
	var firstNotFoundBody []byte
	var lastErr error
	for _, nodeID := range nodeIDs {
		endpoint := endpoints[nodeID]
		req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, endpoint+r.URL.RequestURI(), nil)
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := h.gateway.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if writeStatusLookup && resp.StatusCode == http.StatusNotFound {
			if firstNotFoundHeader == nil {
				firstNotFoundHeader = resp.Header.Clone()
				firstNotFoundBody, _ = io.ReadAll(resp.Body)
			}
			lastErr = fmt.Errorf("node %q returned status %d", nodeID, resp.StatusCode)
			_ = resp.Body.Close()
			continue
		}
		if resp.StatusCode == http.StatusOK || resp.StatusCode < http.StatusInternalServerError {
			copyResponse(w, resp)
			return
		}
		lastErr = fmt.Errorf("node %q returned status %d", nodeID, resp.StatusCode)
		_ = resp.Body.Close()
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no node endpoints available")
	}
	if writeStatusLookup && firstNotFoundHeader != nil {
		writeCapturedResponse(w, http.StatusNotFound, firstNotFoundHeader, firstNotFoundBody)
		return
	}
	writeGatewayUnavailable(w, lastErr.Error())
}

func (h *gatewayHandler) proxyClusterGet(w http.ResponseWriter, r *http.Request, endpoints map[string]string, nodeIDs []string) {
	var firstNotFound *getResponse
	var lastErr error
	for _, nodeID := range nodeIDs {
		endpoint := endpoints[nodeID]
		req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, endpoint+r.URL.RequestURI(), nil)
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := h.gateway.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		switch {
		case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound:
			var out getResponse
			if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
				lastErr = err
				_ = resp.Body.Close()
				continue
			}
			_ = resp.Body.Close()
			if out.Found {
				writeJSON(w, http.StatusOK, out)
				return
			}
			if firstNotFound == nil {
				copy := out
				firstNotFound = &copy
			}
		case resp.StatusCode < http.StatusInternalServerError:
			copyResponse(w, resp)
			return
		default:
			lastErr = fmt.Errorf("node %q returned status %d", nodeID, resp.StatusCode)
			_ = resp.Body.Close()
		}
	}
	if firstNotFound != nil {
		writeJSON(w, http.StatusNotFound, *firstNotFound)
		return
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no node endpoints available")
	}
	writeGatewayUnavailable(w, lastErr.Error())
}

func writeGatewayUnavailable(w http.ResponseWriter, msg string) {
	writeJSON(w, http.StatusServiceUnavailable, writeErrorResponse{
		Error:     msg,
		Code:      "gateway_unavailable",
		Retryable: true,
	})
}

func isWriteStatusRequest(r *http.Request) bool {
	return r != nil && r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/kv/write-status/")
}

func sortedNodeEndpointIDs(endpoints map[string]string) []string {
	nodeIDs := make([]string, 0, len(endpoints))
	for nodeID, endpoint := range endpoints {
		if nodeID == "" || endpoint == "" {
			continue
		}
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)
	return nodeIDs
}

func copyResponse(w http.ResponseWriter, resp *http.Response) {
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	writeCapturedResponse(w, resp.StatusCode, resp.Header, body)
}

func writeCapturedResponse(w http.ResponseWriter, status int, header http.Header, body []byte) {
	for key, values := range header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
