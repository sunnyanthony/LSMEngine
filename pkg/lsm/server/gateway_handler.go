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
// configured cluster read proxying through one gateway endpoint.
func NewGatewayHandler(gateway *Gateway, opts HandlerOptions) http.Handler {
	resolved := resolveHandlerOptions(opts)
	mux := http.NewServeMux()
	handler := &gatewayHandler{
		gateway:                 gateway,
		writeConsistencyDefault: resolved.writeConsistencyDefault,
	}
	mux.HandleFunc("/healthz", handler.handleHealth)
	mux.HandleFunc("/readyz", handler.handleReady)
	mux.HandleFunc("/gateway/status", handler.handleGatewayStatus)
	mux.HandleFunc("/gateway/metrics", handler.handleGatewayMetrics)
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

func (h *gatewayHandler) handleReady(w http.ResponseWriter, r *http.Request) {
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
	status, err := h.gateway.ClusterStatus(r.Context())
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, lsm.Health{
			Ready:  false,
			Reason: status.Reason,
		})
		return
	}
	if strings.TrimSpace(status.WriteLeader) == "" {
		writeJSON(w, http.StatusServiceUnavailable, lsm.Health{
			Ready:  false,
			Reason: "write_leader_unavailable",
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

func (h *gatewayHandler) handleGatewayMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if h.gateway == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		writeGatewayMetrics(w, GatewayClusterStatus{Reason: "gateway_unavailable"})
		return
	}
	status, err := h.gateway.ClusterStatus(r.Context())
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	writeGatewayMetrics(w, status)
}

func writeGatewayMetrics(w io.Writer, status GatewayClusterStatus) {
	writeMetricHelp(w, "lsm_gateway_ready", "Whether this gateway currently sees a usable backend view.")
	writeMetricGauge(w, "lsm_gateway_ready", boolMetric(status.Ready))
	writeMetricHelp(w, "lsm_gateway_node_count", "Backend node count known to this gateway.")
	writeMetricGauge(w, "lsm_gateway_node_count", float64(status.NodeCount))
	writeMetricHelp(w, "lsm_gateway_reachable_nodes", "Backend nodes reachable during the latest gateway status sample.")
	writeMetricGauge(w, "lsm_gateway_reachable_nodes", float64(status.ReachableNodes))
	writeMetricHelp(w, "lsm_gateway_write_leader_known", "Whether the latest gateway status sample found a backend write leader.")
	writeMetricGauge(w, "lsm_gateway_write_leader_known", boolMetric(strings.TrimSpace(status.WriteLeader) != ""))

	writeMetricHelp(w, "lsm_gateway_routing_write_attempts_total", "Process-local gateway write backend attempts.")
	writeMetricCounter(w, "lsm_gateway_routing_write_attempts_total", status.Routing.WriteAttempts)
	writeMetricHelp(w, "lsm_gateway_routing_write_retries_total", "Process-local gateway retryable write attempts.")
	writeMetricCounter(w, "lsm_gateway_routing_write_retries_total", status.Routing.WriteRetries)
	writeMetricHelp(w, "lsm_gateway_routing_write_failures_total", "Process-local gateway writes that exhausted routing attempts.")
	writeMetricCounter(w, "lsm_gateway_routing_write_failures_total", status.Routing.WriteFailures)
	writeMetricHelp(w, "lsm_gateway_routing_read_attempts_total", "Process-local gateway read backend attempts.")
	writeMetricCounter(w, "lsm_gateway_routing_read_attempts_total", status.Routing.ReadAttempts)
	writeMetricHelp(w, "lsm_gateway_routing_read_fallbacks_total", "Process-local gateway read attempts against a second-or-later backend target.")
	writeMetricCounter(w, "lsm_gateway_routing_read_fallbacks_total", status.Routing.ReadFallbacks)
	writeMetricHelp(w, "lsm_gateway_routing_read_failures_total", "Process-local gateway read requests that exhausted all backend targets.")
	writeMetricCounter(w, "lsm_gateway_routing_read_failures_total", status.Routing.ReadFailures)
	writeMetricHelp(w, "lsm_gateway_routing_route_refreshes_total", "Process-local gateway route refresh attempts.")
	writeMetricCounter(w, "lsm_gateway_routing_route_refreshes_total", status.Routing.RouteRefreshes)
	writeMetricHelp(w, "lsm_gateway_routing_route_refresh_failures_total", "Process-local gateway route refresh failures.")
	writeMetricCounter(w, "lsm_gateway_routing_route_refresh_failures_total", status.Routing.RouteRefreshFailures)
	writeMetricHelp(w, "lsm_gateway_routing_route_hint_updates_total", "Process-local gateway route hint updates applied from write errors.")
	writeMetricCounter(w, "lsm_gateway_routing_route_hint_updates_total", status.Routing.RouteHintUpdates)

	writeMetricHelp(w, "lsm_gateway_backend_up", "Whether a backend node was reachable during the latest gateway status sample.")
	writeMetricType(w, "lsm_gateway_backend_up", "gauge")
	writeMetricHelp(w, "lsm_gateway_backend_degraded", "Whether a backend endpoint is temporarily deferred behind healthy endpoints.")
	writeMetricType(w, "lsm_gateway_backend_degraded", "gauge")
	writeMetricHelp(w, "lsm_gateway_backend_write_available", "Whether a backend reports commit-log writes are available.")
	writeMetricType(w, "lsm_gateway_backend_write_available", "gauge")
	writeMetricHelp(w, "lsm_gateway_backend_apply_lag", "Backend-reported commit-log apply lag.")
	writeMetricType(w, "lsm_gateway_backend_apply_lag", "gauge")
	for _, node := range status.Nodes {
		labels := `node="` + metricLabelValue(node.Node) + `"`
		writeMetricGaugeWithLabels(w, "lsm_gateway_backend_up", labels, boolMetric(node.OK))
		writeMetricGaugeWithLabels(w, "lsm_gateway_backend_degraded", labels, boolMetric(node.Degraded))
		writeAvailable := false
		applyLag := uint64(0)
		if node.Status != nil {
			writeAvailable = node.Status.CommitLogRuntime.WriteAvailable
			applyLag = node.Status.CommitLogRuntime.ApplyLag
		}
		writeMetricGaugeWithLabels(w, "lsm_gateway_backend_write_available", labels, boolMetric(writeAvailable))
		writeMetricGaugeWithLabels(w, "lsm_gateway_backend_apply_lag", labels, float64(applyLag))
	}
}

func writeMetricHelp(w io.Writer, name string, help string) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
}

func writeMetricType(w io.Writer, name string, metricType string) {
	fmt.Fprintf(w, "# TYPE %s %s\n", name, metricType)
}

func writeMetricGauge(w io.Writer, name string, value float64) {
	writeMetricType(w, name, "gauge")
	fmt.Fprintf(w, "%s %g\n", name, value)
}

func writeMetricGaugeWithLabels(w io.Writer, name string, labels string, value float64) {
	fmt.Fprintf(w, "%s{%s} %g\n", name, labels, value)
}

func writeMetricCounter(w io.Writer, name string, value uint64) {
	writeMetricType(w, name, "counter")
	fmt.Fprintf(w, "%s %d\n", name, value)
}

func boolMetric(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func metricLabelValue(value string) string {
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`).Replace(value)
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
		h.gateway.routing.readFailures.Add(1)
		writeGatewayUnavailable(w, err.Error())
		return
	}
	targets, err := h.gateway.readTargets(r.Context(), endpoints, isKVReadRequest(r))
	if err != nil {
		h.gateway.routing.readFailures.Add(1)
		writeJSON(w, http.StatusServiceUnavailable, writeErrorResponse{
			Error:     err.Error(),
			Code:      "gateway_unavailable",
			Retryable: true,
		})
		return
	}
	if r.URL.Path == "/kv/get" {
		h.proxyClusterGet(w, r, targets)
		return
	}
	writeStatusLookup := isWriteStatusRequest(r)
	var firstNotFoundHeader http.Header
	var firstNotFoundBody []byte
	var lastErr error
	for i, target := range targets {
		h.gateway.routing.readAttempts.Add(1)
		if i > 0 {
			h.gateway.routing.readFallbacks.Add(1)
		}
		nodeID := target.nodeID
		endpoint := target.endpoint
		req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, endpoint+r.URL.RequestURI(), nil)
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := h.gateway.client.Do(req)
		if err != nil {
			h.gateway.markEndpointFailure(endpoint)
			lastErr = err
			continue
		}
		if writeStatusLookup && resp.StatusCode == http.StatusNotFound {
			if firstNotFoundHeader == nil {
				firstNotFoundHeader = resp.Header.Clone()
				firstNotFoundBody, _ = io.ReadAll(resp.Body)
			}
			h.gateway.markEndpointSuccess(endpoint)
			lastErr = fmt.Errorf("node %q returned status %d", nodeID, resp.StatusCode)
			_ = resp.Body.Close()
			continue
		}
		if resp.StatusCode == http.StatusOK || resp.StatusCode < http.StatusInternalServerError {
			h.gateway.markEndpointSuccess(endpoint)
			copyResponse(w, resp)
			return
		}
		h.gateway.markEndpointFailure(endpoint)
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
	h.gateway.routing.readFailures.Add(1)
	writeGatewayUnavailable(w, lastErr.Error())
}

func (h *gatewayHandler) proxyClusterGet(w http.ResponseWriter, r *http.Request, targets []gatewayReadTarget) {
	var firstNotFound *getResponse
	var lastErr error
	for i, target := range targets {
		h.gateway.routing.readAttempts.Add(1)
		if i > 0 {
			h.gateway.routing.readFallbacks.Add(1)
		}
		nodeID := target.nodeID
		endpoint := target.endpoint
		req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, endpoint+r.URL.RequestURI(), nil)
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := h.gateway.client.Do(req)
		if err != nil {
			h.gateway.markEndpointFailure(endpoint)
			lastErr = err
			continue
		}
		switch {
		case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound:
			h.gateway.markEndpointSuccess(endpoint)
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
			h.gateway.markEndpointSuccess(endpoint)
			copyResponse(w, resp)
			return
		default:
			h.gateway.markEndpointFailure(endpoint)
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
	h.gateway.routing.readFailures.Add(1)
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

func isKVReadRequest(r *http.Request) bool {
	return r != nil && r.Method == http.MethodGet && (r.URL.Path == "/kv/get" || r.URL.Path == "/kv/range")
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
