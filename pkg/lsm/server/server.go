// HTTP monitoring handlers for stats and health.

package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
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
	WriteConsistencyDefault     lsm.WriteConsistency
	MaxWriteRequests            int
	GatewayReadyMinReachable    int
	GatewayReadyMaxReadApplyLag *uint64
	GatewayReadyMinReadReady    int
}

type writeSeqProvider interface {
	PutWithSeq(key []byte, value []byte) (uint64, error)
	DeleteWithSeq(key []byte) (uint64, error)
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
	if resume, ok := provider.(lsm.ControlResumeProvider); ok {
		handler.controlResume = resume
		if advanced, ok := provider.(lsm.ControlResumeProviderWithOptions); ok {
			handler.controlResumeWithOptions = advanced
		}
	}
	if membership, ok := provider.(lsm.RaftMembershipProvider); ok {
		handler.raftMembership = membership
	}
	if writer, ok := provider.(lsm.WriteProvider); ok {
		handler.writer = writer
		handler.requests = newWriteRequestStore(resolved.maxWriteRequests)
		handler.writeConsistencyDefault = resolved.writeConsistencyDefault
	}
	if reader, ok := provider.(lsm.ReadProvider); ok {
		handler.reader = reader
	}
	if ranger, ok := provider.(lsm.RangeProvider); ok {
		handler.ranger = ranger
	}
	if cdc, ok := provider.(lsm.CDCProvider); ok {
		handler.cdc = cdc
	}
	if compaction, ok := provider.(lsm.CompactionProvider); ok {
		handler.compaction = compaction
	}
	if raft, ok := provider.(raftPeerMessageHandler); ok {
		handler.raft = raft
	}
	mux.HandleFunc("/healthz", handler.handleHealth)
	mux.HandleFunc("/stats", handler.handleStats)
	mux.HandleFunc("/metrics", handler.handleMetrics)
	mux.HandleFunc("/compact", handler.handleCompact)
	mux.HandleFunc("/cluster/status", handler.handleClusterStatus)
	mux.HandleFunc("/cluster/shards", handler.handleShards)
	mux.HandleFunc("/cluster/routes", handler.handleRoutes)
	mux.HandleFunc(RaftPeerMessagesPath, handler.handleRaftPeerMessages)
	mux.HandleFunc("/cdc/events", handler.handleCDCEvents)
	mux.HandleFunc("/cluster/shards/", handler.handleShardAction)
	mux.HandleFunc("/cluster/nodes/", handler.handleNodeAction)
	mux.HandleFunc("/kv/get", handler.handleGet)
	mux.HandleFunc("/kv/range", handler.handleRange)
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
	provider                 lsm.StatsProvider
	control                  lsm.ControlProvider
	controlWithOptions       lsm.ControlProviderWithOptions
	controlResume            lsm.ControlResumeProvider
	controlResumeWithOptions lsm.ControlResumeProviderWithOptions
	raftMembership           lsm.RaftMembershipProvider
	reader                   lsm.ReadProvider
	ranger                   lsm.RangeProvider
	writer                   lsm.WriteProvider
	cdc                      lsm.CDCProvider
	compaction               lsm.CompactionProvider
	raft                     raftPeerMessageHandler
	requests                 *writeRequestStore
	writeConsistencyDefault  lsm.WriteConsistency
}

type raftPeerMessageHandler interface {
	HandlePeerMessages(ctx context.Context, messages []lsm.RaftPeerMessage) error
}

const defaultWriteRequestCapacity = 4096
const defaultRangeLimit = 100
const maxRangeLimit = 1000

type resolvedHandlerOptions struct {
	writeConsistencyDefault     lsm.WriteConsistency
	maxWriteRequests            int
	gatewayReadyMinReachable    int
	gatewayReadyMaxReadApplyLag *uint64
	gatewayReadyMinReadReady    int
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
	minReachable := opts.GatewayReadyMinReachable
	if minReachable < 0 {
		minReachable = 0
	}
	minReadReady := opts.GatewayReadyMinReadReady
	if minReadReady < 0 {
		minReadReady = 0
	}
	return resolvedHandlerOptions{
		writeConsistencyDefault:     consistency,
		maxWriteRequests:            max,
		gatewayReadyMinReachable:    minReachable,
		gatewayReadyMaxReadApplyLag: cloneUint64Ptr(opts.GatewayReadyMaxReadApplyLag),
		gatewayReadyMinReadReady:    minReadReady,
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

func (h *handler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if h.provider == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		writeServerMetrics(w, lsm.Stats{})
		return
	}
	writeServerMetrics(w, h.provider.Stats())
}

func (h *handler) handleCompact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.compaction == nil {
		http.Error(w, "compaction unavailable", http.StatusServiceUnavailable)
		return
	}
	writeActionResult(w, h.compaction.TriggerCompaction())
}

func writeServerMetrics(w io.Writer, stats lsm.Stats) {
	writeMetricHelp(w, "lsm_engine_memtable_bytes", "Current mutable memtable bytes.")
	writeMetricGauge(w, "lsm_engine_memtable_bytes", float64(stats.MemtableBytes))
	writeMetricHelp(w, "lsm_engine_memtable_entries", "Current mutable memtable entries.")
	writeMetricGauge(w, "lsm_engine_memtable_entries", float64(stats.MemtableEntries))
	writeMetricHelp(w, "lsm_engine_immutable_memtables", "Immutable memtables queued or being flushed.")
	writeMetricGauge(w, "lsm_engine_immutable_memtables", float64(stats.ImmutableCount))
	writeMetricHelp(w, "lsm_engine_flush_queue_depth", "Current flush queue depth.")
	writeMetricGauge(w, "lsm_engine_flush_queue_depth", float64(stats.FlushQueueDepth))
	writeMetricHelp(w, "lsm_engine_flush_queue_capacity", "Configured flush queue capacity.")
	writeMetricGauge(w, "lsm_engine_flush_queue_capacity", float64(stats.FlushQueueCapacity))
	writeMetricHelp(w, "lsm_engine_sstable_count", "Current SSTable count.")
	writeMetricGauge(w, "lsm_engine_sstable_count", float64(stats.SSTableCount))
	writeMetricHelp(w, "lsm_engine_sstable_bytes", "Current SSTable bytes.")
	writeMetricGauge(w, "lsm_engine_sstable_bytes", float64(stats.SSTableBytes))
	writeMetricHelp(w, "lsm_engine_l0_sstable_count", "Current level-0 SSTable count.")
	writeMetricGauge(w, "lsm_engine_l0_sstable_count", float64(stats.L0TableCount))
	writeMetricHelp(w, "lsm_engine_l0_bytes", "Current level-0 SSTable bytes.")
	writeMetricGauge(w, "lsm_engine_l0_bytes", float64(stats.L0SizeBytes))
	writeMetricHelp(w, "lsm_engine_compaction_l0_threshold", "Configured L0 SSTable count threshold that marks compaction pending.")
	writeMetricGauge(w, "lsm_engine_compaction_l0_threshold", float64(stats.CompactionL0Threshold))
	writeMetricHelp(w, "lsm_engine_compaction_check_interval_seconds", "Configured periodic compaction runtime wake interval in seconds.")
	writeMetricGauge(w, "lsm_engine_compaction_check_interval_seconds", float64(stats.CompactionCheckIntervalMS)/1000)
	writeMetricHelp(w, "lsm_engine_compaction_pending", "Whether L0 has reached the configured compaction threshold.")
	writeMetricGauge(w, "lsm_engine_compaction_pending", boolMetric(stats.CompactionPending))

	writeMetricHelp(w, "lsm_engine_sstable_level_count", "Current SSTable count by level.")
	writeMetricType(w, "lsm_engine_sstable_level_count", "gauge")
	writeMetricHelp(w, "lsm_engine_sstable_level_bytes", "Current SSTable bytes by level.")
	writeMetricType(w, "lsm_engine_sstable_level_bytes", "gauge")
	for _, level := range stats.SSTableLevels {
		labels := fmt.Sprintf(`level="%d"`, level.Level)
		writeMetricGaugeWithLabels(w, "lsm_engine_sstable_level_count", labels, float64(level.TableCount))
		writeMetricGaugeWithLabels(w, "lsm_engine_sstable_level_bytes", labels, float64(level.SizeBytes))
	}

	writeMetricHelp(w, "lsm_engine_point_reads_total", "Process-local point reads.")
	writeMetricCounter(w, "lsm_engine_point_reads_total", stats.PointReads)
	writeMetricHelp(w, "lsm_engine_point_read_memtable_hits_total", "Process-local point read memtable hits.")
	writeMetricCounter(w, "lsm_engine_point_read_memtable_hits_total", stats.PointReadMemtableHits)
	writeMetricHelp(w, "lsm_engine_point_read_immutable_hits_total", "Process-local point read immutable memtable hits.")
	writeMetricCounter(w, "lsm_engine_point_read_immutable_hits_total", stats.PointReadImmutableHits)
	writeMetricHelp(w, "lsm_engine_point_read_sstable_hits_total", "Process-local point read SSTable hits.")
	writeMetricCounter(w, "lsm_engine_point_read_sstable_hits_total", stats.PointReadSSTableHits)
	writeMetricHelp(w, "lsm_engine_point_read_misses_total", "Process-local point read misses.")
	writeMetricCounter(w, "lsm_engine_point_read_misses_total", stats.PointReadMisses)
	writeMetricHelp(w, "lsm_engine_point_read_sstable_probes_total", "Process-local SSTable probes during point reads.")
	writeMetricCounter(w, "lsm_engine_point_read_sstable_probes_total", stats.PointReadSSTableProbes)
	writeMetricHelp(w, "lsm_engine_point_read_max_sstable_probes", "Maximum SSTable probes observed in one point read.")
	writeMetricGauge(w, "lsm_engine_point_read_max_sstable_probes", float64(stats.PointReadMaxSSTableProbes))
	writeMetricHelp(w, "lsm_engine_sstable_flow_cache_hits_total", "Process-local SSTable block cache hits.")
	writeMetricCounter(w, "lsm_engine_sstable_flow_cache_hits_total", stats.SSTableFlow.CacheHit)
	writeMetricHelp(w, "lsm_engine_sstable_flow_cache_misses_total", "Process-local SSTable block cache misses.")
	writeMetricCounter(w, "lsm_engine_sstable_flow_cache_misses_total", stats.SSTableFlow.CacheMiss)
	writeMetricHelp(w, "lsm_engine_sstable_flow_filter_pass_total", "Process-local SSTable filter pass observations.")
	writeMetricCounter(w, "lsm_engine_sstable_flow_filter_pass_total", stats.SSTableFlow.FilterPass)
	writeMetricHelp(w, "lsm_engine_sstable_flow_filter_skip_total", "Process-local SSTable filter skip observations.")
	writeMetricCounter(w, "lsm_engine_sstable_flow_filter_skip_total", stats.SSTableFlow.FilterSkip)
	writeMetricHelp(w, "lsm_engine_sstable_flow_errors_total", "Process-local SSTable read-pipeline errors.")
	writeMetricCounter(w, "lsm_engine_sstable_flow_errors_total", stats.SSTableFlow.Errors)

	writeMetricHelp(w, "lsm_engine_compaction_triggers_total", "Process-local compaction triggers.")
	writeMetricCounter(w, "lsm_engine_compaction_triggers_total", stats.CompactionRuntime.Triggers)
	writeMetricHelp(w, "lsm_engine_compaction_coalesced_triggers_total", "Process-local compaction triggers coalesced while a run was already pending.")
	writeMetricCounter(w, "lsm_engine_compaction_coalesced_triggers_total", stats.CompactionRuntime.CoalescedTriggers)
	writeMetricHelp(w, "lsm_engine_compaction_runs_total", "Process-local compaction runs.")
	writeMetricCounter(w, "lsm_engine_compaction_runs_total", stats.CompactionRuntime.Runs)
	writeMetricHelp(w, "lsm_engine_compaction_steps_total", "Process-local compaction steps.")
	writeMetricCounter(w, "lsm_engine_compaction_steps_total", stats.CompactionRuntime.Steps)
	writeMetricHelp(w, "lsm_engine_compaction_successful_steps_total", "Process-local successful compaction steps.")
	writeMetricCounter(w, "lsm_engine_compaction_successful_steps_total", stats.CompactionRuntime.SuccessfulSteps)
	writeMetricHelp(w, "lsm_engine_compaction_errors_total", "Process-local compaction errors.")
	writeMetricCounter(w, "lsm_engine_compaction_errors_total", stats.CompactionRuntime.Errors)
	writeMetricHelp(w, "lsm_engine_compaction_running", "Whether a compaction worker is currently running.")
	writeMetricGauge(w, "lsm_engine_compaction_running", boolMetric(stats.CompactionRuntime.Running))

	writeMetricHelp(w, "lsm_engine_write_backpressure_active", "Whether local write admission backpressure is active.")
	writeMetricGauge(w, "lsm_engine_write_backpressure_active", boolMetric(stats.WriteBackpressure.Active))
	writeMetricHelp(w, "lsm_engine_write_backpressure_rejects_total", "Process-local local writes rejected by write admission backpressure.")
	writeMetricCounter(w, "lsm_engine_write_backpressure_rejects_total", stats.WriteBackpressure.Rejects)
	writeMetricHelp(w, "lsm_engine_write_backpressure_flush_queue_threshold", "Configured immutable flush queue depth threshold for local write backpressure.")
	writeMetricGauge(w, "lsm_engine_write_backpressure_flush_queue_threshold", float64(stats.WriteBackpressure.FlushQueueThreshold))
	writeMetricHelp(w, "lsm_engine_write_backpressure_compaction_l0_threshold", "Configured L0 table count threshold for local write backpressure.")
	writeMetricGauge(w, "lsm_engine_write_backpressure_compaction_l0_threshold", float64(stats.WriteBackpressure.CompactionL0Threshold))
	writeMetricHelp(w, "lsm_engine_write_backpressure_compaction_l0_tables", "L0 table count observed by the write backpressure policy.")
	writeMetricGauge(w, "lsm_engine_write_backpressure_compaction_l0_tables", float64(stats.WriteBackpressure.CompactionL0TableCount))
	writeMetricHelp(w, "lsm_engine_write_backpressure_reason", "Current write backpressure reason as a labeled gauge.")
	writeMetricType(w, "lsm_engine_write_backpressure_reason", "gauge")
	if stats.WriteBackpressure.Reason != "" {
		writeMetricGaugeWithLabels(w, "lsm_engine_write_backpressure_reason", `reason="`+metricLabelValue(stats.WriteBackpressure.Reason)+`"`, 1)
	}

	writeMetricHelp(w, "lsm_engine_wal_segments", "Current WAL segment count.")
	writeMetricGauge(w, "lsm_engine_wal_segments", float64(stats.WAL.SegmentCount))
	writeMetricHelp(w, "lsm_engine_wal_archived_segments", "Current archived WAL segment count.")
	writeMetricGauge(w, "lsm_engine_wal_archived_segments", float64(stats.WAL.ArchivedSegmentCount))
	writeMetricHelp(w, "lsm_engine_wal_bytes", "Current total WAL bytes.")
	writeMetricGauge(w, "lsm_engine_wal_bytes", float64(stats.WAL.TotalBytes))
	writeMetricHelp(w, "lsm_engine_wal_checkpoint_lag", "Current WAL checkpoint lag in sequence numbers.")
	writeMetricGauge(w, "lsm_engine_wal_checkpoint_lag", float64(stats.WAL.CheckpointLag))
	writeMetricHelp(w, "lsm_engine_wal_max_segment_bytes", "Configured maximum WAL segment bytes.")
	writeMetricGauge(w, "lsm_engine_wal_max_segment_bytes", float64(stats.WAL.MaxSegmentBytes))
	writeMetricHelp(w, "lsm_engine_wal_retain_archived_segments", "Configured archived WAL segments to retain after checkpoint pruning.")
	writeMetricGauge(w, "lsm_engine_wal_retain_archived_segments", float64(stats.WAL.RetainArchivedSegments))
	writeMetricHelp(w, "lsm_engine_wal_block_size", "Configured WAL block size.")
	writeMetricGauge(w, "lsm_engine_wal_block_size", float64(stats.WAL.BlockSize))
	writeMetricHelp(w, "lsm_engine_wal_pending_block_records", "Current buffered WAL records not yet flushed as a block.")
	writeMetricGauge(w, "lsm_engine_wal_pending_block_records", float64(stats.WAL.PendingBlockRecords))
	writeMetricHelp(w, "lsm_engine_closed", "Whether the engine is closed.")
	writeMetricGauge(w, "lsm_engine_closed", boolMetric(stats.Closed))
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

type cdcReadResponse struct {
	ShardID       string             `json:"shard_id"`
	FromOffset    uint64             `json:"from_offset"`
	NextOffset    uint64             `json:"next_offset"`
	OldestOffset  uint64             `json:"oldest_offset"`
	DroppedBefore bool               `json:"dropped_before"`
	Events        []cdcEventResponse `json:"events"`
}

type cdcEventResponse struct {
	Offset      uint64    `json:"offset"`
	Operation   string    `json:"operation"`
	KeyBase64   string    `json:"key_base64,omitempty"`
	ValueBase64 string    `json:"value_base64,omitempty"`
	Tombstone   bool      `json:"tombstone,omitempty"`
	CommittedAt time.Time `json:"committed_at"`
}

type raftPeerMessagesRequest struct {
	Messages []lsm.RaftPeerMessage `json:"messages"`
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

func (h *handler) handleCDCEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.cdc == nil {
		http.NotFound(w, r)
		return
	}
	query := r.URL.Query()
	shardID := strings.TrimSpace(query.Get("shard"))
	if shardID == "" && h.control != nil {
		shards := h.control.Shards()
		if len(shards) == 1 {
			shardID = shards[0].ID
		}
	}
	if shardID == "" {
		http.Error(w, "missing shard query parameter", http.StatusBadRequest)
		return
	}
	offset := uint64(0)
	if raw := strings.TrimSpace(query.Get("offset")); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			http.Error(w, "invalid offset", http.StatusBadRequest)
			return
		}
		offset = parsed
	}
	limit := 0
	if raw := strings.TrimSpace(query.Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			http.Error(w, "invalid limit", http.StatusBadRequest)
			return
		}
		limit = parsed
	}
	result, err := h.cdc.ReadCDCEvents(shardID, offset, limit)
	if err != nil {
		if errors.Is(err, errs.ErrShardNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	out := cdcReadResponse{
		ShardID:       result.ShardID,
		FromOffset:    result.FromOffset,
		NextOffset:    result.NextOffset,
		OldestOffset:  result.OldestOffset,
		DroppedBefore: result.DroppedBefore,
		Events:        make([]cdcEventResponse, 0, len(result.Events)),
	}
	for _, event := range result.Events {
		out.Events = append(out.Events, cdcEventResponse{
			Offset:      event.Offset,
			Operation:   event.Operation,
			KeyBase64:   base64.StdEncoding.EncodeToString(event.Key),
			ValueBase64: base64.StdEncoding.EncodeToString(event.Value),
			Tombstone:   event.Tombstone,
			CommittedAt: event.CommittedAt,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *handler) handleRaftPeerMessages(w http.ResponseWriter, r *http.Request) {
	if h.raft == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req raftPeerMessagesRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	if err := h.raft.HandlePeerMessages(r.Context(), req.Messages); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
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
	case "add-replica":
		var req targetRequest
		if !decodeJSONBody(w, r, &req) {
			return
		}
		writeActionResult(w, h.addReplica(shardID, req))
	case "remove-replica":
		var req targetRequest
		if !decodeJSONBody(w, r, &req) {
			return
		}
		writeActionResult(w, h.removeReplica(shardID, req))
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
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	nodeID, action, ok := nodeActionPath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	switch action {
	case "drain":
		if h.control == nil {
			http.NotFound(w, r)
			return
		}
		var req drainRequest
		if !decodeJSONBody(w, r, &req) {
			return
		}
		writeActionResult(w, h.drain(nodeID, req.controlWriteOptions()))
	case "resume":
		if h.controlResume == nil {
			http.NotFound(w, r)
			return
		}
		var req drainRequest
		if !decodeJSONBody(w, r, &req) {
			return
		}
		writeActionResult(w, h.resumeDrain(nodeID, req.controlWriteOptions()))
	case "raft-add":
		if h.raftMembership == nil {
			http.NotFound(w, r)
			return
		}
		var req controlRequestOptions
		if !decodeJSONBody(w, r, &req) {
			return
		}
		if req.hasControlWriteOptions() {
			writeActionResult(w, errs.ErrControlWriteOptionsUnsupported)
			return
		}
		writeActionResult(w, h.raftMembership.AddRaftPeer(nodeID))
	case "raft-remove":
		if h.raftMembership == nil {
			http.NotFound(w, r)
			return
		}
		var req controlRequestOptions
		if !decodeJSONBody(w, r, &req) {
			return
		}
		if req.hasControlWriteOptions() {
			writeActionResult(w, errs.ErrControlWriteOptionsUnsupported)
			return
		}
		writeActionResult(w, h.raftMembership.RemoveRaftPeer(nodeID))
	default:
		http.NotFound(w, r)
	}
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

type getResponse struct {
	Found       bool   `json:"found"`
	KeyBase64   string `json:"key_base64"`
	ValueBase64 string `json:"value_base64"`
	Tombstone   bool   `json:"tombstone,omitempty"`
	Seq         uint64 `json:"seq"`
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

func (h *handler) handleGet(w http.ResponseWriter, r *http.Request) {
	if h.reader == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	keyBase64 := strings.TrimSpace(r.URL.Query().Get("key_base64"))
	if keyBase64 == "" {
		http.Error(w, "key_base64 required", http.StatusBadRequest)
		return
	}
	key, err := base64.StdEncoding.DecodeString(keyBase64)
	if err != nil {
		http.Error(w, "invalid key_base64", http.StatusBadRequest)
		return
	}
	entry, ok := h.reader.Get(key)
	if !ok || entry.Tombstone {
		writeJSON(w, http.StatusNotFound, getResponse{
			Found:     false,
			KeyBase64: base64.StdEncoding.EncodeToString(key),
			Tombstone: entry.Tombstone,
			Seq:       entry.Seq,
		})
		return
	}
	writeJSON(w, http.StatusOK, getResponse{
		Found:       true,
		KeyBase64:   base64.StdEncoding.EncodeToString(key),
		ValueBase64: base64.StdEncoding.EncodeToString(entry.Value),
		Seq:         entry.Seq,
	})
}

func (h *handler) handleRange(w http.ResponseWriter, r *http.Request) {
	if h.ranger == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	start, err := optionalBase64Query(r, "start_key_base64")
	if err != nil {
		http.Error(w, "invalid start_key_base64", http.StatusBadRequest)
		return
	}
	end, err := optionalBase64Query(r, "end_key_base64")
	if err != nil {
		http.Error(w, "invalid end_key_base64", http.StatusBadRequest)
		return
	}
	limit, err := rangeLimit(r.URL.Query().Get("limit"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	snap := h.ranger.Snapshot()
	if snap == nil {
		http.Error(w, "snapshot unavailable", http.StatusServiceUnavailable)
		return
	}
	defer snap.Close()
	iter := snap.Range(start, end)
	entries := make([]rangeEntryResponse, 0, limit)
	truncated := false
	for iter.Next() {
		entry := iter.Entry()
		if len(entries) >= limit {
			truncated = true
			break
		}
		entries = append(entries, rangeEntryResponse{
			KeyBase64:   base64.StdEncoding.EncodeToString(entry.Key),
			ValueBase64: base64.StdEncoding.EncodeToString(entry.Value),
			Tombstone:   entry.Tombstone,
			Seq:         entry.Seq,
		})
	}
	if err := iter.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, rangeResponse{
		Entries:   entries,
		Limit:     limit,
		Truncated: truncated,
	})
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
	h.executeWrite(w, consistency, "put", key, func() (uint64, error) {
		if writer, ok := h.writer.(writeSeqProvider); ok {
			return writer.PutWithSeq(key, value)
		}
		return 0, h.writer.Put(key, value)
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
	h.executeWrite(w, consistency, "delete", key, func() (uint64, error) {
		if writer, ok := h.writer.(writeSeqProvider); ok {
			return writer.DeleteWithSeq(key)
		}
		return 0, h.writer.Delete(key)
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
	apply func() (uint64, error),
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
	seq, err := apply()
	if err != nil {
		h.requests.Reject(status.RequestID, err)
		h.writeWriteError(w, key, err)
		return
	}
	final := h.requests.Commit(status.RequestID, seq)
	writeJSON(w, http.StatusOK, final)
}

func (h *handler) executeAccepted(requestID string, apply func() (uint64, error)) {
	seq, err := apply()
	if err != nil {
		h.requests.Reject(requestID, err)
		return
	}
	h.requests.Commit(requestID, seq)
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

func (h *handler) addReplica(shardID string, req targetRequest) error {
	opts := req.controlWriteOptions()
	if h.controlWithOptions != nil {
		return h.controlWithOptions.AddReplicaWithOptions(shardID, req.Target, opts)
	}
	if req.hasControlWriteOptions() {
		return errs.ErrControlWriteOptionsUnsupported
	}
	return h.control.AddReplica(shardID, req.Target)
}

func (h *handler) removeReplica(shardID string, req targetRequest) error {
	opts := req.controlWriteOptions()
	if h.controlWithOptions != nil {
		return h.controlWithOptions.RemoveReplicaWithOptions(shardID, req.Target, opts)
	}
	if req.hasControlWriteOptions() {
		return errs.ErrControlWriteOptionsUnsupported
	}
	return h.control.RemoveReplica(shardID, req.Target)
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

func (h *handler) resumeDrain(nodeID string, opts lsm.ControlWriteOptions) error {
	if h.controlResumeWithOptions != nil {
		return h.controlResumeWithOptions.ResumeDrainWithOptions(nodeID, opts)
	}
	if strings.TrimSpace(opts.OperationID) != "" || opts.ExpectedRevision != nil {
		return errs.ErrControlWriteOptionsUnsupported
	}
	return h.controlResume.ResumeDrain(nodeID)
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

func optionalBase64Query(r *http.Request, name string) ([]byte, error) {
	raw := strings.TrimSpace(r.URL.Query().Get(name))
	if raw == "" {
		return nil, nil
	}
	return base64.StdEncoding.DecodeString(raw)
}

func rangeLimit(raw string) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return defaultRangeLimit, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid limit")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be positive")
	}
	if limit > maxRangeLimit {
		return 0, fmt.Errorf("limit must be <= %d", maxRangeLimit)
	}
	return limit, nil
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
	case errors.Is(err, errs.ErrCommitLogUnavailable):
		return http.StatusServiceUnavailable
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

type rangeResponse struct {
	Entries   []rangeEntryResponse `json:"entries"`
	Limit     int                  `json:"limit"`
	Truncated bool                 `json:"truncated"`
}

type rangeEntryResponse struct {
	KeyBase64   string `json:"key_base64"`
	ValueBase64 string `json:"value_base64"`
	Tombstone   bool   `json:"tombstone"`
	Seq         uint64 `json:"seq"`
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
	case errors.Is(err, errs.ErrCommitLogUnavailable):
		return "commit_log_unavailable"
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
		errors.Is(err, errs.ErrCommitLogUnavailable) ||
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

func (s *writeRequestStore) Commit(requestID string, seq uint64) lsm.WriteRequestStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := s.statusMap[requestID]
	status.State = lsm.WriteRequestCommitted
	status.Seq = seq
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
