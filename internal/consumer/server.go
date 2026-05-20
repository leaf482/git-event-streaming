package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/github-pulse/git-event-streaming/internal/persistence"
)

type Server struct {
	server  *http.Server
	ready   atomic.Bool
	store   *Store
	history *persistence.Store
	metrics *Metrics
	limit   int
}

type trendingResponse struct {
	Repos []RepoTrend `json:"repos"`
}

type recentEventsResponse struct {
	Events []RecentEvent `json:"events"`
}

type repoHistoryResponse struct {
	RepoName string                         `json:"repo_name"`
	Points   []persistence.RepoHistoryPoint `json:"points"`
}

type historicalTrendingResponse struct {
	Repos []persistence.HistoricalTrend `json:"repos"`
}

type historyWindowsResponse struct {
	Windows []persistence.AggregationWindow `json:"windows"`
}

func NewServer(addr string, store *Store, history *persistence.Store, metrics *Metrics, limit int, logger *slog.Logger) *Server {
	s := &Server{
		store:   store,
		history: history,
		metrics: metrics,
		limit:   limit,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/live", s.handleLive)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/api/trending/repos", s.handleTrendingRepos)
	mux.HandleFunc("/api/trending/repos/stream", s.handleTrendingReposStream)
	mux.HandleFunc("/api/events/recent", s.handleRecentEvents)
	mux.HandleFunc("/api/events/stream", s.handleRecentEventsStream)
	mux.HandleFunc("/api/history/repos/", s.handleRepoHistory)
	mux.HandleFunc("/api/history/trending/repos", s.handleHistoricalTrendingRepos)
	mux.HandleFunc("/api/history/windows", s.handleHistoryWindows)

	s.server = &http.Server{
		Addr:              addr,
		Handler:           requestLogger(mux, logger, s.metrics),
		ReadHeaderTimeout: 5 * time.Second,
	}

	return s
}

func (s *Server) SetReady(ready bool) {
	s.ready.Store(ready)
}

func (s *Server) ListenAndServe() error {
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *Server) handleLive(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "github-events-consumer"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if !s.ready.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}
	if err := s.store.Ping(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready", "error": "redis_unavailable"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ready", "service": "github-events-consumer"})
}

func (s *Server) handleTrendingRepos(w http.ResponseWriter, r *http.Request) {
	limit, ok := s.parseLimit(w, r, s.limit)
	if !ok {
		return
	}

	repos, err := s.store.TrendingRepos(r.Context(), limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to read trending repositories"})
		return
	}

	writeJSON(w, http.StatusOK, trendingResponse{Repos: repos})
}

func (s *Server) handleRecentEvents(w http.ResponseWriter, r *http.Request) {
	limit, ok := s.parseLimit(w, r, 50)
	if !ok {
		return
	}

	events, err := s.store.RecentEvents(r.Context(), limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to read recent events"})
		return
	}

	writeJSON(w, http.StatusOK, recentEventsResponse{Events: events})
}

func (s *Server) handleTrendingReposStream(w http.ResponseWriter, r *http.Request) {
	limit, ok := s.parseLimit(w, r, s.limit)
	if !ok {
		return
	}

	s.streamJSON(w, r, "trending", func(ctx context.Context) (any, error) {
		repos, err := s.store.TrendingRepos(ctx, limit)
		if err != nil {
			return nil, err
		}
		return trendingResponse{Repos: repos}, nil
	})
}

func (s *Server) handleRecentEventsStream(w http.ResponseWriter, r *http.Request) {
	limit, ok := s.parseLimit(w, r, 50)
	if !ok {
		return
	}

	s.streamJSON(w, r, "events", func(ctx context.Context) (any, error) {
		events, err := s.store.RecentEvents(ctx, limit)
		if err != nil {
			return nil, err
		}
		return recentEventsResponse{Events: events}, nil
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	snapshot := s.metrics.Snapshot(time.Now().UTC())
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(w, "# HELP github_pulse_consumer_uptime_seconds Seconds since consumer startup.\n")
	_, _ = fmt.Fprintf(w, "# TYPE github_pulse_consumer_uptime_seconds gauge\n")
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_uptime_seconds %d\n", snapshot.UptimeSeconds)
	_, _ = fmt.Fprintf(w, "# HELP github_pulse_consumer_events_consumed_total Total Kafka messages consumed.\n")
	_, _ = fmt.Fprintf(w, "# TYPE github_pulse_consumer_events_consumed_total counter\n")
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_events_consumed_total %d\n", snapshot.ConsumedTotal)
	_, _ = fmt.Fprintf(w, "# HELP github_pulse_consumer_events_processed_total Total events applied to Redis aggregations.\n")
	_, _ = fmt.Fprintf(w, "# TYPE github_pulse_consumer_events_processed_total counter\n")
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_events_processed_total %d\n", snapshot.ProcessedTotal)
	_, _ = fmt.Fprintf(w, "# HELP github_pulse_consumer_duplicate_events_skipped_total Total duplicate events skipped by idempotency.\n")
	_, _ = fmt.Fprintf(w, "# TYPE github_pulse_consumer_duplicate_events_skipped_total counter\n")
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_duplicate_events_skipped_total %d\n", snapshot.DuplicateSkippedTotal)
	_, _ = fmt.Fprintf(w, "# HELP github_pulse_consumer_redis_failures_total Total Redis operation failures.\n")
	_, _ = fmt.Fprintf(w, "# TYPE github_pulse_consumer_redis_failures_total counter\n")
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_redis_failures_total %d\n", snapshot.RedisFailuresTotal)
	_, _ = fmt.Fprintf(w, "# HELP github_pulse_consumer_processing_failures_total Total non-Redis processing failures.\n")
	_, _ = fmt.Fprintf(w, "# TYPE github_pulse_consumer_processing_failures_total counter\n")
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_processing_failures_total %d\n", snapshot.ProcessingFailures)
	_, _ = fmt.Fprintf(w, "# HELP github_pulse_consumer_processing_latency_seconds_total Total processing latency in seconds.\n")
	_, _ = fmt.Fprintf(w, "# TYPE github_pulse_consumer_processing_latency_seconds_total counter\n")
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_processing_latency_seconds_total %.6f\n", float64(snapshot.ProcessingLatencyNanos)/float64(time.Second))
	_, _ = fmt.Fprintf(w, "# HELP github_pulse_consumer_processing_latency_seconds Processing latency histogram.\n")
	_, _ = fmt.Fprintf(w, "# TYPE github_pulse_consumer_processing_latency_seconds histogram\n")
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_processing_latency_seconds_bucket{le=\"0.01\"} %d\n", snapshot.ProcessingLatencyBuckets.LE10ms)
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_processing_latency_seconds_bucket{le=\"0.05\"} %d\n", snapshot.ProcessingLatencyBuckets.LE10ms+snapshot.ProcessingLatencyBuckets.LE50ms)
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_processing_latency_seconds_bucket{le=\"0.1\"} %d\n", snapshot.ProcessingLatencyBuckets.LE10ms+snapshot.ProcessingLatencyBuckets.LE50ms+snapshot.ProcessingLatencyBuckets.LE100ms)
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_processing_latency_seconds_bucket{le=\"0.5\"} %d\n", snapshot.ProcessingLatencyBuckets.LE10ms+snapshot.ProcessingLatencyBuckets.LE50ms+snapshot.ProcessingLatencyBuckets.LE100ms+snapshot.ProcessingLatencyBuckets.LE500ms)
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_processing_latency_seconds_bucket{le=\"1\"} %d\n", snapshot.ProcessingLatencyBuckets.LE10ms+snapshot.ProcessingLatencyBuckets.LE50ms+snapshot.ProcessingLatencyBuckets.LE100ms+snapshot.ProcessingLatencyBuckets.LE500ms+snapshot.ProcessingLatencyBuckets.LE1s)
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_processing_latency_seconds_bucket{le=\"+Inf\"} %d\n", snapshot.ProcessedTotal)
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_processing_latency_seconds_sum %.6f\n", float64(snapshot.ProcessingLatencyNanos)/float64(time.Second))
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_processing_latency_seconds_count %d\n", snapshot.ProcessedTotal)
	_, _ = fmt.Fprintf(w, "# HELP github_pulse_consumer_postgres_writes_total Total successful PostgreSQL persistence writes.\n")
	_, _ = fmt.Fprintf(w, "# TYPE github_pulse_consumer_postgres_writes_total counter\n")
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_postgres_writes_total %d\n", snapshot.PostgresWritesTotal)
	_, _ = fmt.Fprintf(w, "# HELP github_pulse_consumer_postgres_write_failures_total Total PostgreSQL persistence failures.\n")
	_, _ = fmt.Fprintf(w, "# TYPE github_pulse_consumer_postgres_write_failures_total counter\n")
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_postgres_write_failures_total %d\n", snapshot.PostgresWriteFailures)
	_, _ = fmt.Fprintf(w, "# HELP github_pulse_consumer_postgres_write_latency_seconds_total Total PostgreSQL write latency in seconds.\n")
	_, _ = fmt.Fprintf(w, "# TYPE github_pulse_consumer_postgres_write_latency_seconds_total counter\n")
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_postgres_write_latency_seconds_total %.6f\n", float64(snapshot.PostgresWriteLatencyNanos)/float64(time.Second))
	_, _ = fmt.Fprintf(w, "# HELP github_pulse_consumer_persistence_dropped_total Total events dropped from async persistence queue.\n")
	_, _ = fmt.Fprintf(w, "# TYPE github_pulse_consumer_persistence_dropped_total counter\n")
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_persistence_dropped_total %d\n", snapshot.PersistenceDroppedTotal)
	_, _ = fmt.Fprintf(w, "# HELP github_pulse_consumer_historical_queries_total Total historical API queries.\n")
	_, _ = fmt.Fprintf(w, "# TYPE github_pulse_consumer_historical_queries_total counter\n")
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_historical_queries_total %d\n", snapshot.HistoricalQueriesTotal)
	_, _ = fmt.Fprintf(w, "# HELP github_pulse_consumer_historical_query_failures_total Total historical API query failures.\n")
	_, _ = fmt.Fprintf(w, "# TYPE github_pulse_consumer_historical_query_failures_total counter\n")
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_historical_query_failures_total %d\n", snapshot.HistoricalQueryFailures)
	_, _ = fmt.Fprintf(w, "# HELP github_pulse_consumer_replay_operations_total Total replay operations started by this service.\n")
	_, _ = fmt.Fprintf(w, "# TYPE github_pulse_consumer_replay_operations_total counter\n")
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_replay_operations_total %d\n", snapshot.ReplayOperationsTotal)
	_, _ = fmt.Fprintf(w, "# HELP github_pulse_consumer_snapshot_writes_total Total repository snapshot writes.\n")
	_, _ = fmt.Fprintf(w, "# TYPE github_pulse_consumer_snapshot_writes_total counter\n")
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_snapshot_writes_total %d\n", snapshot.SnapshotWritesTotal)
	_, _ = fmt.Fprintf(w, "# HELP github_pulse_consumer_api_requests_total Total consumer API requests.\n")
	_, _ = fmt.Fprintf(w, "# TYPE github_pulse_consumer_api_requests_total counter\n")
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_api_requests_total %d\n", snapshot.APIRequestsTotal)
	_, _ = fmt.Fprintf(w, "# HELP github_pulse_consumer_api_errors_total Total consumer API requests that returned 5xx.\n")
	_, _ = fmt.Fprintf(w, "# TYPE github_pulse_consumer_api_errors_total counter\n")
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_api_errors_total %d\n", snapshot.APIErrorsTotal)
	_, _ = fmt.Fprintf(w, "# HELP github_pulse_consumer_sse_connections_total Total SSE stream connections accepted.\n")
	_, _ = fmt.Fprintf(w, "# TYPE github_pulse_consumer_sse_connections_total counter\n")
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_sse_connections_total %d\n", snapshot.SSEConnectionsTotal)
	_, _ = fmt.Fprintf(w, "# HELP github_pulse_consumer_sse_active_connections Active SSE stream connections.\n")
	_, _ = fmt.Fprintf(w, "# TYPE github_pulse_consumer_sse_active_connections gauge\n")
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_sse_active_connections %d\n", snapshot.SSEActiveConnections)
	_, _ = fmt.Fprintf(w, "# HELP github_pulse_consumer_persistence_queue_depth Current async PostgreSQL persistence queue depth.\n")
	_, _ = fmt.Fprintf(w, "# TYPE github_pulse_consumer_persistence_queue_depth gauge\n")
	_, _ = fmt.Fprintf(w, "github_pulse_consumer_persistence_queue_depth %d\n", snapshot.PersistenceQueueDepth)
}

func (s *Server) handleRepoHistory(w http.ResponseWriter, r *http.Request) {
	if s.history == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "historical persistence is disabled"})
		return
	}

	repoPath := strings.TrimPrefix(r.URL.Path, "/api/history/repos/")
	parts := strings.SplitN(repoPath, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "expected /api/history/repos/:owner/:repo"})
		return
	}
	repoName := parts[0] + "/" + parts[1]
	limit, ok := s.parseLimit(w, r, 48)
	if !ok {
		return
	}

	s.metrics.RecordHistoricalQuery()
	points, err := s.history.RepoHistory(r.Context(), repoName, limit)
	if err != nil {
		s.metrics.RecordHistoricalQueryFailure()
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to read repository history"})
		return
	}

	writeJSON(w, http.StatusOK, repoHistoryResponse{RepoName: repoName, Points: points})
}

func (s *Server) handleHistoricalTrendingRepos(w http.ResponseWriter, r *http.Request) {
	if s.history == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "historical persistence is disabled"})
		return
	}

	limit, ok := s.parseLimit(w, r, s.limit)
	if !ok {
		return
	}

	s.metrics.RecordHistoricalQuery()
	repos, err := s.history.HistoricalTrending(r.Context(), limit)
	if err != nil {
		s.metrics.RecordHistoricalQueryFailure()
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to read historical trending repositories"})
		return
	}

	writeJSON(w, http.StatusOK, historicalTrendingResponse{Repos: repos})
}

func (s *Server) handleHistoryWindows(w http.ResponseWriter, r *http.Request) {
	if s.history == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "historical persistence is disabled"})
		return
	}

	limit, ok := s.parseLimit(w, r, 24)
	if !ok {
		return
	}

	s.metrics.RecordHistoricalQuery()
	windows, err := s.history.AggregationWindows(r.Context(), limit)
	if err != nil {
		s.metrics.RecordHistoricalQueryFailure()
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to read aggregation windows"})
		return
	}

	writeJSON(w, http.StatusOK, historyWindowsResponse{Windows: windows})
}

func (s *Server) parseLimit(w http.ResponseWriter, r *http.Request, fallback int) (int, bool) {
	limit := fallback
	if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
		parsedLimit, err := strconv.Atoi(rawLimit)
		if err != nil || parsedLimit <= 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be a positive integer"})
			return 0, false
		}
		if parsedLimit < limit {
			limit = parsedLimit
		}
	}

	return limit, true
}

func (s *Server) streamJSON(w http.ResponseWriter, r *http.Request, eventName string, snapshot func(context.Context) (any, error)) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "streaming unsupported"})
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	s.metrics.RecordSSEConnected()
	defer s.metrics.RecordSSEDisconnected()
	once := r.URL.Query().Get("once") == "true"
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		payload, err := snapshot(r.Context())
		if err != nil {
			_, _ = fmt.Fprintf(w, "event: error\ndata: %q\n\n", err.Error())
		} else {
			data, err := json.Marshal(payload)
			if err != nil {
				_, _ = fmt.Fprintf(w, "event: error\ndata: %q\n\n", err.Error())
			} else {
				_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", eventName, data)
			}
		}
		flusher.Flush()
		if once {
			return
		}

		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

func requestLogger(next http.Handler, logger *slog.Logger, metrics *Metrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(recorder, r)
		metrics.RecordAPIRequest(recorder.statusCode)
		logger.DebugContext(r.Context(), "handled consumer request", "method", r.Method, "path", r.URL.Path, "duration_ms", time.Since(started).Milliseconds())
	})
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(statusCode int) {
	r.statusCode = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}

func (r *statusRecorder) Flush() {
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}
