package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/github-pulse/git-event-streaming/internal/metrics"
)

type HealthServer struct {
	server  *http.Server
	ready   atomic.Bool
	started time.Time
}

type HealthResponse struct {
	Status    string `json:"status"`
	Service   string `json:"service"`
	Uptime    string `json:"uptime"`
	Timestamp string `json:"timestamp"`
}

type MetricsProvider interface {
	Snapshot(time.Time) metrics.Snapshot
	RecordAPIRequest(statusCode int)
}

func NewHealthServer(addr, serviceName string, metrics MetricsProvider, logger *slog.Logger) *HealthServer {
	health := &HealthServer{
		started: time.Now().UTC(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/live", health.handleLive(serviceName))
	mux.HandleFunc("/ready", health.handleReady(serviceName))
	mux.HandleFunc("/metrics", health.handleMetrics(metrics))

	health.server = &http.Server{
		Addr:              addr,
		Handler:           requestLogger(mux, logger, metrics),
		ReadHeaderTimeout: 5 * time.Second,
	}

	return health
}

func (h *HealthServer) handleMetrics(metrics MetricsProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		snapshot := metrics.Snapshot(time.Now().UTC())
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintf(w, "# HELP github_pulse_ingestor_uptime_seconds Seconds since ingestor startup.\n")
		_, _ = fmt.Fprintf(w, "# TYPE github_pulse_ingestor_uptime_seconds gauge\n")
		_, _ = fmt.Fprintf(w, "github_pulse_ingestor_uptime_seconds %d\n", snapshot.UptimeSeconds)
		_, _ = fmt.Fprintf(w, "# HELP github_pulse_ingestor_last_poll_timestamp_seconds Unix timestamp of the latest poll attempt.\n")
		_, _ = fmt.Fprintf(w, "# TYPE github_pulse_ingestor_last_poll_timestamp_seconds gauge\n")
		_, _ = fmt.Fprintf(w, "github_pulse_ingestor_last_poll_timestamp_seconds %d\n", snapshot.LastPollUnix)
		_, _ = fmt.Fprintf(w, "# HELP github_pulse_ingestor_last_successful_poll_timestamp_seconds Unix timestamp of the latest successful poll.\n")
		_, _ = fmt.Fprintf(w, "# TYPE github_pulse_ingestor_last_successful_poll_timestamp_seconds gauge\n")
		_, _ = fmt.Fprintf(w, "github_pulse_ingestor_last_successful_poll_timestamp_seconds %d\n", snapshot.LastSuccessfulUnix)
		_, _ = fmt.Fprintf(w, "# HELP github_pulse_ingestor_polls_total Total GitHub poll attempts.\n")
		_, _ = fmt.Fprintf(w, "# TYPE github_pulse_ingestor_polls_total counter\n")
		_, _ = fmt.Fprintf(w, "github_pulse_ingestor_polls_total %d\n", snapshot.PollTotal)
		_, _ = fmt.Fprintf(w, "# HELP github_pulse_ingestor_poll_errors_total Total GitHub poll errors.\n")
		_, _ = fmt.Fprintf(w, "# TYPE github_pulse_ingestor_poll_errors_total counter\n")
		_, _ = fmt.Fprintf(w, "github_pulse_ingestor_poll_errors_total %d\n", snapshot.PollErrorTotal)
		_, _ = fmt.Fprintf(w, "# HELP github_pulse_ingestor_events_received_total Total GitHub events received.\n")
		_, _ = fmt.Fprintf(w, "# TYPE github_pulse_ingestor_events_received_total counter\n")
		_, _ = fmt.Fprintf(w, "github_pulse_ingestor_events_received_total %d\n", snapshot.EventsReceivedTotal)
		_, _ = fmt.Fprintf(w, "# HELP github_pulse_ingestor_events_published_total Total normalized events published to Kafka.\n")
		_, _ = fmt.Fprintf(w, "# TYPE github_pulse_ingestor_events_published_total counter\n")
		_, _ = fmt.Fprintf(w, "github_pulse_ingestor_events_published_total %d\n", snapshot.EventsPublishedTotal)
		_, _ = fmt.Fprintf(w, "# HELP github_pulse_ingestor_events_failed_total Total events that failed normalization or publishing.\n")
		_, _ = fmt.Fprintf(w, "# TYPE github_pulse_ingestor_events_failed_total counter\n")
		_, _ = fmt.Fprintf(w, "github_pulse_ingestor_events_failed_total %d\n", snapshot.EventsFailedTotal)
		_, _ = fmt.Fprintf(w, "# HELP github_pulse_ingestor_api_requests_total Total ingestor API requests.\n")
		_, _ = fmt.Fprintf(w, "# TYPE github_pulse_ingestor_api_requests_total counter\n")
		_, _ = fmt.Fprintf(w, "github_pulse_ingestor_api_requests_total %d\n", snapshot.APIRequestsTotal)
		_, _ = fmt.Fprintf(w, "# HELP github_pulse_ingestor_api_errors_total Total ingestor API requests that returned 5xx.\n")
		_, _ = fmt.Fprintf(w, "# TYPE github_pulse_ingestor_api_errors_total counter\n")
		_, _ = fmt.Fprintf(w, "github_pulse_ingestor_api_errors_total %d\n", snapshot.APIErrorsTotal)
	}
}

func (h *HealthServer) SetReady(ready bool) {
	h.ready.Store(ready)
}

func (h *HealthServer) ListenAndServe() error {
	return h.server.ListenAndServe()
}

func (h *HealthServer) Shutdown(ctx context.Context) error {
	return h.server.Shutdown(ctx)
}

func (h *HealthServer) handleLive(serviceName string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, h.response("ok", serviceName))
	}
}

func (h *HealthServer) handleReady(serviceName string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !h.ready.Load() {
			writeJSON(w, http.StatusServiceUnavailable, h.response("not_ready", serviceName))
			return
		}

		writeJSON(w, http.StatusOK, h.response("ready", serviceName))
	}
}

func (h *HealthServer) response(status, serviceName string) HealthResponse {
	return HealthResponse{
		Status:    status,
		Service:   serviceName,
		Uptime:    time.Since(h.started).Round(time.Second).String(),
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
}

func requestLogger(next http.Handler, logger *slog.Logger, metrics MetricsProvider) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(recorder, r)
		metrics.RecordAPIRequest(recorder.statusCode)
		if r.URL.Path == "/live" || r.URL.Path == "/ready" || r.URL.Path == "/metrics" {
			logger.DebugContext(r.Context(), "handled operational endpoint request", "method", r.Method, "path", r.URL.Path, "duration_ms", time.Since(started).Milliseconds())
			return
		}
		logger.InfoContext(r.Context(), "handled request", "method", r.Method, "path", r.URL.Path, "duration_ms", time.Since(started).Milliseconds())
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

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}
