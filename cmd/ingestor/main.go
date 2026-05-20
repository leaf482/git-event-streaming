package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/github-pulse/git-event-streaming/internal/config"
	"github.com/github-pulse/git-event-streaming/internal/events"
	ghclient "github.com/github-pulse/git-event-streaming/internal/github"
	eventkafka "github.com/github-pulse/git-event-streaming/internal/kafka"
	"github.com/github-pulse/git-event-streaming/internal/metrics"
	"github.com/github-pulse/git-event-streaming/internal/server"
)

func main() {
	bootstrapLogger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load()
	if err != nil {
		bootstrapLogger.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLogLevel(cfg.LogLevel)}))
	logger.Info("starting service", "config", cfg.SafeSummary())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	metricsCollector := metrics.NewCollector(time.Now().UTC())
	healthServer := server.NewHealthServer(cfg.HTTPAddr, cfg.ServiceName, metricsCollector, logger)
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("starting health server", "addr", cfg.HTTPAddr)
		if err := healthServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	githubClient := ghclient.NewClient(cfg.GitHubAPIURL, cfg.GitHubToken, cfg.GitHubUserAgent, cfg.RequestTimeout, logger)
	producer := eventkafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaClientID)
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Error("failed to close kafka producer", "error", err)
		}
	}()

	healthServer.SetReady(true)

	pollErr := make(chan error, 1)
	go func() {
		pollErr <- runPollLoop(ctx, cfg, githubClient, producer, metricsCollector, logger)
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-serverErr:
		logger.Error("health server failed", "error", err)
	case err := <-pollErr:
		if err != nil {
			logger.Error("poll loop failed", "error", err)
		}
	}

	healthServer.SetReady(false)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := healthServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("failed to shutdown health server", "error", err)
	}

	logger.Info("service stopped")
}

func parseLogLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

type githubPoller interface {
	PollEvents(context.Context) ([]events.GitHubPublicEvent, error)
}

type eventProducer interface {
	PublishEvent(context.Context, events.NormalizedEvent) error
}

func runPollLoop(ctx context.Context, cfg config.Config, githubClient githubPoller, producer eventProducer, metricsCollector *metrics.Collector, logger *slog.Logger) error {
	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	for {
		pollOnce(ctx, cfg, githubClient, producer, metricsCollector, logger)

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func pollOnce(ctx context.Context, cfg config.Config, githubClient githubPoller, producer eventProducer, metricsCollector *metrics.Collector, logger *slog.Logger) {
	started := time.Now().UTC()
	metricsCollector.RecordPoll(started)

	pollCtx, cancel := context.WithTimeout(ctx, cfg.RequestTimeout)
	defer cancel()

	publicEvents, err := githubClient.PollEvents(pollCtx)
	if err != nil {
		metricsCollector.RecordPollError()
		logger.Error("github events poll failed", "error", err, "duration_ms", time.Since(started).Milliseconds())
		return
	}
	if len(publicEvents) == 0 {
		metricsCollector.RecordBatch(0, 0, 0, time.Now().UTC())
		logger.Info("no new github events returned", "duration_ms", time.Since(started).Milliseconds())
		return
	}

	ingestedAt := time.Now().UTC()
	var publishedCount int
	var failedCount int

	for _, publicEvent := range publicEvents {
		normalized, err := events.NormalizeGitHubEvent(publicEvent, ingestedAt)
		if err != nil {
			failedCount++
			logger.Warn("failed to normalize github event", "event_id", publicEvent.ID, "event_type", publicEvent.Type, "error", err)
			continue
		}

		publishCtx, publishCancel := context.WithTimeout(ctx, cfg.RequestTimeout)
		err = producer.PublishEvent(publishCtx, normalized)
		publishCancel()
		if err != nil {
			failedCount++
			logger.Error("failed to publish normalized event", "event_id", normalized.EventID, "event_type", normalized.EventType, "repo", normalized.RepoName, "error", err)
			continue
		}

		publishedCount++
	}

	metricsCollector.RecordBatch(len(publicEvents), publishedCount, failedCount, time.Now().UTC())
	logger.Info("processed github events batch", "received", len(publicEvents), "published", publishedCount, "failed", failedCount, "topic", cfg.KafkaTopic, "duration_ms", time.Since(started).Milliseconds())
}
