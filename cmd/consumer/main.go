package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/github-pulse/git-event-streaming/internal/config"
	"github.com/github-pulse/git-event-streaming/internal/consumer"
	"github.com/github-pulse/git-event-streaming/internal/events"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
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

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	defer func() {
		if err := redisClient.Close(); err != nil {
			logger.Error("failed to close redis client", "error", err)
		}
	}()

	store := consumer.NewStore(redisClient, cfg.IdempotencyTTL, cfg.WindowSize, cfg.WindowTTL)
	metrics := consumer.NewMetrics(time.Now().UTC())

	apiServer := consumer.NewServer(cfg.HTTPAddr, store, metrics, cfg.TrendingLimit, logger)
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("starting consumer api server", "addr", cfg.HTTPAddr)
		if err := apiServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    cfg.KafkaTopic,
		GroupID:  cfg.KafkaGroupID,
		MinBytes: 1,
		MaxBytes: 10e6,
		Dialer: &kafka.Dialer{
			ClientID: cfg.KafkaClientID,
			Timeout:  cfg.RequestTimeout,
		},
	})
	defer func() {
		if err := reader.Close(); err != nil {
			logger.Error("failed to close kafka reader", "error", err)
		}
	}()

	apiServer.SetReady(true)

	consumeErr := make(chan error, 1)
	go func() {
		consumeErr <- runConsumer(ctx, reader, store, metrics, logger)
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-serverErr:
		logger.Error("api server failed", "error", err)
	case err := <-consumeErr:
		if err != nil {
			logger.Error("consumer failed", "error", err)
		}
	}

	apiServer.SetReady(false)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := apiServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("failed to shutdown api server", "error", err)
	}

	logger.Info("service stopped")
}

type messageReader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
}

func runConsumer(ctx context.Context, reader messageReader, store *consumer.Store, metrics *consumer.Metrics, logger *slog.Logger) error {
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		commit, err := processMessage(ctx, message, store, metrics, logger)
		if err != nil {
			logger.Error("failed to process kafka message", "topic", message.Topic, "partition", message.Partition, "offset", message.Offset, "error", err)
			if commit {
				if commitErr := reader.CommitMessages(ctx, message); commitErr != nil {
					return commitErr
				}
			}
			continue
		}

		if err := reader.CommitMessages(ctx, message); err != nil {
			return err
		}
	}
}

func processMessage(ctx context.Context, message kafka.Message, store *consumer.Store, metrics *consumer.Metrics, logger *slog.Logger) (bool, error) {
	started := time.Now()
	metrics.RecordConsumed()

	var event events.NormalizedEvent
	if err := json.Unmarshal(message.Value, &event); err != nil {
		metrics.RecordProcessingFailure()
		return true, err
	}

	status, err := store.ProcessEvent(ctx, event)
	if err != nil {
		if errors.Is(err, consumer.ErrInvalidEvent) {
			metrics.RecordProcessingFailure()
			return true, err
		}

		metrics.RecordRedisFailure()
		return false, err
	}

	switch status {
	case consumer.ProcessStatusProcessed:
		metrics.RecordProcessed(time.Since(started))
		logger.InfoContext(ctx, "processed github event", "event_id", event.EventID, "event_type", event.EventType, "repo", event.RepoName, "latency_ms", time.Since(started).Milliseconds())
	case consumer.ProcessStatusDuplicate:
		metrics.RecordDuplicateSkipped()
		logger.DebugContext(ctx, "skipped duplicate github event", "event_id", event.EventID, "event_type", event.EventType, "repo", event.RepoName)
	case consumer.ProcessStatusIgnored:
		logger.DebugContext(ctx, "ignored unweighted github event", "event_id", event.EventID, "event_type", event.EventType, "repo", event.RepoName)
	}

	return true, nil
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
