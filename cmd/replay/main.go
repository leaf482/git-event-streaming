package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/github-pulse/git-event-streaming/internal/config"
	"github.com/github-pulse/git-event-streaming/internal/consumer"
	"github.com/github-pulse/git-event-streaming/internal/events"
	"github.com/github-pulse/git-event-streaming/internal/persistence"
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	defer redisClient.Close()
	realtimeStore := consumer.NewStore(redisClient, cfg.IdempotencyTTL, cfg.WindowSize, cfg.WindowTTL)

	historyStore, err := persistence.Connect(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Error("postgres is required for replay foundation", "error", err)
		os.Exit(1)
	}
	defer historyStore.Close()
	if err := historyStore.InitSchema(ctx); err != nil {
		logger.Error("failed to initialize postgres schema", "error", err)
		os.Exit(1)
	}

	groupID := cfg.KafkaGroupID
	if groupID == "github-events-consumer" {
		groupID = "github-events-replay"
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.KafkaTopic,
		GroupID: groupID,
		Dialer: &kafka.Dialer{
			ClientID: cfg.KafkaClientID + "-replay",
			Timeout:  cfg.RequestTimeout,
		},
	})
	defer reader.Close()

	logger.Info("starting replay foundation worker", "topic", cfg.KafkaTopic, "group_id", groupID)
	var processed int
	started := time.Now()
	var rateTicker *time.Ticker
	if cfg.ReplayRateLimit > 0 {
		rateTicker = time.NewTicker(time.Second / time.Duration(cfg.ReplayRateLimit))
		defer rateTicker.Stop()
	}
	for {
		if cfg.ReplayMaxMessages > 0 && processed >= cfg.ReplayMaxMessages {
			logger.Info("replay max messages reached", "processed", processed, "duration_ms", time.Since(started).Milliseconds())
			return
		}
		if rateTicker != nil {
			select {
			case <-ctx.Done():
				logger.Info("replay worker stopped")
				return
			case <-rateTicker.C:
			}
		}
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				logger.Info("replay worker stopped")
				return
			}
			logger.Error("failed to fetch replay message", "error", err)
			os.Exit(1)
		}

		var event events.NormalizedEvent
		if err := json.Unmarshal(message.Value, &event); err != nil {
			logger.Error("failed to decode replay event", "offset", message.Offset, "error", err)
			_ = reader.CommitMessages(ctx, message)
			continue
		}

		status, err := realtimeStore.ProcessEvent(ctx, event)
		if err != nil {
			logger.Error("failed to rebuild redis state", "event_id", event.EventID, "error", err)
			continue
		}
		if _, err := historyStore.PersistEvent(ctx, persistence.EventRecord{Event: event, Score: consumer.EventScore(event.EventType)}); err != nil {
			logger.Error("failed to rebuild postgres state", "event_id", event.EventID, "error", err)
			continue
		}
		if err := reader.CommitMessages(ctx, message); err != nil {
			logger.Error("failed to commit replay message", "error", err)
			os.Exit(1)
		}

		processed++
		logger.Debug("replayed event", "event_id", event.EventID, "repo", event.RepoName, "status", status)
		if processed%100 == 0 {
			logger.Info("replay progress", "processed", processed, "events_per_second", float64(processed)/time.Since(started).Seconds())
		}
	}
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
