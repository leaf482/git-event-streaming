package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/github-pulse/git-event-streaming/internal/config"
	"github.com/github-pulse/git-event-streaming/internal/events"
	"github.com/segmentio/kafka-go"
)

var eventTypes = []string{"WatchEvent", "ForkEvent", "PullRequestEvent", "PushEvent", "IssuesEvent", "CreateEvent"}

func main() {
	var total int
	var rate int
	var repoCount int
	var batchSize int
	var prefix string
	flag.IntVar(&total, "events", getIntEnv("BENCH_EVENTS", 1000), "number of synthetic events to publish")
	flag.IntVar(&rate, "rate", getIntEnv("BENCH_RATE", 100), "maximum events per second; 0 means unthrottled")
	flag.IntVar(&repoCount, "repos", getIntEnv("BENCH_REPOS", 100), "number of synthetic repositories")
	flag.IntVar(&batchSize, "batch", getIntEnv("BENCH_BATCH_SIZE", 100), "Kafka write batch size")
	flag.StringVar(&prefix, "prefix", getEnv("BENCH_PREFIX", "bench"), "synthetic event id prefix")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}
	if total <= 0 || repoCount <= 0 {
		logger.Error("events and repos must be positive", "events", total, "repos", repoCount)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers...),
		Topic:        cfg.KafkaTopic,
		RequiredAcks: kafka.RequireOne,
		BatchSize:    batchSize,
		BatchTimeout: 25 * time.Millisecond,
		Balancer:     &kafka.Hash{},
		Transport: &kafka.Transport{
			ClientID: cfg.KafkaClientID + "-benchgen",
		},
	}
	defer writer.Close()

	runID := randomID()
	started := time.Now().UTC()
	var ticker *time.Ticker
	if rate > 0 {
		ticker = time.NewTicker(time.Second / time.Duration(rate))
		defer ticker.Stop()
	}

	logger.Info("starting synthetic benchmark publish", "events", total, "rate", rate, "repos", repoCount, "batch_size", batchSize, "topic", cfg.KafkaTopic, "run_id", runID)
	messages := make([]kafka.Message, 0, batchSize)
	for i := 0; i < total; i++ {
		if ticker != nil {
			select {
			case <-ctx.Done():
				logger.Info("benchmark publish interrupted", "published", i)
				return
			case <-ticker.C:
			}
		}

		event := syntheticEvent(prefix, runID, i, repoCount, started)
		message, err := kafkaMessage(event)
		if err != nil {
			logger.Error("failed to encode synthetic event", "event_id", event.EventID, "error", err)
			os.Exit(1)
		}
		messages = append(messages, message)
		if len(messages) >= batchSize {
			writeMessages(ctx, writer, cfg.RequestTimeout, messages, logger)
			messages = messages[:0]
		}
	}
	if len(messages) > 0 {
		writeMessages(ctx, writer, cfg.RequestTimeout, messages, logger)
	}

	elapsed := time.Since(started)
	logger.Info("synthetic benchmark publish complete", "events", total, "duration_ms", elapsed.Milliseconds(), "events_per_second", float64(total)/elapsed.Seconds(), "run_id", runID)
}

func kafkaMessage(event events.NormalizedEvent) (kafka.Message, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return kafka.Message{}, fmt.Errorf("marshal normalized event: %w", err)
	}

	return kafka.Message{
		Key:   []byte(event.EventID),
		Value: payload,
		Time:  event.IngestedAt,
		Headers: []kafka.Header{
			{Key: "schema_version", Value: []byte(event.SchemaVersion)},
			{Key: "event_type", Value: []byte(event.EventType)},
			{Key: "source", Value: []byte(event.Source)},
		},
	}, nil
}

func writeMessages(ctx context.Context, writer *kafka.Writer, timeout time.Duration, messages []kafka.Message, logger *slog.Logger) {
	publishCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := writer.WriteMessages(publishCtx, messages...); err != nil {
		logger.Error("failed to publish synthetic event batch", "batch_size", len(messages), "error", err)
		os.Exit(1)
	}
}

func syntheticEvent(prefix, runID string, index, repoCount int, started time.Time) events.NormalizedEvent {
	repoID := index % repoCount
	eventType := eventTypes[index%len(eventTypes)]
	return events.NormalizedEvent{
		SchemaVersion: "1.0",
		Source:        "synthetic_benchmark",
		EventID:       fmt.Sprintf("%s-%s-%d", prefix, runID, index),
		EventType:     eventType,
		ActorID:       int64(100000 + index%1000),
		ActorLogin:    fmt.Sprintf("bench-actor-%d", index%1000),
		RepoID:        int64(900000 + repoID),
		RepoName:      fmt.Sprintf("bench-org/repo-%03d", repoID),
		RepoURL:       fmt.Sprintf("https://api.github.com/repos/bench-org/repo-%03d", repoID),
		Public:        true,
		CreatedAt:     started.Add(time.Duration(index) * time.Millisecond),
		IngestedAt:    time.Now().UTC(),
		Payload:       json.RawMessage(`{"synthetic":true}`),
	}
}

func randomID() string {
	var buf [4]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprint(time.Now().UnixNano())
	}
	return hex.EncodeToString(buf[:])
}

func getEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func getIntEnv(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	var value int
	if _, err := fmt.Sscanf(raw, "%d", &value); err != nil {
		return fallback
	}
	return value
}
