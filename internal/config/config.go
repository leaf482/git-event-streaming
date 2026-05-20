package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ServiceName          string
	Environment          string
	LogLevel             string
	HTTPAddr             string
	GitHubAPIURL         string
	GitHubToken          string
	GitHubUserAgent      string
	PollInterval         time.Duration
	RequestTimeout       time.Duration
	ShutdownTimeout      time.Duration
	KafkaBrokers         []string
	KafkaTopic           string
	KafkaClientID        string
	KafkaGroupID         string
	RedisAddr            string
	RedisPassword        string
	RedisDB              int
	PostgresDSN          string
	PostgresEnabled      bool
	PersistenceQueueSize int
	PersistenceWorkers   int
	ReplayMode           bool
	ReplayRateLimit      int
	ReplayMaxMessages    int
	IdempotencyTTL       time.Duration
	WindowSize           time.Duration
	WindowTTL            time.Duration
	TrendingLimit        int
}

func Load() (Config, error) {
	cfg := Config{
		ServiceName:          getEnv("SERVICE_NAME", "github-events-ingestor"),
		Environment:          getEnv("ENVIRONMENT", "local"),
		LogLevel:             getEnv("LOG_LEVEL", "info"),
		HTTPAddr:             getEnv("HTTP_ADDR", ":8080"),
		GitHubAPIURL:         getEnv("GITHUB_EVENTS_API_URL", "https://api.github.com/events"),
		GitHubToken:          os.Getenv("GITHUB_TOKEN"),
		GitHubUserAgent:      getEnv("GITHUB_USER_AGENT", "github-pulse-ingestor"),
		PollInterval:         getDurationEnv("POLL_INTERVAL", 15*time.Second),
		RequestTimeout:       getDurationEnv("REQUEST_TIMEOUT", 10*time.Second),
		ShutdownTimeout:      getDurationEnv("SHUTDOWN_TIMEOUT", 10*time.Second),
		KafkaBrokers:         getCSVEnv("KAFKA_BROKERS", []string{"localhost:9092"}),
		KafkaTopic:           getEnv("KAFKA_TOPIC_GITHUB_EVENTS", "github.events.raw.v1"),
		KafkaClientID:        getEnv("KAFKA_CLIENT_ID", "github-events-ingestor"),
		KafkaGroupID:         getEnv("KAFKA_GROUP_ID", "github-events-consumer"),
		RedisAddr:            getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:        os.Getenv("REDIS_PASSWORD"),
		RedisDB:              getIntEnv("REDIS_DB", 0),
		PostgresDSN:          getEnv("POSTGRES_DSN", "postgres://github_pulse:github_pulse@localhost:5432/github_pulse?sslmode=disable"),
		PostgresEnabled:      getBoolEnv("POSTGRES_ENABLED", true),
		PersistenceQueueSize: getIntEnv("PERSISTENCE_QUEUE_SIZE", 1000),
		PersistenceWorkers:   getIntEnv("PERSISTENCE_WORKERS", 1),
		ReplayMode:           getBoolEnv("REPLAY_MODE", false),
		ReplayRateLimit:      getIntEnv("REPLAY_RATE_LIMIT", 0),
		ReplayMaxMessages:    getIntEnv("REPLAY_MAX_MESSAGES", 0),
		IdempotencyTTL:       getDurationEnv("IDEMPOTENCY_TTL", 72*time.Hour),
		WindowSize:           getDurationEnv("TRENDING_WINDOW_SIZE", 5*time.Minute),
		WindowTTL:            getDurationEnv("TRENDING_WINDOW_TTL", 2*time.Hour),
		TrendingLimit:        getIntEnv("TRENDING_LIMIT", 10),
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func (c Config) Validate() error {
	var validationErrors []error

	if len(c.KafkaBrokers) == 0 {
		validationErrors = append(validationErrors, errors.New("KAFKA_BROKERS must contain at least one broker"))
	}
	if c.KafkaTopic == "" {
		validationErrors = append(validationErrors, errors.New("KAFKA_TOPIC_GITHUB_EVENTS must not be empty"))
	}
	if c.GitHubAPIURL == "" {
		validationErrors = append(validationErrors, errors.New("GITHUB_EVENTS_API_URL must not be empty"))
	}
	if c.PollInterval <= 0 {
		validationErrors = append(validationErrors, errors.New("POLL_INTERVAL must be positive"))
	}
	if c.RequestTimeout <= 0 {
		validationErrors = append(validationErrors, errors.New("REQUEST_TIMEOUT must be positive"))
	}
	if c.ShutdownTimeout <= 0 {
		validationErrors = append(validationErrors, errors.New("SHUTDOWN_TIMEOUT must be positive"))
	}
	if c.RedisAddr == "" {
		validationErrors = append(validationErrors, errors.New("REDIS_ADDR must not be empty"))
	}
	if c.IdempotencyTTL <= 0 {
		validationErrors = append(validationErrors, errors.New("IDEMPOTENCY_TTL must be positive"))
	}
	if c.WindowSize <= 0 {
		validationErrors = append(validationErrors, errors.New("TRENDING_WINDOW_SIZE must be positive"))
	}
	if c.WindowTTL <= 0 {
		validationErrors = append(validationErrors, errors.New("TRENDING_WINDOW_TTL must be positive"))
	}
	if c.TrendingLimit <= 0 {
		validationErrors = append(validationErrors, errors.New("TRENDING_LIMIT must be positive"))
	}
	if c.PostgresEnabled && c.PostgresDSN == "" {
		validationErrors = append(validationErrors, errors.New("POSTGRES_DSN must not be empty when POSTGRES_ENABLED=true"))
	}
	if c.PersistenceQueueSize <= 0 {
		validationErrors = append(validationErrors, errors.New("PERSISTENCE_QUEUE_SIZE must be positive"))
	}
	if c.PersistenceWorkers <= 0 {
		validationErrors = append(validationErrors, errors.New("PERSISTENCE_WORKERS must be positive"))
	}
	if c.ReplayRateLimit < 0 {
		validationErrors = append(validationErrors, errors.New("REPLAY_RATE_LIMIT must be zero or positive"))
	}
	if c.ReplayMaxMessages < 0 {
		validationErrors = append(validationErrors, errors.New("REPLAY_MAX_MESSAGES must be zero or positive"))
	}

	if len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	return nil
}

func getEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func getCSVEnv(key string, fallback []string) []string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}

	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value != "" {
			values = append(values, value)
		}
	}

	return values
}

func getDurationEnv(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}

	duration, err := time.ParseDuration(raw)
	if err == nil {
		return duration
	}

	seconds, secondsErr := strconv.Atoi(raw)
	if secondsErr == nil {
		return time.Duration(seconds) * time.Second
	}

	return fallback
}

func getIntEnv(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}

	return value
}

func getBoolEnv(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}

	value, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}

	return value
}

func (c Config) SafeSummary() map[string]any {
	return map[string]any{
		"service_name":           c.ServiceName,
		"environment":            c.Environment,
		"log_level":              c.LogLevel,
		"http_addr":              c.HTTPAddr,
		"github_api_url":         c.GitHubAPIURL,
		"github_token_set":       c.GitHubToken != "",
		"poll_interval":          c.PollInterval.String(),
		"request_timeout":        c.RequestTimeout.String(),
		"shutdown_timeout":       c.ShutdownTimeout.String(),
		"kafka_brokers":          c.KafkaBrokers,
		"kafka_topic":            c.KafkaTopic,
		"kafka_client_id":        c.KafkaClientID,
		"kafka_group_id":         c.KafkaGroupID,
		"redis_addr":             c.RedisAddr,
		"redis_password_set":     c.RedisPassword != "",
		"redis_db":               c.RedisDB,
		"postgres_enabled":       c.PostgresEnabled,
		"postgres_dsn_set":       c.PostgresDSN != "",
		"persistence_queue_size": c.PersistenceQueueSize,
		"persistence_workers":    c.PersistenceWorkers,
		"replay_mode":            c.ReplayMode,
		"replay_rate_limit":      c.ReplayRateLimit,
		"replay_max_messages":    c.ReplayMaxMessages,
		"idempotency_ttl":        c.IdempotencyTTL.String(),
		"window_size":            c.WindowSize.String(),
		"window_ttl":             c.WindowTTL.String(),
		"trending_limit":         c.TrendingLimit,
		"github_user_agent":      c.GitHubUserAgent,
	}
}

func (c Config) String() string {
	return fmt.Sprintf("%v", c.SafeSummary())
}
