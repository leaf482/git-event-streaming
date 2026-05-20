package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/github-pulse/git-event-streaming/internal/events"
	"github.com/redis/go-redis/v9"
)

const (
	processedKeyPrefix = "github_pulse:events:processed:"
	trendingReposKey   = "github_pulse:trending:repos"
	repoMetadataKey    = "github_pulse:repos:metadata"
	windowKeyPrefix    = "github_pulse:trending:repos:window:"
	recentEventsKey    = "github_pulse:events:recent"
	recentEventsLimit  = 100
)

var ErrInvalidEvent = errors.New("invalid event")

var aggregateEventScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
	return 0
end

redis.call("ZINCRBY", KEYS[2], ARGV[1], ARGV[2])
redis.call("HSET", KEYS[3], ARGV[2], ARGV[3])
redis.call("ZINCRBY", KEYS[4], ARGV[1], ARGV[2])
redis.call("EXPIRE", KEYS[4], ARGV[4])
redis.call("SET", KEYS[1], "1", "EX", ARGV[5])
redis.call("LPUSH", KEYS[5], ARGV[6])
redis.call("LTRIM", KEYS[5], 0, ARGV[7])

return 1
`)

type Store struct {
	client         *redis.Client
	idempotencyTTL time.Duration
	windowSize     time.Duration
	windowTTL      time.Duration
}

type ProcessStatus string

const (
	ProcessStatusProcessed ProcessStatus = "processed"
	ProcessStatusDuplicate ProcessStatus = "duplicate"
	ProcessStatusIgnored   ProcessStatus = "ignored"
)

type RepoTrend struct {
	Rank  int     `json:"rank"`
	Name  string  `json:"repo_name"`
	Score float64 `json:"score"`
}

type RecentEvent struct {
	EventID     string  `json:"event_id"`
	EventType   string  `json:"event_type"`
	RepoName    string  `json:"repo_name"`
	ActorLogin  string  `json:"actor_login"`
	Score       float64 `json:"score"`
	CreatedAt   string  `json:"created_at"`
	ProcessedAt string  `json:"processed_at"`
}

type repoMetadata struct {
	RepoID    int64  `json:"repo_id"`
	RepoName  string `json:"repo_name"`
	RepoURL   string `json:"repo_url"`
	EventType string `json:"last_event_type"`
	UpdatedAt string `json:"updated_at"`
}

func NewStore(client *redis.Client, idempotencyTTL, windowSize, windowTTL time.Duration) *Store {
	return &Store{
		client:         client,
		idempotencyTTL: idempotencyTTL,
		windowSize:     windowSize,
		windowTTL:      windowTTL,
	}
}

func (s *Store) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

func (s *Store) ProcessEvent(ctx context.Context, event events.NormalizedEvent) (ProcessStatus, error) {
	score := scoreForEvent(event.EventType)
	if score == 0 {
		return ProcessStatusIgnored, nil
	}
	if event.EventID == "" {
		return ProcessStatusIgnored, fmt.Errorf("%w: event id is required", ErrInvalidEvent)
	}
	if event.RepoName == "" {
		return ProcessStatusIgnored, fmt.Errorf("%w: repo name is required for event_id=%s", ErrInvalidEvent, event.EventID)
	}

	now := time.Now().UTC()
	eventTime := event.CreatedAt
	if eventTime.IsZero() {
		eventTime = now
	}

	metadata := repoMetadata{
		RepoID:    event.RepoID,
		RepoName:  event.RepoName,
		RepoURL:   event.RepoURL,
		EventType: event.EventType,
		UpdatedAt: now.Format(time.RFC3339),
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return ProcessStatusIgnored, fmt.Errorf("marshal repo metadata: %w", err)
	}
	recentEvent := RecentEvent{
		EventID:     event.EventID,
		EventType:   event.EventType,
		RepoName:    event.RepoName,
		ActorLogin:  event.ActorLogin,
		Score:       score,
		CreatedAt:   eventTime.Format(time.RFC3339),
		ProcessedAt: now.Format(time.RFC3339),
	}
	recentEventJSON, err := json.Marshal(recentEvent)
	if err != nil {
		return ProcessStatusIgnored, fmt.Errorf("marshal recent event: %w", err)
	}

	keys := []string{
		processedKeyPrefix + event.EventID,
		trendingReposKey,
		repoMetadataKey,
		s.windowKey(eventTime),
		recentEventsKey,
	}
	args := []any{
		score,
		event.RepoName,
		string(metadataJSON),
		int(s.windowTTL.Seconds()),
		int(s.idempotencyTTL.Seconds()),
		string(recentEventJSON),
		recentEventsLimit - 1,
	}

	result, err := aggregateEventScript.Run(ctx, s.client, keys, args...).Int()
	if err != nil {
		return ProcessStatusIgnored, fmt.Errorf("aggregate event in redis event_id=%s: %w", event.EventID, err)
	}

	if result == 1 {
		return ProcessStatusProcessed, nil
	}

	return ProcessStatusDuplicate, nil
}

func (s *Store) TrendingRepos(ctx context.Context, limit int) ([]RepoTrend, error) {
	if limit <= 0 {
		limit = 10
	}

	results, err := s.client.ZRevRangeWithScores(ctx, trendingReposKey, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, fmt.Errorf("read trending repos: %w", err)
	}

	trends := make([]RepoTrend, 0, len(results))
	for index, result := range results {
		repoName, ok := result.Member.(string)
		if !ok {
			repoName = fmt.Sprint(result.Member)
		}

		trends = append(trends, RepoTrend{
			Rank:  index + 1,
			Name:  repoName,
			Score: result.Score,
		})
	}

	return trends, nil
}

func (s *Store) RecentEvents(ctx context.Context, limit int) ([]RecentEvent, error) {
	if limit <= 0 || limit > recentEventsLimit {
		limit = recentEventsLimit
	}

	results, err := s.client.LRange(ctx, recentEventsKey, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, fmt.Errorf("read recent events: %w", err)
	}

	recentEvents := make([]RecentEvent, 0, len(results))
	for _, result := range results {
		var event RecentEvent
		if err := json.Unmarshal([]byte(result), &event); err != nil {
			return nil, fmt.Errorf("decode recent event: %w", err)
		}
		recentEvents = append(recentEvents, event)
	}

	return recentEvents, nil
}

func (s *Store) windowKey(eventTime time.Time) string {
	bucket := eventTime.UTC().Truncate(s.windowSize).Unix()
	return fmt.Sprintf("%s%d", windowKeyPrefix, bucket)
}

func scoreForEvent(eventType string) float64 {
	switch eventType {
	case "WatchEvent":
		return 3
	case "ForkEvent", "PullRequestEvent":
		return 2
	case "PushEvent", "IssuesEvent", "CreateEvent":
		return 1
	default:
		return 0
	}
}
