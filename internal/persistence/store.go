package persistence

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/github-pulse/git-event-streaming/internal/events"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultWindowSize = time.Hour

type Store struct {
	pool *pgxpool.Pool
}

type EventRecord struct {
	Event events.NormalizedEvent
	Score float64
}

type RepoHistoryPoint struct {
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	Score       float64   `json:"score"`
	EventCount  int64     `json:"event_count"`
	Rank        int       `json:"rank"`
}

type HistoricalTrend struct {
	Rank        int       `json:"rank"`
	RepoName    string    `json:"repo_name"`
	Score       float64   `json:"score"`
	EventCount  int64     `json:"event_count"`
	LastEventAt time.Time `json:"last_event_at"`
}

type AggregationWindow struct {
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	WindowSize  string    `json:"window_size"`
	EventCount  int64     `json:"event_count"`
	RepoCount   int64     `json:"repo_count"`
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func Connect(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return NewStore(pool), nil
}

func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

func (s *Store) InitSchema(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schemaSQL)
	if err != nil {
		return fmt.Errorf("initialize postgres schema: %w", err)
	}
	return nil
}

func (s *Store) Ping(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return errors.New("postgres store is not configured")
	}
	return s.pool.Ping(ctx)
}

func (s *Store) PersistEvent(ctx context.Context, record EventRecord) (bool, error) {
	event := record.Event
	if event.EventID == "" {
		return false, errors.New("event id is required")
	}
	if event.RepoName == "" {
		return false, errors.New("repo name is required")
	}
	eventTime := event.CreatedAt
	if eventTime.IsZero() {
		eventTime = time.Now().UTC()
	}
	windowStart := eventTime.UTC().Truncate(defaultWindowSize)
	windowEnd := windowStart.Add(defaultWindowSize)

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin postgres persistence transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	inserted, err := insertProcessedEvent(ctx, tx, event, record.Score, eventTime)
	if err != nil {
		return false, err
	}
	if !inserted {
		return false, tx.Commit(ctx)
	}
	if record.Score <= 0 {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit postgres event metadata transaction: %w", err)
		}
		return true, nil
	}

	if err := upsertWindow(ctx, tx, windowStart, windowEnd, "1h"); err != nil {
		return false, err
	}
	if err := upsertRepoSnapshot(ctx, tx, event, record.Score, windowStart, windowEnd, eventTime); err != nil {
		return false, err
	}
	if err := upsertHourlySummary(ctx, tx, event, record.Score, windowStart, windowEnd, eventTime); err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit postgres persistence transaction: %w", err)
	}

	return true, nil
}

func (s *Store) RepoHistory(ctx context.Context, repoName string, limit int) ([]RepoHistoryPoint, error) {
	if limit <= 0 {
		limit = 48
	}

	rows, err := s.pool.Query(ctx, `
		SELECT window_start, window_end, score, event_count, rank
		FROM repository_score_snapshots
		WHERE repo_name = $1
		ORDER BY window_start DESC
		LIMIT $2
	`, repoName, limit)
	if err != nil {
		return nil, fmt.Errorf("query repository history: %w", err)
	}
	defer rows.Close()

	points := make([]RepoHistoryPoint, 0)
	for rows.Next() {
		var point RepoHistoryPoint
		if err := rows.Scan(&point.WindowStart, &point.WindowEnd, &point.Score, &point.EventCount, &point.Rank); err != nil {
			return nil, fmt.Errorf("scan repository history: %w", err)
		}
		points = append(points, point)
	}

	return points, rows.Err()
}

func (s *Store) HistoricalTrending(ctx context.Context, limit int) ([]HistoricalTrend, error) {
	if limit <= 0 {
		limit = 10
	}

	rows, err := s.pool.Query(ctx, `
		SELECT repo_name, SUM(score) AS score, SUM(event_count) AS event_count, MAX(last_event_at) AS last_event_at
		FROM repository_score_snapshots
		GROUP BY repo_name
		ORDER BY score DESC, last_event_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("query historical trending repositories: %w", err)
	}
	defer rows.Close()

	trends := make([]HistoricalTrend, 0)
	rank := 1
	for rows.Next() {
		var trend HistoricalTrend
		if err := rows.Scan(&trend.RepoName, &trend.Score, &trend.EventCount, &trend.LastEventAt); err != nil {
			return nil, fmt.Errorf("scan historical trend: %w", err)
		}
		trend.Rank = rank
		rank++
		trends = append(trends, trend)
	}

	return trends, rows.Err()
}

func (s *Store) AggregationWindows(ctx context.Context, limit int) ([]AggregationWindow, error) {
	if limit <= 0 {
		limit = 24
	}

	rows, err := s.pool.Query(ctx, `
		SELECT w.window_start, w.window_end, w.window_size,
		       COALESCE(SUM(s.event_count), 0) AS event_count,
		       COUNT(DISTINCT s.repo_name) AS repo_count
		FROM aggregation_windows w
		LEFT JOIN repository_score_snapshots s ON s.window_start = w.window_start
		GROUP BY w.window_start, w.window_end, w.window_size
		ORDER BY w.window_start DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("query aggregation windows: %w", err)
	}
	defer rows.Close()

	windows := make([]AggregationWindow, 0)
	for rows.Next() {
		var window AggregationWindow
		if err := rows.Scan(&window.WindowStart, &window.WindowEnd, &window.WindowSize, &window.EventCount, &window.RepoCount); err != nil {
			return nil, fmt.Errorf("scan aggregation window: %w", err)
		}
		windows = append(windows, window)
	}

	return windows, rows.Err()
}

func insertProcessedEvent(ctx context.Context, tx pgx.Tx, event events.NormalizedEvent, score float64, eventTime time.Time) (bool, error) {
	tag, err := tx.Exec(ctx, `
		INSERT INTO processed_event_metadata (
			event_id, event_type, repo_id, repo_name, repo_url, actor_id, actor_login,
			score, event_created_at, ingested_at, processed_at
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
		ON CONFLICT (event_id) DO NOTHING
	`, event.EventID, event.EventType, event.RepoID, event.RepoName, event.RepoURL, event.ActorID, event.ActorLogin, score, eventTime, event.IngestedAt)
	if err != nil {
		return false, fmt.Errorf("insert processed event metadata: %w", err)
	}

	return tag.RowsAffected() == 1, nil
}

func upsertWindow(ctx context.Context, tx pgx.Tx, windowStart, windowEnd time.Time, windowSize string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO aggregation_windows (window_start, window_end, window_size)
		VALUES ($1,$2,$3)
		ON CONFLICT (window_start, window_size) DO UPDATE
		SET window_end = EXCLUDED.window_end
	`, windowStart, windowEnd, windowSize)
	if err != nil {
		return fmt.Errorf("upsert aggregation window: %w", err)
	}
	return nil
}

func upsertRepoSnapshot(ctx context.Context, tx pgx.Tx, event events.NormalizedEvent, score float64, windowStart, windowEnd, eventTime time.Time) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO repository_score_snapshots (
			repo_name, repo_id, repo_url, window_start, window_end, score, event_count, rank, last_event_at, updated_at
		)
		VALUES ($1,$2,$3,$4,$5,$6,1,0,$7,NOW())
		ON CONFLICT (repo_name, window_start) DO UPDATE
		SET score = repository_score_snapshots.score + EXCLUDED.score,
		    event_count = repository_score_snapshots.event_count + 1,
		    repo_id = EXCLUDED.repo_id,
		    repo_url = EXCLUDED.repo_url,
		    last_event_at = GREATEST(repository_score_snapshots.last_event_at, EXCLUDED.last_event_at),
		    updated_at = NOW()
	`, event.RepoName, event.RepoID, event.RepoURL, windowStart, windowEnd, score, eventTime)
	if err != nil {
		return fmt.Errorf("upsert repository score snapshot: %w", err)
	}
	return nil
}

func upsertHourlySummary(ctx context.Context, tx pgx.Tx, event events.NormalizedEvent, score float64, windowStart, windowEnd, eventTime time.Time) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO repository_hourly_summaries (
			repo_name, hour_start, hour_end, score, event_count, last_event_at, updated_at
		)
		VALUES ($1,$2,$3,$4,1,$5,NOW())
		ON CONFLICT (repo_name, hour_start) DO UPDATE
		SET score = repository_hourly_summaries.score + EXCLUDED.score,
		    event_count = repository_hourly_summaries.event_count + 1,
		    last_event_at = GREATEST(repository_hourly_summaries.last_event_at, EXCLUDED.last_event_at),
		    updated_at = NOW()
	`, event.RepoName, windowStart, windowEnd, score, eventTime)
	if err != nil {
		return fmt.Errorf("upsert repository hourly summary: %w", err)
	}
	return nil
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS processed_event_metadata (
	event_id TEXT PRIMARY KEY,
	event_type TEXT NOT NULL,
	repo_id BIGINT,
	repo_name TEXT NOT NULL,
	repo_url TEXT,
	actor_id BIGINT,
	actor_login TEXT,
	score DOUBLE PRECISION NOT NULL,
	event_created_at TIMESTAMPTZ NOT NULL,
	ingested_at TIMESTAMPTZ,
	processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aggregation_windows (
	window_start TIMESTAMPTZ NOT NULL,
	window_end TIMESTAMPTZ NOT NULL,
	window_size TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	PRIMARY KEY (window_start, window_size)
);

CREATE TABLE IF NOT EXISTS repository_score_snapshots (
	repo_name TEXT NOT NULL,
	repo_id BIGINT,
	repo_url TEXT,
	window_start TIMESTAMPTZ NOT NULL,
	window_end TIMESTAMPTZ NOT NULL,
	score DOUBLE PRECISION NOT NULL DEFAULT 0,
	event_count BIGINT NOT NULL DEFAULT 0,
	rank INTEGER NOT NULL DEFAULT 0,
	last_event_at TIMESTAMPTZ NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	PRIMARY KEY (repo_name, window_start)
);

CREATE TABLE IF NOT EXISTS repository_hourly_summaries (
	repo_name TEXT NOT NULL,
	hour_start TIMESTAMPTZ NOT NULL,
	hour_end TIMESTAMPTZ NOT NULL,
	score DOUBLE PRECISION NOT NULL DEFAULT 0,
	event_count BIGINT NOT NULL DEFAULT 0,
	last_event_at TIMESTAMPTZ NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	PRIMARY KEY (repo_name, hour_start)
);

CREATE INDEX IF NOT EXISTS idx_processed_event_metadata_repo_time
	ON processed_event_metadata (repo_name, event_created_at DESC);

CREATE INDEX IF NOT EXISTS idx_repository_score_snapshots_window_score
	ON repository_score_snapshots (window_start DESC, score DESC);

CREATE INDEX IF NOT EXISTS idx_repository_hourly_summaries_repo_hour
	ON repository_hourly_summaries (repo_name, hour_start DESC);
`
