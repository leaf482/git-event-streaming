package github

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/github-pulse/git-event-streaming/internal/events"
)

type Client struct {
	baseURL   string
	token     string
	userAgent string
	http      *http.Client
	logger    *slog.Logger
	etag      string
}

func NewClient(baseURL, token, userAgent string, timeout time.Duration, logger *slog.Logger) *Client {
	return &Client{
		baseURL:   baseURL,
		token:     token,
		userAgent: userAgent,
		http: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (c *Client) PollEvents(ctx context.Context) ([]events.GitHubPublicEvent, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create github events request: %w", err)
	}

	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", c.userAgent)
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if c.etag != "" {
		req.Header.Set("If-None-Match", c.etag)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("poll github events: %w", err)
	}
	defer resp.Body.Close()

	if resp.Header.Get("ETag") != "" {
		c.etag = resp.Header.Get("ETag")
	}

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotModified:
		return nil, nil
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("github events api returned status=%d body=%q", resp.StatusCode, string(body))
	}

	var publicEvents []events.GitHubPublicEvent
	if err := json.NewDecoder(resp.Body).Decode(&publicEvents); err != nil {
		return nil, fmt.Errorf("decode github events response: %w", err)
	}

	c.logger.InfoContext(ctx, "polled github public events", "event_count", len(publicEvents), "etag_set", c.etag != "")
	return publicEvents, nil
}
