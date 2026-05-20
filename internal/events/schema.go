package events

import (
	"encoding/json"
	"errors"
	"time"
)

const SourceGitHubPublicEvents = "github_public_events_api"

type GitHubPublicEvent struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Actor     GitHubActor     `json:"actor"`
	Repo      GitHubRepo      `json:"repo"`
	Payload   json.RawMessage `json:"payload"`
	Public    bool            `json:"public"`
	CreatedAt time.Time       `json:"created_at"`
}

type GitHubActor struct {
	ID    int64  `json:"id"`
	Login string `json:"login"`
}

type GitHubRepo struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
	URL  string `json:"url"`
}

type NormalizedEvent struct {
	SchemaVersion string          `json:"schema_version"`
	Source        string          `json:"source"`
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	ActorID       int64           `json:"actor_id"`
	ActorLogin    string          `json:"actor_login"`
	RepoID        int64           `json:"repo_id"`
	RepoName      string          `json:"repo_name"`
	RepoURL       string          `json:"repo_url"`
	Public        bool            `json:"public"`
	CreatedAt     time.Time       `json:"created_at"`
	IngestedAt    time.Time       `json:"ingested_at"`
	Payload       json.RawMessage `json:"payload"`
}

func NormalizeGitHubEvent(event GitHubPublicEvent, ingestedAt time.Time) (NormalizedEvent, error) {
	if event.ID == "" {
		return NormalizedEvent{}, errors.New("github event id is required")
	}
	if event.Type == "" {
		return NormalizedEvent{}, errors.New("github event type is required")
	}
	if len(event.Payload) == 0 {
		event.Payload = json.RawMessage(`{}`)
	}

	return NormalizedEvent{
		SchemaVersion: "1.0",
		Source:        SourceGitHubPublicEvents,
		EventID:       event.ID,
		EventType:     event.Type,
		ActorID:       event.Actor.ID,
		ActorLogin:    event.Actor.Login,
		RepoID:        event.Repo.ID,
		RepoName:      event.Repo.Name,
		RepoURL:       event.Repo.URL,
		Public:        event.Public,
		CreatedAt:     event.CreatedAt,
		IngestedAt:    ingestedAt,
		Payload:       event.Payload,
	}, nil
}
