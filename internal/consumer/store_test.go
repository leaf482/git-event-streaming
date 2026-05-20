package consumer

import "testing"

func TestScoreForEvent(t *testing.T) {
	tests := map[string]float64{
		"WatchEvent":       3,
		"ForkEvent":        2,
		"PullRequestEvent": 2,
		"PushEvent":        1,
		"IssuesEvent":      1,
		"CreateEvent":      1,
		"DeleteEvent":      0,
	}

	for eventType, expectedScore := range tests {
		if score := scoreForEvent(eventType); score != expectedScore {
			t.Fatalf("scoreForEvent(%q) = %v, want %v", eventType, score, expectedScore)
		}
	}
}
