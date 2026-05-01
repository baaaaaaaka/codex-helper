package teams

import (
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestLatestRetryableTurnID(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	state := teamstore.State{
		Turns: map[string]teamstore.Turn{
			"completed": {
				ID:        "completed",
				SessionID: "s001",
				Status:    teamstore.TurnStatusCompleted,
				UpdatedAt: now.Add(3 * time.Minute),
			},
			"old-failed": {
				ID:        "old-failed",
				SessionID: "s001",
				Status:    teamstore.TurnStatusFailed,
				UpdatedAt: now.Add(time.Minute),
			},
			"new-interrupted": {
				ID:            "new-interrupted",
				SessionID:     "s001",
				Status:        teamstore.TurnStatusInterrupted,
				InterruptedAt: now.Add(2 * time.Minute),
			},
			"other-session": {
				ID:        "other-session",
				SessionID: "s002",
				Status:    teamstore.TurnStatusInterrupted,
				UpdatedAt: now.Add(4 * time.Minute),
			},
		},
	}
	got, ok := latestRetryableTurnID(state, "s001")
	if !ok || got != "new-interrupted" {
		t.Fatalf("latestRetryableTurnID = %q ok=%v, want new-interrupted true", got, ok)
	}
	if got, ok := latestRetryableTurnID(state, "missing"); ok || got != "" {
		t.Fatalf("missing latestRetryableTurnID = %q ok=%v, want empty false", got, ok)
	}
}
