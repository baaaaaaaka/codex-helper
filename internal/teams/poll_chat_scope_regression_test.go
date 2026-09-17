package teams

import (
	"context"
	"testing"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestPollChatTurnQueueStatesAggregatesSharedChatSessions(t *testing.T) {
	state := teamstore.State{
		Sessions: map[string]teamstore.SessionContext{
			"selected-session": {ID: "selected-session", TeamsChatID: "shared-chat"},
			"sibling-session":  {ID: "sibling-session", TeamsChatID: "shared-chat"},
			"other-session":    {ID: "other-session", TeamsChatID: "other-chat"},
		},
		Turns: map[string]teamstore.Turn{
			"queued-sibling": {ID: "queued-sibling", SessionID: "sibling-session", Status: teamstore.TurnStatusQueued},
			"running-other":  {ID: "running-other", SessionID: "other-session", Status: teamstore.TurnStatusRunning},
			"completed":      {ID: "completed", SessionID: "selected-session", Status: teamstore.TurnStatusCompleted},
		},
	}

	got := pollChatTurnQueueStates(state)
	shared := got["shared-chat"]
	if shared.Queued != 1 || shared.Running {
		t.Fatalf("shared chat queue state=%#v, want one queued and no running turn", shared)
	}
	if !runningPollChats(state)["shared-chat"] {
		t.Fatal("queued sibling did not fence the shared chat")
	}
	other := got["other-chat"]
	if !other.Running || other.Queued != 0 {
		t.Fatalf("other chat queue state=%#v, want one running turn", other)
	}
	if runningPollChats(state)["completed-chat"] {
		t.Fatal("completed-only session unexpectedly fenced a chat")
	}
}

func TestPollSessionIdentityDoesNotFallBackToSharedChatSibling(t *testing.T) {
	bridge := &Bridge{reg: Registry{Sessions: []Session{
		{ID: "sibling-session", ChatID: "shared-chat", Status: "active"},
		{ID: "selected-session", ChatID: "shared-chat", Status: "active"},
	}}}
	ctx := withPollSessionIdentity(context.Background(), "shared-chat", "selected-session")
	selected := bridge.sessionByChatIDForPollContext(ctx, "shared-chat")
	if selected == nil || selected.ID != "selected-session" {
		t.Fatalf("selected shared-chat session = %#v, want selected-session", selected)
	}

	// A selected session that was closed while Graph was in flight is also
	// stale. The shared chat must not make the poll resume under a closed
	// session or silently choose its active sibling.
	bridge.reg.Sessions[1].Status = "closed"
	if got := bridge.sessionByChatIDForPollContext(ctx, "shared-chat"); got != nil {
		t.Fatalf("closed selected session remained pollable: %#v", got)
	}
	bridge.reg.Sessions[1].Status = "active"

	// A rebind/close during the Graph request must not make the safety check
	// silently choose the sibling that happens to be first in Registry.Sessions.
	bridge.reg.Sessions[1].ChatID = "new-chat"
	if got := bridge.sessionByChatIDForPollContext(ctx, "shared-chat"); got != nil {
		t.Fatalf("stale selected session fell back to shared-chat sibling: %#v", got)
	}
}
