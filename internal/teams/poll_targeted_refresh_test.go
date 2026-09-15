package teams

import (
	"testing"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestMergeHotPollSelectedStateReplacesOnlyAdmittedScope(t *testing.T) {
	dst := teamstore.State{
		Sessions: map[string]teamstore.SessionContext{
			"selected-session":   {ID: "selected-session", TeamsChatID: "selected-chat"},
			"unselected-session": {ID: "unselected-session", TeamsChatID: "unselected-chat"},
		},
		ChatPolls: map[string]teamstore.ChatPollState{
			"control-chat":    {ChatID: "control-chat", PollState: "warm"},
			"selected-chat":   {ChatID: "selected-chat", PollState: "hot"},
			"unselected-chat": {ChatID: "unselected-chat", PollState: "warm"},
		},
		Turns: map[string]teamstore.Turn{
			"selected-running":   {ID: "selected-running", SessionID: "selected-session", Status: teamstore.TurnStatusRunning},
			"unselected-running": {ID: "unselected-running", SessionID: "unselected-session", Status: teamstore.TurnStatusRunning},
		},
		ImportCheckpoints: map[string]teamstore.ImportCheckpoint{
			"selected-checkpoint":   {ID: "selected-checkpoint", SessionID: "selected-session"},
			"unselected-checkpoint": {ID: "unselected-checkpoint", SessionID: "unselected-session"},
		},
	}
	refreshed := teamstore.State{
		ChatPolls: map[string]teamstore.ChatPollState{
			"control-chat":  {ChatID: "control-chat", PollState: "cool"},
			"selected-chat": {ChatID: "selected-chat", PollState: "cold"},
		},
		Turns: map[string]teamstore.Turn{
			"selected-new": {ID: "selected-new", SessionID: "selected-session", Status: teamstore.TurnStatusQueued},
		},
		ImportCheckpoints: map[string]teamstore.ImportCheckpoint{
			"selected-new-checkpoint": {ID: "selected-new-checkpoint", SessionID: "selected-session"},
		},
	}

	mergeHotPollSelectedState(&dst, refreshed,
		[]string{"control-chat", "selected-chat", "selected-chat"},
		[]string{"selected-session", "selected-session"},
	)

	if got := dst.ChatPolls["selected-chat"].PollState; got != "cold" {
		t.Fatalf("selected poll state=%q, want fresh value", got)
	}
	if got := dst.ChatPolls["control-chat"].PollState; got != "cool" {
		t.Fatalf("control poll state=%q, want fresh value", got)
	}
	if got := dst.ChatPolls["unselected-chat"].PollState; got != "warm" {
		t.Fatalf("unselected poll state=%q, want unchanged value", got)
	}
	if _, ok := dst.Turns["selected-running"]; ok {
		t.Fatal("stale selected turn was not removed")
	}
	if _, ok := dst.Turns["selected-new"]; !ok {
		t.Fatal("fresh selected turn was not overlaid")
	}
	if _, ok := dst.Turns["unselected-running"]; !ok {
		t.Fatal("unselected turn was changed")
	}
	if _, ok := dst.ImportCheckpoints["selected-checkpoint"]; ok {
		t.Fatal("stale selected checkpoint was not removed")
	}
	if _, ok := dst.ImportCheckpoints["selected-new-checkpoint"]; !ok {
		t.Fatal("fresh selected checkpoint was not overlaid")
	}
	if _, ok := dst.ImportCheckpoints["unselected-checkpoint"]; !ok {
		t.Fatal("unselected checkpoint was changed")
	}
}

func TestMergeHotPollSelectedStateRemovesStaleSharedChatFences(t *testing.T) {
	dst := teamstore.State{
		Sessions: map[string]teamstore.SessionContext{
			"selected-session": {ID: "selected-session", TeamsChatID: "selected-chat"},
			"shared-session":   {ID: "shared-session", TeamsChatID: "selected-chat"},
			"unselected-session": {
				ID: "unselected-session", TeamsChatID: "unselected-chat",
			},
		},
		Turns: map[string]teamstore.Turn{
			"shared-stale-turn": {
				ID: "shared-stale-turn", SessionID: "shared-session", Status: teamstore.TurnStatusRunning,
			},
			"unselected-running": {
				ID: "unselected-running", SessionID: "unselected-session", Status: teamstore.TurnStatusRunning,
			},
		},
		ImportCheckpoints: map[string]teamstore.ImportCheckpoint{
			"shared-stale-checkpoint": {
				ID: "shared-stale-checkpoint", SessionID: "shared-session",
			},
			"unselected-checkpoint": {
				ID: "unselected-checkpoint", SessionID: "unselected-session",
			},
		},
	}
	refreshed := teamstore.State{
		Sessions: map[string]teamstore.SessionContext{
			"selected-session": {ID: "selected-session", TeamsChatID: "selected-chat"},
			"shared-session":   {ID: "shared-session", TeamsChatID: "selected-chat"},
		},
		// The shared session's active turn and importing checkpoint are absent
		// because the selected refresh observed their terminal state.
		Turns:             map[string]teamstore.Turn{},
		ImportCheckpoints: map[string]teamstore.ImportCheckpoint{},
	}

	mergeHotPollSelectedState(&dst, refreshed,
		[]string{"selected-chat"}, []string{"selected-session"})

	if _, ok := dst.Turns["shared-stale-turn"]; ok {
		t.Fatal("stale turn from a second session sharing the selected chat was not removed")
	}
	if _, ok := dst.ImportCheckpoints["shared-stale-checkpoint"]; ok {
		t.Fatal("stale checkpoint from a second session sharing the selected chat was not removed")
	}
	if _, ok := dst.Turns["unselected-running"]; !ok {
		t.Fatal("unselected turn was changed")
	}
	if _, ok := dst.ImportCheckpoints["unselected-checkpoint"]; !ok {
		t.Fatal("unselected checkpoint was changed")
	}
}
