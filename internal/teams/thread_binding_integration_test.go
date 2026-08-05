package teams

import (
	"context"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestBeforeFirstCodexTurnHookBindsDurableSessionAndTurn(t *testing.T) {
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, nil)
	session := bridge.reg.SessionByID("s001")
	if session == nil {
		t.Fatal("test session missing")
	}
	session.ModelGeneration = 2
	turn := teamstore.Turn{ID: "turn-pre-dispatch", SessionID: session.ID, Status: teamstore.TurnStatusRunning, ModelGeneration: 2}
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		state.Sessions[session.ID] = teamstore.SessionContext{ID: session.ID, Status: teamstore.SessionStatusActive, TeamsChatID: session.ChatID, ModelGeneration: 2}
		state.Turns[turn.ID] = turn
		return nil
	}); err != nil {
		t.Fatalf("seed pre-dispatch state: %v", err)
	}
	hook := bridge.beforeFirstCodexTurnHook(session, &turn)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := hook(ctx, codexrunner.ThreadStartInfo{ThreadID: "thread-pre-dispatch"}); err != nil {
		t.Fatalf("pre-dispatch hook: %v", err)
	}
	if err := hook(context.Background(), codexrunner.ThreadStartInfo{ThreadID: "thread-pre-dispatch"}); err != nil {
		t.Fatalf("idempotent pre-dispatch hook: %v", err)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load bound state: %v", err)
	}
	if got := state.Sessions[session.ID].CodexThreadID; got != "thread-pre-dispatch" {
		t.Fatalf("session thread = %q", got)
	}
	if got := state.Turns[turn.ID].CodexThreadID; got != "thread-pre-dispatch" {
		t.Fatalf("turn thread = %q", got)
	}
	if got := turn.CodexThreadID; got != "thread-pre-dispatch" {
		t.Fatalf("local turn thread = %q", got)
	}
	journal, err := bridge.readThreadLinkJournal(context.Background(), session.ID)
	if err != nil {
		t.Fatalf("read pre-dispatch journal: %v", err)
	}
	if len(journal) != 1 || journal[0].Source != "pre_dispatch" || journal[0].TeamsTurnID != turn.ID {
		t.Fatalf("journal = %#v", journal)
	}
	if journal[0].ObservedAt.Before(time.Now().Add(-time.Minute)) {
		t.Fatalf("journal timestamp = %v", journal[0].ObservedAt)
	}
}
