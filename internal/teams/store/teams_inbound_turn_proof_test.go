package store

import (
	"context"
	"testing"
	"time"
)

func TestHasTeamsInboundTurnProofIsNarrowAndFailClosed(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name        string
		codexThread string
		codexTurn   string
		chat        string
		source      string
		session     string
		want        bool
	}{
		{name: "matching teams turn", codexThread: "thread-proof", codexTurn: "codex-turn-proof", chat: "chat-proof", source: "teams", session: "session-proof", want: true},
		{name: "wrong codex thread", codexThread: "thread-other", codexTurn: "codex-turn-proof", chat: "chat-proof", source: "teams", session: "session-proof", want: false},
		{name: "wrong codex turn", codexThread: "thread-proof", codexTurn: "codex-turn-other", chat: "chat-proof", source: "teams", session: "session-proof", want: false},
		{name: "wrong chat", codexThread: "thread-proof", codexTurn: "codex-turn-proof", chat: "chat-other", source: "teams", session: "session-proof", want: false},
		{name: "non teams source", codexThread: "thread-proof", codexTurn: "codex-turn-proof", chat: "chat-proof", source: "codex", session: "session-proof", want: false},
		{name: "wrong session", codexThread: "thread-proof", codexTurn: "codex-turn-proof", chat: "chat-proof", source: "teams", session: "session-other", want: false},
	}
	for _, useSQLite := range []bool{false, true} {
		backend := "json"
		if useSQLite {
			backend = "sqlite"
		}
		for _, tc := range cases {
			t.Run(backend+"/"+tc.name, func(t *testing.T) {
				store := newTestStore(t)
				now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
				if err := store.Update(ctx, func(state *State) error {
					state.InboundEvents["inbound-proof"] = InboundEvent{
						ID: "inbound-proof", SessionID: tc.session, TeamsChatID: tc.chat,
						TeamsMessageID: "message-proof", Source: tc.source,
						Status: InboundStatusQueued, TurnID: "turn-proof", CreatedAt: now, UpdatedAt: now,
					}
					state.Turns["turn-proof"] = Turn{
						ID: "turn-proof", SessionID: tc.session, InboundEventID: "inbound-proof",
						Status: TurnStatusCompleted, CodexThreadID: tc.codexThread,
						CodexTurnID: tc.codexTurn, CreatedAt: now, UpdatedAt: now,
					}
					return nil
				}); err != nil {
					t.Fatalf("seed proof state: %v", err)
				}
				if useSQLite {
					migrateStoreToSQLiteForTest(t, store)
				}

				fullLoads := 0
				previousHook := sqliteStateLoadTestHook
				if useSQLite {
					sqliteStateLoadTestHook = func() { fullLoads++ }
				}
				t.Cleanup(func() { sqliteStateLoadTestHook = previousHook })

				got, err := store.HasTeamsInboundTurnProof(ctx, "session-proof", "thread-proof", "codex-turn-proof", "chat-proof")
				if err != nil {
					t.Fatalf("HasTeamsInboundTurnProof: %v", err)
				}
				if got != tc.want {
					t.Fatalf("HasTeamsInboundTurnProof = %v, want %v", got, tc.want)
				}
				if useSQLite && fullLoads != 0 {
					t.Fatalf("SQLite proof invoked full state loader %d time(s)", fullLoads)
				}
			})
		}
	}
}

func TestHasTeamsInboundTurnProofAcceptsLegacyEmptyChat(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.InboundEvents["inbound-empty-chat-proof"] = InboundEvent{
			ID: "inbound-empty-chat-proof", SessionID: "session-empty-chat-proof", Source: "teams",
			TurnID: "turn-empty-chat-proof", Status: InboundStatusQueued,
		}
		state.Turns["turn-empty-chat-proof"] = Turn{
			ID: "turn-empty-chat-proof", SessionID: "session-empty-chat-proof", InboundEventID: "inbound-empty-chat-proof",
			Status: TurnStatusCompleted, CodexThreadID: "thread-empty-chat-proof", CodexTurnID: "codex-empty-chat-proof",
		}
		return nil
	}); err != nil {
		t.Fatalf("seed empty-chat proof state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	got, err := store.HasTeamsInboundTurnProof(ctx, "session-empty-chat-proof", "thread-empty-chat-proof", "codex-empty-chat-proof", "chat-bound-proof")
	if err != nil {
		t.Fatalf("HasTeamsInboundTurnProof empty chat: %v", err)
	}
	if !got {
		t.Fatal("HasTeamsInboundTurnProof rejected a legacy Teams inbound row with an empty chat")
	}
}
