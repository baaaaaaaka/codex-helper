package teams

import (
	"context"
	"strings"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestBridgeRecoversPersistedAndQueuedInboundOrphansAcrossBackends(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		useSQLite := useSQLite
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			graph, _ := newBridgeTestGraph(t)
			store := newBridgeTestStore(t)
			executor := &recordingExecutor{result: ExecutionResult{
				Text:          "recovered answer",
				CodexThreadID: "thread-recovered",
				CodexTurnID:   "codex-turn-recovered",
			}}
			bridge := newBridgeTestBridge(graph, store, executor)
			bridge.asyncTurns = true
			now := time.Now()
			bridge.reg.Sessions = []Session{
				{ID: "orphan-s1", ChatID: "orphan-chat-1", ChatURL: "https://teams.example/orphan-chat-1", Status: "active", CreatedAt: now, UpdatedAt: now},
				{ID: "orphan-s2", ChatID: "orphan-chat-2", ChatURL: "https://teams.example/orphan-chat-2", Status: "active", CreatedAt: now, UpdatedAt: now},
			}
			for _, session := range bridge.reg.Sessions {
				if err := bridge.ensureDurableSession(context.Background(), &session); err != nil {
					t.Fatalf("ensureDurableSession %s: %v", session.ID, err)
				}
			}
			if err := store.Update(context.Background(), func(state *teamstore.State) error {
				state.InboundEvents["orphan-persisted"] = teamstore.InboundEvent{
					ID: "orphan-persisted", SessionID: "orphan-s1", TeamsChatID: "orphan-chat-1",
					TeamsMessageID: "orphan-message-1", Text: "persisted orphan prompt", Source: "teams",
					Status: teamstore.InboundStatusPersisted, CreatedAt: now, UpdatedAt: now,
				}
				state.InboundEvents["orphan-queued"] = teamstore.InboundEvent{
					ID: "orphan-queued", SessionID: "orphan-s2", TeamsChatID: "orphan-chat-2",
					TeamsMessageID: "orphan-message-2", Text: "queued orphan prompt", Source: "teams",
					Status: teamstore.InboundStatusQueued, CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
				}
				return nil
			}); err != nil {
				t.Fatalf("seed inbound orphans: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(context.Background(), 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}

			if err := bridge.processDeferredInbound(context.Background()); err != nil {
				t.Fatalf("processDeferredInbound: %v", err)
			}
			deadline := time.Now().Add(bridgeAsyncTestTimeout)
			for executor.promptCount() < 2 && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			if got := executor.promptCount(); got != 2 {
				t.Fatalf("recovered executor prompt count = %d, want 2", got)
			}
			for _, inboundID := range []string{"orphan-persisted", "orphan-queued"} {
				inbound, ok, err := store.InboundEventByID(context.Background(), inboundID)
				if err != nil || !ok {
					t.Fatalf("InboundEventByID(%s): %#v ok=%v err=%v", inboundID, inbound, ok, err)
				}
				if inbound.Status != teamstore.InboundStatusQueued || inbound.TurnID == "" {
					t.Fatalf("recovered inbound %s = %#v, want queued with turn", inboundID, inbound)
				}
			}

			// The next recovery sweep must not execute either message again. The
			// durable turn link, rather than an in-memory seen set, is the dedupe.
			if err := bridge.processDeferredInbound(context.Background()); err != nil {
				t.Fatalf("second processDeferredInbound: %v", err)
			}
			if got := executor.promptCount(); got != 2 {
				t.Fatalf("recovery replayed already-linked inbound; prompt count = %d, want 2", got)
			}
		})
	}
}

func TestBridgeMissingDeferredSessionRemainsRetryableAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newBridgeTestStore(t)
			bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
			if err := bridge.ensureDurableSession(ctx, bridge.reg.SessionByID("s001")); err != nil {
				t.Fatalf("seed unrelated durable session: %v", err)
			}
			inbound := teamstore.InboundEvent{
				ID: "deferred-missing-session-" + name, SessionID: "session-does-not-exist-" + name,
				TeamsChatID: "chat-deferred-missing-session-" + name, TeamsMessageID: "message-deferred-missing-session-" + name,
				Text: "must not be discarded", Source: "teams", Status: teamstore.InboundStatusDeferred,
				CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
			}
			if err := store.Update(ctx, func(state *teamstore.State) error {
				state.InboundEvents[inbound.ID] = inbound
				return nil
			}); err != nil {
				t.Fatalf("seed deferred inbound: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate test store to SQLite: %v", err)
				}
			}
			candidates, err := store.InboundRecoveryCandidates(ctx)
			if err != nil || len(candidates) != 1 {
				t.Fatalf("missing-session recovery candidates = %#v err=%v, want one", candidates, err)
			}
			resolved, err := bridge.sessionForInboundEvent(ctx, candidates[0])
			if err != nil || resolved != nil {
				t.Fatalf("missing-session resolution = %#v err=%v, want nil", resolved, err)
			}

			err = bridge.processDeferredInbound(ctx)
			if err == nil || !strings.Contains(err.Error(), "durable input session") {
				t.Fatalf("missing-session recovery error = %v, want row-local diagnostic", err)
			}
			got, found, err := store.InboundEventByID(ctx, inbound.ID)
			if err != nil || !found {
				t.Fatalf("load missing-session inbound: found=%v err=%v inbound=%#v", found, err, got)
			}
			if got.Status != teamstore.InboundStatusDeferred || got.FailureCount != 1 || !got.NextAttemptAt.After(time.Now()) || !strings.Contains(got.LastError, "durable input session") {
				t.Fatalf("missing-session inbound = %#v, want deferred durable retry", got)
			}

			// The durable retry gate prevents the next immediate recovery pass from
			// repeatedly scanning and rewriting the same orphan.
			if err := bridge.processDeferredInbound(ctx); err != nil {
				t.Fatalf("second immediate missing-session recovery: %v", err)
			}
			gotAgain, _, err := store.InboundEventByID(ctx, inbound.ID)
			if err != nil {
				t.Fatalf("reload missing-session inbound: %v", err)
			}
			if gotAgain.FailureCount != got.FailureCount || !gotAgain.NextAttemptAt.Equal(got.NextAttemptAt) {
				t.Fatalf("immediate retry gate changed: first=%#v second=%#v", got, gotAgain)
			}
		})
	}
}
