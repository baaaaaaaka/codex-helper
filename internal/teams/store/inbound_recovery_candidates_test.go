package store

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestInboundRecoveryCandidatesAcrossBackends(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	for _, useSQLite := range []bool{false, true} {
		useSQLite := useSQLite
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			store := newTestStore(t)
			if err := store.Update(ctx, func(state *State) error {
				state.Turns["turn-linked-terminal"] = Turn{ID: "turn-linked-terminal", Status: TurnStatusCompleted, CreatedAt: now}
				state.InboundEvents["deferred-linked"] = InboundEvent{
					ID: "deferred-linked", TeamsChatID: "chat-a", TeamsMessageID: "message-1",
					Status: InboundStatusDeferred, TurnID: "turn-linked-terminal", CreatedAt: now,
				}
				state.InboundEvents["deferred-unlinked"] = InboundEvent{
					ID: "deferred-unlinked", TeamsChatID: "chat-a", TeamsMessageID: "message-2",
					Status: InboundStatusDeferred, CreatedAt: now.Add(time.Second),
				}
				state.InboundEvents["deferred-due"] = InboundEvent{
					ID: "deferred-due", TeamsChatID: "chat-a", TeamsMessageID: "message-2b",
					Status: InboundStatusDeferred, NextAttemptAt: time.Now().Add(-time.Second), CreatedAt: now.Add(1500 * time.Millisecond),
				}
				state.InboundEvents["deferred-future"] = InboundEvent{
					ID: "deferred-future", TeamsChatID: "chat-a", TeamsMessageID: "message-2c",
					Status: InboundStatusDeferred, NextAttemptAt: time.Now().Add(time.Hour), CreatedAt: now.Add(1500 * time.Millisecond),
				}
				state.InboundEvents["persisted-orphan"] = InboundEvent{
					ID: "persisted-orphan", TeamsChatID: "chat-b", TeamsMessageID: "message-3",
					Status: InboundStatusPersisted, CreatedAt: now.Add(2 * time.Second),
				}
				state.InboundEvents["queued-orphan"] = InboundEvent{
					ID: "queued-orphan", TeamsChatID: "chat-b", TeamsMessageID: "message-4",
					Status: InboundStatusQueued, CreatedAt: now.Add(3 * time.Second),
				}
				state.InboundEvents["persisted-due"] = InboundEvent{
					ID: "persisted-due", TeamsChatID: "chat-f", TeamsMessageID: "message-9",
					Status: InboundStatusPersisted, NextAttemptAt: time.Now().UTC().Add(-time.Second), CreatedAt: now.Add(8 * time.Second),
				}
				state.InboundEvents["queued-due"] = InboundEvent{
					ID: "queued-due", TeamsChatID: "chat-f", TeamsMessageID: "message-10",
					Status: InboundStatusQueued, NextAttemptAt: time.Now().UTC().Add(-time.Second), CreatedAt: now.Add(9 * time.Second),
				}
				state.InboundEvents["persisted-future"] = InboundEvent{
					ID: "persisted-future", TeamsChatID: "chat-g", TeamsMessageID: "message-11",
					Status: InboundStatusPersisted, NextAttemptAt: time.Now().UTC().Add(time.Hour), CreatedAt: now.Add(10 * time.Second),
				}
				state.InboundEvents["queued-future"] = InboundEvent{
					ID: "queued-future", TeamsChatID: "chat-g", TeamsMessageID: "message-12",
					Status: InboundStatusQueued, NextAttemptAt: time.Now().UTC().Add(time.Hour), CreatedAt: now.Add(11 * time.Second),
				}
				state.InboundEvents["persisted-linked"] = InboundEvent{
					ID: "persisted-linked", TeamsChatID: "chat-c", TeamsMessageID: "message-5",
					Status: InboundStatusPersisted, TurnID: "turn-linked-terminal", CreatedAt: now.Add(4 * time.Second),
				}
				state.InboundEvents["queued-linked"] = InboundEvent{
					ID: "queued-linked", TeamsChatID: "chat-c", TeamsMessageID: "message-6",
					Status: InboundStatusQueued, TurnID: "turn-linked-terminal", CreatedAt: now.Add(5 * time.Second),
				}
				state.InboundEvents["ignored-orphan"] = InboundEvent{
					ID: "ignored-orphan", TeamsChatID: "chat-d", TeamsMessageID: "message-7",
					Status: InboundStatusIgnored, CreatedAt: now.Add(6 * time.Second),
				}
				state.InboundEvents["registry-migration"] = InboundEvent{
					ID: "registry-migration", TeamsChatID: "chat-e", TeamsMessageID: "message-8",
					Source: "registry_migration", Status: InboundStatusPersisted, CreatedAt: now.Add(7 * time.Second),
				}
				return nil
			}); err != nil {
				t.Fatalf("seed inbound recovery candidates: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}

			fullLoads := 0
			previousHook := sqliteStateLoadTestHook
			if useSQLite {
				sqliteStateLoadTestHook = func() { fullLoads++ }
				t.Cleanup(func() { sqliteStateLoadTestHook = previousHook })
			}
			got, err := store.InboundRecoveryCandidates(ctx)
			if err != nil {
				t.Fatalf("InboundRecoveryCandidates: %v", err)
			}
			ids := make([]string, 0, len(got))
			for _, event := range got {
				ids = append(ids, event.ID)
			}
			want := []string{"deferred-linked", "deferred-unlinked", "deferred-due", "persisted-orphan", "queued-orphan", "persisted-due", "queued-due"}
			if !reflect.DeepEqual(ids, want) {
				t.Fatalf("InboundRecoveryCandidates ids = %#v, want %#v", ids, want)
			}
			limited, err := store.InboundRecoveryCandidatesWithLimit(ctx, 2)
			if err != nil {
				t.Fatalf("InboundRecoveryCandidatesWithLimit: %v", err)
			}
			limitedIDs := make([]string, 0, len(limited))
			for _, event := range limited {
				limitedIDs = append(limitedIDs, event.ID)
			}
			if wantLimited := []string{"deferred-linked", "deferred-unlinked"}; !reflect.DeepEqual(limitedIDs, wantLimited) {
				t.Fatalf("InboundRecoveryCandidatesWithLimit ids = %#v, want %#v", limitedIDs, wantLimited)
			}
			if useSQLite && fullLoads != 0 {
				t.Fatalf("SQLite recovery candidate query invoked full loader %d time(s)", fullLoads)
			}
		})
	}
}

func TestSQLiteInboundRecoveryCandidatesLimitDoesNotDecodeTail(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := store.Update(ctx, func(state *State) error {
		for i := 0; i < 3; i++ {
			id := fmt.Sprintf("inbound:bounded:%d", i)
			state.InboundEvents[id] = InboundEvent{
				ID: id, TeamsChatID: "bounded-chat", TeamsMessageID: id,
				Status: InboundStatusPersisted, CreatedAt: now.Add(time.Duration(i) * time.Second),
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed bounded candidates: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE inbound_events SET json = ? WHERE id = ?`, []byte(`{"id":"inbound:bounded:2"`), "inbound:bounded:2")
		return err
	})

	candidates, err := store.InboundRecoveryCandidatesWithLimit(ctx, 2)
	if err != nil {
		t.Fatalf("bounded InboundRecoveryCandidates: %v", err)
	}
	if got := len(candidates); got != 2 {
		t.Fatalf("bounded candidate count = %d, want 2", got)
	}
	for i, candidate := range candidates {
		wantID := fmt.Sprintf("inbound:bounded:%d", i)
		if candidate.ID != wantID {
			t.Fatalf("bounded candidate[%d] = %#v, want id %q", i, candidate, wantID)
		}
	}
}

func TestSQLiteInboundRecoveryCandidatesUsesOrphanOrderIndex(t *testing.T) {
	store := newSQLiteTestStore(t)
	ctx := context.Background()
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		turnID := sqliteSafeJSONExtract("json", "$.turn_id")
		recoveryDue := sqliteInboundDeferredDueSQL("json")
		registryMigration := sqliteInboundRegistryMigrationWithoutTurnSQL("json")
		query := `EXPLAIN QUERY PLAN SELECT json FROM inbound_events
			WHERE status = 'persisted' AND ` + recoveryDue + `
			  AND trim(COALESCE(` + turnID + `, '')) = ''
			  AND COALESCE(NOT (` + registryMigration + `), 1)
			ORDER BY teams_chat_id, created_at, teams_message_id LIMIT 8`
		rows, err := tx.QueryContext(ctx, query, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		defer rows.Close()
		var details []string
		for rows.Next() {
			var id int
			var parent, notUsed, detail string
			if err := rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
				return err
			}
			details = append(details, detail)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		for _, detail := range details {
			if strings.Contains(detail, "inbound_recovery_nonregistry_order_idx") {
				return nil
			}
		}
		return fmt.Errorf("recovery query plan = %v, want inbound_recovery_nonregistry_order_idx", details)
	})
}

func TestSQLiteSelectedColdStateDoesNotDecodeNativeRowMaps(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.ControlChat = ControlChatBinding{TeamsChatID: "control-selected"}
		for i := 0; i < 64; i++ {
			id := fmt.Sprintf("selected-inbound-%03d", i)
			state.InboundEvents[id] = InboundEvent{
				ID: id, TeamsChatID: "chat-selected", TeamsMessageID: id,
				Status: InboundStatusPersisted,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed selected-state fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		selected, err := loadSQLiteSelectedColdStateWithoutRowMaps(ctx, tx, stateFieldSet("control_chat", "inbound_events"))
		if err != nil {
			return err
		}
		if selected.ControlChat.TeamsChatID != "control-selected" {
			return fmt.Errorf("selected control chat = %q, want control-selected", selected.ControlChat.TeamsChatID)
		}
		if len(selected.InboundEvents) != 0 {
			return fmt.Errorf("selected cold state decoded %d native inbound rows", len(selected.InboundEvents))
		}
		return nil
	})
}

func TestSQLiteSchemaPreparationRepairsRecoveryIndexAfterOlderMarker(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteTestStore(t)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DROP INDEX IF EXISTS inbound_recovery_nonregistry_order_idx`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = '3' WHERE key = ?`, sqliteSchemaPreparationVersionKey)
		return err
	})
	if err := store.PrepareSQLiteSchemaBeforeOwner(ctx); err != nil {
		t.Fatalf("prepare schema after older marker: %v", err)
	}
	var marker string
	if err := withSQLiteRawQueryForTest(store, `SELECT value FROM state_meta WHERE key = ?`, func(raw []byte) error {
		marker = string(raw)
		return nil
	}, sqliteSchemaPreparationVersionKey); err != nil {
		t.Fatalf("read repaired schema marker: %v", err)
	}
	if marker != sqliteSchemaPreparationVersion {
		t.Fatalf("schema marker = %q, want %q", marker, sqliteSchemaPreparationVersion)
	}
	if err := withSQLiteRawQueryForTest(store, `SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'inbound_recovery_nonregistry_order_idx'`, func(raw []byte) error {
		if string(raw) != "1" {
			return fmt.Errorf("recovery index query returned %q", raw)
		}
		return nil
	}); err != nil {
		t.Fatalf("read repaired recovery index: %v", err)
	}
}

func TestInboundRecoveryCandidateDoesNotAdmitUnknownStatus(t *testing.T) {
	for _, status := range []InboundStatus{"", "unknown", InboundStatusIgnored} {
		t.Run(fmt.Sprintf("status=%q", status), func(t *testing.T) {
			if inboundRecoveryCandidate(InboundEvent{Status: status}) {
				t.Fatalf("inboundRecoveryCandidate(%q) = true, want false", status)
			}
		})
	}
}

func TestSQLiteInboundRecoveryFutureGatesDoNotHideDueRows(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := store.Update(ctx, func(state *State) error {
		state.InboundEvents["inbound:due"] = InboundEvent{
			ID: "inbound:due", TeamsChatID: "chat-due", TeamsMessageID: "message-due",
			Status: InboundStatusDeferred, CreatedAt: now, UpdatedAt: now,
		}
		for i := 0; i < 256; i++ {
			id := fmt.Sprintf("inbound:future:%03d", i)
			state.InboundEvents[id] = InboundEvent{
				ID: id, TeamsChatID: "chat-future", TeamsMessageID: id,
				Status: InboundStatusDeferred, NextAttemptAt: now.Add(24 * time.Hour),
				CreatedAt: now.Add(time.Duration(i+1) * time.Millisecond), UpdatedAt: now,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed future-gated inbound rows: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	candidates, err := store.InboundRecoveryCandidates(ctx)
	if err != nil {
		t.Fatalf("InboundRecoveryCandidates: %v", err)
	}
	if len(candidates) != 1 || candidates[0].ID != "inbound:due" {
		t.Fatalf("future-gated recovery candidates = %#v, want only due row", candidates)
	}
}

func TestInboundRecoveryRetryGateSurvivesSQLiteReopen(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	future := time.Now().UTC().Add(time.Hour)
	event := InboundEvent{
		ID: "deferred-retry-restart", TeamsChatID: "retry-chat", TeamsMessageID: "retry-message",
		Status: InboundStatusDeferred, NextAttemptAt: future, FailureCount: 3,
		LastError: "Graph GET failed: HTTP 429 Too Many Requests", CreatedAt: time.Now().UTC(),
	}
	if err := store.Update(ctx, func(state *State) error {
		state.InboundEvents[event.ID] = event
		return nil
	}); err != nil {
		t.Fatalf("seed deferred retry row: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	path := store.Path()
	if err := store.Close(); err != nil {
		t.Fatalf("close before deferred retry restart: %v", err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen deferred retry store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	candidates, err := reopened.InboundRecoveryCandidates(ctx)
	if err != nil {
		t.Fatalf("future-gated candidates after reopen: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("future-gated candidates after reopen = %#v, want none", candidates)
	}
	if _, _, err := reopened.UpdateInboundEvent(ctx, event.ID, func(current InboundEvent, found bool, now time.Time) (InboundEvent, bool, error) {
		if !found {
			t.Fatalf("deferred retry row disappeared after reopen")
		}
		current.NextAttemptAt = time.Time{}
		return current, true, nil
	}); err != nil {
		t.Fatalf("open retry gate: %v", err)
	}
	candidates, err = reopened.InboundRecoveryCandidates(ctx)
	if err != nil {
		t.Fatalf("due candidates after reopen: %v", err)
	}
	if len(candidates) != 1 || candidates[0].ID != event.ID || candidates[0].FailureCount != event.FailureCount || candidates[0].LastError != event.LastError {
		t.Fatalf("due candidates after reopen = %#v, want persisted retry metadata", candidates)
	}
}
