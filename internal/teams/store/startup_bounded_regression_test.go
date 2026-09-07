package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestExistingSQLiteStartupBoundedReadsDoNotDecodeBusinessRows protects the
// startup path that was previously observed hanging on a large SQLite-backed
// state file.  The business rows are deliberately made unreadable after the
// pointer is published.  Scope, session, turn-status, migration validation,
// and runtime metadata must still work because each operation has a bounded
// projection of its own.
func TestExistingSQLiteStartupBoundedReadsDoNotDecodeBusinessRows(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	scope := ScopeIdentity{
		ID:        "scope-startup-bounded",
		AccountID: "account-startup-bounded",
		Profile:   "default",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := store.Update(ctx, func(state *State) error {
		state.Scope = scope
		state.MachineIdentity = MachineIdentity{
			ID:        "machine-startup-bounded",
			Label:     "startup-machine",
			AccountID: scope.AccountID,
			Profile:   scope.Profile,
			ScopeID:   scope.ID,
			CreatedAt: now,
			UpdatedAt: now,
		}
		state.ControlChat = ControlChatBinding{
			ScopeID:     scope.ID,
			AccountID:   scope.AccountID,
			Profile:     scope.Profile,
			TeamsChatID: "control-startup-bounded",
			UpdatedAt:   now,
		}
		state.Sessions["session-startup-bounded"] = SessionContext{
			ID:          "session-startup-bounded",
			Status:      SessionStatusActive,
			TeamsChatID: "chat-startup-bounded",
			UpdatedAt:   now,
		}
		state.Turns["turn-completed-startup-bounded"] = Turn{
			ID:        "turn-completed-startup-bounded",
			SessionID: "session-startup-bounded",
			Status:    TurnStatusCompleted,
			UpdatedAt: now,
		}
		state.InboundEvents["inbound-startup-bounded"] = InboundEvent{
			ID:             "inbound-startup-bounded",
			SessionID:      "session-startup-bounded",
			TeamsChatID:    "chat-startup-bounded",
			TeamsMessageID: "message-startup-bounded",
			Status:         InboundStatusPersisted,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		state.OutboxMessages["outbox-startup-bounded"] = OutboxMessage{
			ID:          "outbox-startup-bounded",
			SessionID:   "session-startup-bounded",
			TeamsChatID: "chat-startup-bounded",
			Kind:        "final",
			Status:      OutboxStatusSent,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed startup bounded state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	// Corrupt unbounded rows after migration. The startup operations under test
	// must not touch these rows at all. Use the store helper below instead of a
	// second database handle so the test also exercises the active handle.
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE inbound_events SET json = '{broken-inbound' WHERE id = ?`, "inbound-startup-bounded"); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = '{broken-outbox' WHERE id = ?`, "outbox-startup-bounded")
		return err
	})

	fullLoads := 0
	sqliteStateLoadTestHook = func() { fullLoads++ }
	t.Cleanup(func() { sqliteStateLoadTestHook = nil })

	if sqlite, err := store.IsSQLite(ctx); err != nil || !sqlite {
		t.Fatalf("IsSQLite = %v, want true; err=%v", sqlite, err)
	}
	gotScope, err := store.ReadScope(ctx)
	if err != nil {
		t.Fatalf("ReadScope: %v", err)
	}
	if gotScope != scope {
		t.Fatalf("ReadScope = %#v, want %#v", gotScope, scope)
	}
	gotSessions, err := store.SessionContexts(ctx)
	if err != nil {
		t.Fatalf("SessionContexts: %v", err)
	}
	if got := gotSessions["session-startup-bounded"].TeamsChatID; got != "chat-startup-bounded" {
		t.Fatalf("SessionContexts chat = %q, want chat-startup-bounded", got)
	}
	hasUnfinished, err := store.HasUnfinishedTurns(ctx)
	if err != nil {
		t.Fatalf("HasUnfinishedTurns: %v", err)
	}
	if hasUnfinished {
		t.Fatal("HasUnfinishedTurns = true for a completed-only turn set")
	}
	if _, err := store.RecordScope(ctx, scope); err != nil {
		t.Fatalf("RecordScope unchanged scope: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("idempotent MigrateLargeStateToSQLite: %v", err)
	}
	metadata, err := LoadPathRuntimeMetadataReadOnly(ctx, store.Path())
	if err != nil {
		t.Fatalf("LoadPathRuntimeMetadataReadOnly: %v", err)
	}
	if metadata.MachineIdentity.ID != "machine-startup-bounded" || metadata.MachineIdentity.Label != "startup-machine" {
		t.Fatalf("runtime machine metadata = %#v", metadata.MachineIdentity)
	}
	if metadata.ControlChat.TeamsChatID != "control-startup-bounded" {
		t.Fatalf("runtime control chat = %#v", metadata.ControlChat)
	}
	if fullLoads != 0 {
		t.Fatalf("startup bounded operations invoked the full SQLite loader %d time(s)", fullLoads)
	}
}

func TestLoadPathGlobalOutboundReadOnlySQLiteSkipsUnrelatedRows(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	scope := ScopeIdentity{ID: "scope-global-outbound-bounded", AccountID: "account-global-outbound-bounded", Profile: "default", CreatedAt: now, UpdatedAt: now}
	if err := store.Update(ctx, func(state *State) error {
		state.Scope = scope
		state.MachineIdentity = MachineIdentity{ID: "machine-global-outbound-bounded", ScopeID: scope.ID, AccountID: scope.AccountID, Profile: scope.Profile, CreatedAt: now, UpdatedAt: now}
		state.ControlChat = ControlChatBinding{ScopeID: scope.ID, AccountID: scope.AccountID, Profile: scope.Profile, TeamsChatID: "control-global-outbound-bounded", UpdatedAt: now}
		state.InboundEvents["unrelated-inbound"] = InboundEvent{ID: "unrelated-inbound", TeamsChatID: "chat-unrelated", TeamsMessageID: "message-unrelated", Status: InboundStatusPersisted, CreatedAt: now, UpdatedAt: now}
		state.OutboxMessages["accepted-global-outbound"] = OutboxMessage{ID: "accepted-global-outbound", ScopeID: scope.ID, TeamsChatID: "chat-accepted", TeamsMessageID: "message-accepted", Status: OutboxStatusAccepted, Kind: "progress", CreatedAt: now, UpdatedAt: now}
		state.OutboxMessages["sent-global-outbound"] = OutboxMessage{ID: "sent-global-outbound", ScopeID: scope.ID, TeamsChatID: "chat-sent", TeamsMessageID: "message-sent", Status: OutboxStatusSent, Kind: "final", CreatedAt: now, SentAt: now, UpdatedAt: now}
		state.OutboxMessages["queued-global-outbound"] = OutboxMessage{ID: "queued-global-outbound", ScopeID: scope.ID, TeamsChatID: "chat-queued", Status: OutboxStatusQueued, Kind: "progress", CreatedAt: now, UpdatedAt: now}
		state.MessageProvenance["helper-global-outbound"] = MessageProvenanceRecord{ID: "helper-global-outbound", TeamsChatID: "chat-provenance", TeamsMessageID: "message-provenance", Origin: MessageOriginHelperOutbox, OutboxID: "sent-global-outbound", Kind: "final", CreatedAt: now, UpdatedAt: now}
		state.MessageProvenance["user-global-outbound"] = MessageProvenanceRecord{ID: "user-global-outbound", TeamsChatID: "chat-user", TeamsMessageID: "message-user", Origin: MessageOriginUserInbound, CreatedAt: now, UpdatedAt: now}
		return nil
	}); err != nil {
		t.Fatalf("seed global outbound bounded state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE inbound_events SET json = '{broken-inbound' WHERE id = ?`, "unrelated-inbound"); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = '{broken-queued' WHERE id = ?`, "queued-global-outbound")
		return err
	})

	fullLoads := 0
	sqliteStateLoadTestHook = func() { fullLoads++ }
	t.Cleanup(func() { sqliteStateLoadTestHook = nil })
	snapshot, err := LoadPathGlobalOutboundReadOnly(ctx, store.Path())
	if err != nil {
		t.Fatalf("LoadPathGlobalOutboundReadOnly: %v", err)
	}
	if snapshot.Scope != scope || snapshot.ControlChat.TeamsChatID != "control-global-outbound-bounded" {
		t.Fatalf("bounded global outbound identity = scope:%#v control:%#v", snapshot.Scope, snapshot.ControlChat)
	}
	if len(snapshot.OutboxMessages) != 2 {
		t.Fatalf("bounded outbox rows = %d, want accepted and sent only: %#v", len(snapshot.OutboxMessages), snapshot.OutboxMessages)
	}
	for _, id := range []string{"accepted-global-outbound", "sent-global-outbound"} {
		if _, ok := snapshot.OutboxMessages[id]; !ok {
			t.Fatalf("bounded snapshot missing outbox %q: %#v", id, snapshot.OutboxMessages)
		}
	}
	if _, ok := snapshot.OutboxMessages["queued-global-outbound"]; ok {
		t.Fatal("bounded snapshot included queued outbox")
	}
	if len(snapshot.MessageProvenance) != 3 {
		t.Fatalf("bounded provenance rows = %d, want explicit helper plus accepted/sent projections: %#v", len(snapshot.MessageProvenance), snapshot.MessageProvenance)
	}
	if _, ok := snapshot.MessageProvenance["helper-global-outbound"]; !ok {
		t.Fatalf("bounded snapshot missing helper provenance: %#v", snapshot.MessageProvenance)
	}
	for _, messageID := range []string{"message-accepted", "message-sent"} {
		found := false
		for _, record := range snapshot.MessageProvenance {
			if record.TeamsMessageID == messageID && record.Origin == MessageOriginHelperOutbox {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("bounded snapshot missing outbox-derived provenance %q: %#v", messageID, snapshot.MessageProvenance)
		}
	}
	if fullLoads != 0 {
		t.Fatalf("global outbound bounded read invoked full SQLite loader %d time(s)", fullLoads)
	}
}

func TestMigrateExistingSQLiteFailsClosedWhenRequiredTableIsMissing(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.Scope = ScopeIdentity{ID: "scope-missing-table", CreatedAt: time.Now(), UpdatedAt: time.Now()}
		return nil
	}); err != nil {
		t.Fatalf("seed missing-table state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DROP TABLE inbound_events`)
		return err
	})

	_, err := store.MigrateLargeStateToSQLite(ctx, 0)
	if err == nil {
		t.Fatal("MigrateLargeStateToSQLite accepted SQLite store with missing required table")
	}
	if !errors.Is(err, ErrSQLiteRuntimeProjectionIncomplete) && !strings.Contains(err.Error(), "inbound_events") {
		t.Fatalf("missing-table error = %v, want fail-closed required-table error", err)
	}
}

func TestHasUnfinishedTurnsSQLiteTreatsUnknownIndexedStatusAsActive(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.Turns["turn-unknown-status"] = Turn{
			ID:        "turn-unknown-status",
			SessionID: "session-unknown-status",
			Status:    TurnStatusCompleted,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed unknown-status turn: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE turns SET status = 'future-status' WHERE id = ?`, "turn-unknown-status")
		return err
	})

	fullLoads := 0
	sqliteStateLoadTestHook = func() { fullLoads++ }
	t.Cleanup(func() { sqliteStateLoadTestHook = nil })
	hasUnfinished, err := store.HasUnfinishedTurns(ctx)
	if err != nil {
		t.Fatalf("HasUnfinishedTurns unknown status: %v", err)
	}
	if !hasUnfinished {
		t.Fatal("unknown indexed turn status was treated as terminal")
	}
	if fullLoads != 0 {
		t.Fatalf("HasUnfinishedTurns invoked full SQLite loader %d time(s)", fullLoads)
	}
}

func TestSessionHasTeamsManagedTurnsSQLiteIsTargeted(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["session-latest-turn"] = SessionContext{
			ID:           "session-latest-turn",
			LatestTurnID: "turn-latest-turn",
		}
		state.Sessions["session-json-turn"] = SessionContext{ID: "session-json-turn"}
		state.Sessions["session-clean"] = SessionContext{ID: "session-clean"}
		state.Turns["turn-json-session"] = Turn{ID: "turn-json-session", SessionID: "session-json-turn", Status: TurnStatusCompleted}
		return nil
	}); err != nil {
		t.Fatalf("seed targeted session-turn state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		// Keep the canonical JSON session ID while removing its legacy scalar
		// projection; the targeted query must not lose this old/mixed-version row.
		_, err := tx.ExecContext(ctx, `UPDATE turns SET session_id = NULL WHERE id = ?`, "turn-json-session")
		return err
	})

	fullLoads := 0
	sqliteStateLoadTestHook = func() { fullLoads++ }
	t.Cleanup(func() { sqliteStateLoadTestHook = nil })
	for sessionID, want := range map[string]bool{
		"session-latest-turn": true,
		"session-json-turn":   true,
		"session-clean":       false,
	} {
		got, err := store.SessionHasTeamsManagedTurns(ctx, sessionID)
		if err != nil {
			t.Fatalf("SessionHasTeamsManagedTurns(%q): %v", sessionID, err)
		}
		if got != want {
			t.Fatalf("SessionHasTeamsManagedTurns(%q) = %t, want %t", sessionID, got, want)
		}
	}
	if fullLoads != 0 {
		t.Fatalf("SessionHasTeamsManagedTurns invoked full SQLite loader %d time(s)", fullLoads)
	}
}
