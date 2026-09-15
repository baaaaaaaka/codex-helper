package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
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

func TestSQLiteStartupCompatibilityLookupsAvoidUnboundedOutboxAndInboundLoads(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	scope := ScopeIdentity{ID: "scope-startup-compat", AccountID: "account-startup-compat", Profile: "default", CreatedAt: now, UpdatedAt: now}
	completion := OutboxMessage{
		ID:          "outbox:completion-targeted",
		TeamsChatID: "control-startup-compat",
		Kind:        "control-upgrade-complete",
		Body:        "completion body",
		Status:      OutboxStatusSent,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	attachment := OutboxMessage{
		ID:             "outbox:attachment-targeted",
		TeamsChatID:    "chat-startup-compat",
		Kind:           "attachment",
		Body:           "attachment body",
		Status:         OutboxStatusQueued,
		AttachmentPath: filepath.Join(t.TempDir(), ".outbox", "keep.bin"),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := store.Update(ctx, func(state *State) error {
		state.Scope = scope
		state.MachineIdentity = MachineIdentity{ID: "machine-startup-compat", ScopeID: scope.ID, AccountID: scope.AccountID, Profile: scope.Profile, CreatedAt: now, UpdatedAt: now}
		state.ControlChat = ControlChatBinding{ScopeID: scope.ID, AccountID: scope.AccountID, Profile: scope.Profile, TeamsChatID: "control-startup-compat", UpdatedAt: now}
		state.InboundEvents["unrelated-large-inbound"] = InboundEvent{ID: "unrelated-large-inbound", TeamsChatID: "chat-unrelated", Text: strings.Repeat("inbound ", 1024), Status: InboundStatusPersisted, CreatedAt: now, UpdatedAt: now}
		state.OutboxMessages[completion.ID] = completion
		state.OutboxMessages[attachment.ID] = attachment
		return nil
	}); err != nil {
		t.Fatalf("seed startup compatibility state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	// Make the unbounded business row unreadable. The targeted startup helpers
	// must still be able to protect the attachment and adopt the completion
	// notice without materializing that row or the complete outbox table.
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE inbound_events SET json = '{broken-inbound' WHERE id = ?`, "unrelated-large-inbound")
		return err
	})

	fullLoads := 0
	previousHook := sqliteStateLoadTestHook
	sqliteStateLoadTestHook = func() { fullLoads++ }
	t.Cleanup(func() { sqliteStateLoadTestHook = previousHook })

	control, err := store.ReadControlChat(ctx)
	if err != nil {
		t.Fatalf("ReadControlChat: %v", err)
	}
	if control.TeamsChatID != "control-startup-compat" {
		t.Fatalf("ReadControlChat = %#v, want chat %q", control, "control-startup-compat")
	}
	got, found, err := store.FindOutboxMessageByChatKindBodyAfter(ctx, completion.TeamsChatID, completion.Kind, completion.Body, now.Add(-time.Minute))
	if err != nil {
		t.Fatalf("FindOutboxMessageByChatKindBodyAfter: %v", err)
	}
	if !found || got.ID != completion.ID {
		t.Fatalf("targeted completion lookup = %#v found=%t, want %#v/true", got, found, completion)
	}
	paths, err := store.ActiveOutboxAttachmentPaths(ctx)
	if err != nil {
		t.Fatalf("ActiveOutboxAttachmentPaths: %v", err)
	}
	if len(paths) != 1 || paths[0] != attachment.AttachmentPath {
		t.Fatalf("active attachment paths = %#v, want [%q]", paths, attachment.AttachmentPath)
	}
	if fullLoads != 0 {
		t.Fatalf("startup compatibility lookups invoked full SQLite loader %d time(s)", fullLoads)
	}
}

// Startup cleanup must not keep one SQLite transaction open while it walks a
// large unrelated outbox prefix. The owner heartbeat uses the liveness handle
// and must be able to commit between cleanup pages; otherwise a slow cleanup
// can look like a dead owner and cause a takeover/restart loop.
func TestSQLiteLegacyHistoryGateCleanupReleasesBetweenPages(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	ownerAt := testOwnerStart()
	owner := testOwner("startup-cleanup", "", ownerAt)
	if _, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, ownerAt); err != nil {
		t.Fatalf("seed owner heartbeat: %v", err)
	}
	if err := store.Update(ctx, func(state *State) error {
		for i := 0; i < 12; i++ {
			id := fmt.Sprintf("outbox-normal-%02d", i)
			createdAt := ownerAt.Add(time.Duration(i) * time.Second)
			state.OutboxMessages[id] = OutboxMessage{
				ID: id, TeamsChatID: "chat-startup-cleanup", Kind: "final",
				Status: OutboxStatusQueued, CreatedAt: createdAt, UpdatedAt: createdAt,
			}
		}
		legacyAt := ownerAt.Add(12 * time.Second)
		state.OutboxMessages["outbox-legacy-history-gate"] = OutboxMessage{
			ID: "outbox-legacy-history-gate", TeamsChatID: "chat-startup-cleanup",
			Kind: "sync-status-backlog-blocked", Body: "obsolete history gate",
			Status: OutboxStatusQueued, CreatedAt: legacyAt, UpdatedAt: legacyAt,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed startup cleanup outbox: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	firstPageCommitted := make(chan struct{})
	releaseNextPage := make(chan struct{})
	var pauseOnce sync.Once
	previousHook := sqliteLegacyHistoryGatePageTestHook
	sqliteLegacyHistoryGatePageTestHook = func() {
		pauseOnce.Do(func() {
			close(firstPageCommitted)
			<-releaseNextPage
		})
	}
	t.Cleanup(func() {
		sqliteLegacyHistoryGatePageTestHook = previousHook
		select {
		case <-releaseNextPage:
		default:
			close(releaseNextPage)
		}
	})

	type cleanupResult struct {
		retired int
		err     error
	}
	cleanupDone := make(chan cleanupResult, 1)
	go func() {
		retired, err := store.RetireLegacyHistoryGateOutboxForStartup(ctx, 2, 2)
		cleanupDone <- cleanupResult{retired: retired, err: err}
	}()
	select {
	case <-firstPageCommitted:
	case <-time.After(time.Second):
		t.Fatal("startup cleanup did not commit its first page")
	}

	heartbeatCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	heartbeatAt := ownerAt.Add(time.Minute)
	if _, err := store.RecordOwnerHeartbeat(heartbeatCtx, owner, time.Minute, heartbeatAt); err != nil {
		close(releaseNextPage)
		t.Fatalf("owner heartbeat blocked between cleanup pages: %v", err)
	}
	close(releaseNextPage)

	select {
	case result := <-cleanupDone:
		if result.err != nil {
			t.Fatalf("bounded startup cleanup: %v", result.err)
		}
		if result.retired != 0 {
			t.Fatalf("bounded startup cleanup retired %d rows before reaching the later legacy row", result.retired)
		}
	case <-time.After(time.Second):
		t.Fatal("bounded startup cleanup did not finish after releasing the next page")
	}

	pending, err := store.HasPendingLegacyHistoryGateOutbox(ctx)
	if err != nil {
		t.Fatalf("HasPendingLegacyHistoryGateOutbox after bounded cleanup: %v", err)
	}
	if !pending {
		t.Fatal("bounded cleanup unexpectedly removed the later legacy notice")
	}

	retired, err := store.RetireLegacyHistoryGateOutboxForStartup(ctx, 4, 8)
	if err != nil || retired != 1 {
		t.Fatalf("follow-up startup cleanup retired=%d err=%v, want one legacy notice", retired, err)
	}
	pending, err = store.HasPendingLegacyHistoryGateOutbox(ctx)
	if err != nil || pending {
		t.Fatalf("legacy notice after follow-up cleanup pending=%t err=%v", pending, err)
	}
}

func TestSQLiteTurnRecoverySnapshotLoadsOnlyReferencedActiveInboundRows(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 9, 13, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["session-recovery-bounded"] = SessionContext{
			ID:          "session-recovery-bounded",
			Status:      SessionStatusActive,
			TeamsChatID: "chat-recovery-bounded",
			UpdatedAt:   now,
		}
		state.Turns["turn-recovery-queued"] = Turn{
			ID:             "turn-recovery-queued",
			SessionID:      "session-recovery-bounded",
			InboundEventID: "inbound-recovery-target",
			Status:         TurnStatusQueued,
			QueuedAt:       now,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		state.Turns["turn-recovery-completed"] = Turn{
			ID:             "turn-recovery-completed",
			SessionID:      "session-recovery-bounded",
			InboundEventID: "inbound-recovery-history",
			Status:         TurnStatusCompleted,
			CompletedAt:    now,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		state.Turns["turn-recovery-malformed-active"] = Turn{
			ID:        "turn-recovery-malformed-active",
			SessionID: "session-recovery-bounded",
			Status:    TurnStatusRunning,
			CreatedAt: now,
			UpdatedAt: now,
		}
		state.InboundEvents["inbound-recovery-target"] = InboundEvent{
			ID:             "inbound-recovery-target",
			SessionID:      "session-recovery-bounded",
			TeamsChatID:    "chat-recovery-bounded",
			TeamsMessageID: "message-recovery-target",
			Text:           "recover this queued prompt",
			Status:         InboundStatusQueued,
			TurnID:         "turn-recovery-queued",
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		state.InboundEvents["inbound-recovery-history"] = InboundEvent{
			ID:             "inbound-recovery-history",
			SessionID:      "session-recovery-bounded",
			TeamsChatID:    "chat-recovery-bounded",
			TeamsMessageID: "message-recovery-history",
			Text:           strings.Repeat("historical inbound ", 4096),
			Status:         InboundStatusIgnored,
			TurnID:         "turn-recovery-completed",
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed turn recovery state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	// A completed historical inbound row and an active malformed payload are
	// both useful safety fixtures: the first proves that recovery is targeted,
	// while the second proves the active SQL identity still becomes a held
	// turn rather than disappearing when its JSON is unreadable.
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE inbound_events SET json = '{broken-history-inbound' WHERE id = ?`, "inbound-recovery-history"); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE turns SET json = '{broken-active-turn' WHERE id = ?`, "turn-recovery-malformed-active")
		return err
	})

	fullLoads := 0
	previousHook := sqliteStateLoadTestHook
	sqliteStateLoadTestHook = func() { fullLoads++ }
	t.Cleanup(func() { sqliteStateLoadTestHook = previousHook })

	snapshot, err := store.TurnRecoveryStateSnapshot(ctx)
	if err != nil {
		t.Fatalf("TurnRecoveryStateSnapshot: %v", err)
	}
	if _, ok := snapshot.Sessions["session-recovery-bounded"]; !ok {
		t.Fatalf("recovery snapshot missing session binding: %#v", snapshot.Sessions)
	}
	if len(snapshot.Turns) != 2 {
		t.Fatalf("recovery snapshot turns = %d, want queued plus held malformed active turn: %#v", len(snapshot.Turns), snapshot.Turns)
	}
	if snapshot.Turns["turn-recovery-queued"].Status != TurnStatusQueued {
		t.Fatalf("queued recovery turn = %#v", snapshot.Turns["turn-recovery-queued"])
	}
	if held := snapshot.Turns["turn-recovery-malformed-active"]; held.Status != TurnStatusRunning || held.ID == "" || held.SessionID != "session-recovery-bounded" {
		t.Fatalf("malformed active turn was not held conservatively: %#v", held)
	}
	if len(snapshot.InboundEvents) != 1 || snapshot.InboundEvents["inbound-recovery-target"].TeamsMessageID != "message-recovery-target" {
		t.Fatalf("recovery snapshot inbound rows = %#v, want only target inbound", snapshot.InboundEvents)
	}
	if _, ok := snapshot.InboundEvents["inbound-recovery-history"]; ok {
		t.Fatal("recovery snapshot decoded unrelated completed historical inbound row")
	}
	if fullLoads != 0 {
		t.Fatalf("TurnRecoveryStateSnapshot invoked full SQLite loader %d time(s)", fullLoads)
	}
}

func TestSQLiteActiveOutboxAttachmentPathsFailsClosedOnOpaqueRow(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages["outbox:opaque-attachment"] = OutboxMessage{
			ID:          "outbox:opaque-attachment",
			TeamsChatID: "chat-opaque-attachment",
			Kind:        "attachment",
			Status:      OutboxStatusQueued,
			CreatedAt:   time.Now(),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed opaque attachment state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = '{broken-outbox' WHERE id = ?`, "outbox:opaque-attachment")
		return err
	})
	if _, err := store.ActiveOutboxAttachmentPaths(ctx); err == nil {
		t.Fatal("ActiveOutboxAttachmentPaths accepted an opaque outbox row")
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

// A mixed-version writer can update the indexed turn status before the JSON
// payload contains the newer status field.  Recovery must use that scalar as
// a narrow compatibility fallback for omitted/null/empty JSON status values;
// otherwise a queued turn silently disappears from the startup snapshot and
// can remain stranded forever.  This is deliberately tested through both
// HasUnfinishedTurns and the targeted recovery snapshot, not through a full
// state load.
func TestSQLiteTurnRecoveryUsesIndexedStatusWhenJSONStatusIsBlank(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 13, 9, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		jsonState string
	}{
		{name: "omitted", jsonState: `{"id":"turn-status-fallback","session_id":"session-status-fallback"}`},
		{name: "null", jsonState: `{"id":"turn-status-fallback","session_id":"session-status-fallback","status":null}`},
		{name: "empty", jsonState: `{"id":"turn-status-fallback","session_id":"session-status-fallback","status":""}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestStore(t)
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions["session-status-fallback"] = SessionContext{
					ID: "session-status-fallback", Status: SessionStatusActive,
					TeamsChatID: "chat-status-fallback", UpdatedAt: now,
				}
				state.Turns["turn-status-fallback"] = Turn{
					ID: "turn-status-fallback", SessionID: "session-status-fallback",
					Status: TurnStatusQueued, QueuedAt: now, CreatedAt: now, UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed status fallback turn: %v", err)
			}
			migrateStoreToSQLiteForTest(t, store)
			withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `UPDATE turns SET json = ?, status = ? WHERE id = ?`,
					[]byte(tc.jsonState), string(TurnStatusQueued), "turn-status-fallback")
				return err
			})

			hasUnfinished, err := store.HasUnfinishedTurns(ctx)
			if err != nil {
				t.Fatalf("HasUnfinishedTurns: %v", err)
			}
			if !hasUnfinished {
				t.Fatal("HasUnfinishedTurns hid queued turn with blank JSON status")
			}
			snapshot, err := store.TurnRecoveryStateSnapshot(ctx)
			if err != nil {
				t.Fatalf("TurnRecoveryStateSnapshot: %v", err)
			}
			turn, ok := snapshot.Turns["turn-status-fallback"]
			if !ok {
				t.Fatalf("recovery snapshot omitted queued turn: %#v", snapshot.Turns)
			}
			if turn.Status != TurnStatusQueued {
				t.Fatalf("recovery status = %q, want queued; turn=%#v", turn.Status, turn)
			}
		})
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
