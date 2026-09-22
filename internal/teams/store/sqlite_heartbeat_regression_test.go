package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestPrepareOutboxProjectionIsNoopForLegacyJSONStore(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages["json-only-outbox"] = OutboxMessage{
			ID: "json-only-outbox", Status: OutboxStatusQueued,
			CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed legacy JSON store: %v", err)
	}

	if err := store.PrepareOutboxProjection(ctx); err != nil {
		t.Fatalf("PrepareOutboxProjection on legacy JSON store: %v", err)
	}
	if _, ok, err := store.currentSQLitePointerReadOnly(); err != nil {
		t.Fatalf("read SQLite pointer after JSON no-op: %v", err)
	} else if ok {
		t.Fatal("legacy JSON store unexpectedly acquired an SQLite pointer")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(store.Path()), SQLiteFileName)); !os.IsNotExist(err) {
		t.Fatalf("legacy JSON projection unexpectedly created SQLite file, stat err=%v", err)
	}
}

func TestSQLiteSessionTranscriptDedupeSnapshotAvoidsFullStateLoad(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC()
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["session-live"] = SessionContext{
			ID:          "session-live",
			Status:      SessionStatusActive,
			TeamsChatID: "chat-live",
			UpdatedAt:   now,
		}
		state.Turns["turn-live"] = Turn{
			ID:        "turn-live",
			SessionID: "session-live",
			Status:    TurnStatusRunning,
			UpdatedAt: now,
		}
		state.OutboxMessages["outbox-live"] = OutboxMessage{
			ID:          "outbox-live",
			SessionID:   "session-live",
			TurnID:      "turn-live",
			TeamsChatID: "chat-live",
			Kind:        "codex-progress-1",
			Body:        "already delivered live progress",
			Status:      OutboxStatusSent,
			UpdatedAt:   now,
		}
		state.ImportCheckpoints["checkpoint-live"] = ImportCheckpoint{
			ID:        "checkpoint-live",
			SessionID: "session-live",
			Status:    "complete",
			UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed session transcript state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	fullLoads := 0
	previousHook := sqliteStateLoadTestHook
	sqliteStateLoadTestHook = func() { fullLoads++ }
	t.Cleanup(func() { sqliteStateLoadTestHook = previousHook })

	state, err := store.SessionTranscriptDedupeSnapshot(ctx, "session-live", "checkpoint-live")
	if err != nil {
		t.Fatalf("SessionTranscriptDedupeSnapshot: %v", err)
	}
	if fullLoads != 0 {
		t.Fatalf("scoped transcript snapshot invoked full SQLite state loader %d time(s)", fullLoads)
	}
	if got := state.Sessions["session-live"]; got.TeamsChatID != "chat-live" {
		t.Fatalf("scoped transcript snapshot session = %#v, want durable chat binding", got)
	}
	if state.Turns["turn-live"].ID == "" || state.OutboxMessages["outbox-live"].ID == "" {
		t.Fatalf("scoped transcript snapshot omitted selected session state: %#v", state)
	}
}

func TestSQLiteDashboardSnapshotAvoidsFullStateLoad(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC()
	if err := store.Update(ctx, func(state *State) error {
		state.DashboardViews["control-chat"] = DashboardViewRecord{
			ID:          "view-control",
			ChatID:      "control-chat",
			Kind:        "workspaces",
			WorkspaceID: "workspace-1",
			Items: []DashboardViewItem{{
				Number:      1,
				Kind:        "workspace",
				WorkspaceID: "workspace-1",
				Label:       "project",
			}},
			UpdatedAt: now,
		}
		state.DashboardNumbers["number-workspace"] = DashboardNumberRecord{
			ID:          "number-workspace",
			ChatID:      "control-chat",
			Kind:        "workspace",
			Number:      1,
			WorkspaceID: "workspace-1",
			Label:       "project",
			UpdatedAt:   now,
		}
		state.DashboardNumbers["number-other-chat"] = DashboardNumberRecord{
			ID:          "number-other-chat",
			ChatID:      "other-chat",
			Kind:        "workspace",
			Number:      9,
			WorkspaceID: "workspace-9",
			UpdatedAt:   now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed dashboard state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	var dashboardProjection sqliteDashboardProjection
	if err := withSQLiteRawQueryForTest(store, `SELECT value FROM state_meta WHERE key = ?`, func(raw []byte) error {
		return json.Unmarshal(raw, &dashboardProjection)
	}, sqliteDashboardProjectionKey); err != nil {
		t.Fatalf("read dashboard projection: %v", err)
	}
	if dashboardProjection.StateJSONRevision <= 0 || len(dashboardProjection.DashboardViews) != 1 || len(dashboardProjection.DashboardNumbers) != 2 {
		t.Fatalf("dashboard projection = %#v, want revision-fenced records", dashboardProjection)
	}

	fullLoads := 0
	previousHook := sqliteStateLoadTestHook
	sqliteStateLoadTestHook = func() { fullLoads++ }
	t.Cleanup(func() { sqliteStateLoadTestHook = previousHook })

	state, err := store.DashboardStateSnapshot(ctx)
	if err != nil {
		t.Fatalf("DashboardStateSnapshot: %v", err)
	}
	if fullLoads != 0 {
		t.Fatalf("dashboard snapshot invoked full SQLite state loader %d time(s)", fullLoads)
	}
	if got := state.DashboardViews["control-chat"]; got.WorkspaceID != "workspace-1" || len(got.Items) != 1 {
		t.Fatalf("dashboard view snapshot = %#v", got)
	}
	if got := state.DashboardNumbers["number-workspace"]; got.ChatID != "control-chat" || got.Number != 1 {
		t.Fatalf("dashboard number snapshot = %#v", got)
	}
	if _, ok := state.DashboardNumbers["number-other-chat"]; !ok {
		t.Fatal("dashboard snapshot omitted unrelated durable number; filtering belongs to the caller")
	}
}

func TestSQLiteDashboardSnapshotFallsBackFromStaleProjection(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.DashboardViews["control-chat"] = DashboardViewRecord{
			ID:          "view-canonical",
			ChatID:      "control-chat",
			Kind:        "workspaces",
			WorkspaceID: "workspace-canonical",
		}
		return nil
	}); err != nil {
		t.Fatalf("seed dashboard fallback state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	var revision int64
	if err := withSQLiteRawQueryForTest(store, `SELECT value FROM state_meta WHERE key = ?`, func(raw []byte) error {
		var err error
		revision, err = strconv.ParseInt(string(raw), 10, 64)
		return err
	}, sqliteStateJSONRevisionKey); err != nil {
		t.Fatalf("read dashboard fallback revision: %v", err)
	}
	stale, err := json.Marshal(sqliteDashboardProjection{
		DashboardViews: map[string]DashboardViewRecord{
			"control-chat": {ID: "view-stale", ChatID: "control-chat", WorkspaceID: "workspace-stale"},
		},
		StateJSONRevision: revision - 1,
	})
	if err != nil {
		t.Fatalf("marshal stale dashboard projection: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = ?`, stale, sqliteDashboardProjectionKey)
		return err
	})

	state, err := store.DashboardStateSnapshot(ctx)
	if err != nil {
		t.Fatalf("DashboardStateSnapshot with stale projection: %v", err)
	}
	if got := state.DashboardViews["control-chat"].WorkspaceID; got != "workspace-canonical" {
		t.Fatalf("stale dashboard projection won with workspace %q, want canonical", got)
	}
}

func TestSQLiteRuntimeSelectedReadDoesNotRequireColdStateJSON(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 22, 13, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.ControlChat = ControlChatBinding{TeamsChatID: "control-runtime"}
		state.ServiceOwner = &OwnerMetadata{MachineID: "machine-runtime", PID: 4242, LastHeartbeat: now}
		state.Sessions["session-runtime"] = SessionContext{ID: "session-runtime", Status: SessionStatusActive, TeamsChatID: "chat-runtime"}
		state.Turns["turn-runtime"] = Turn{ID: "turn-runtime", SessionID: "session-runtime", Status: TurnStatusQueued, CreatedAt: now}
		state.ChatPolls["chat-runtime"] = ChatPollState{ChatID: "chat-runtime", PollState: "warm", NextPollAt: now}
		state.ImportCheckpoints["checkpoint-runtime"] = ImportCheckpoint{ID: "checkpoint-runtime", SessionID: "session-runtime", Status: "complete"}
		return nil
	}); err != nil {
		t.Fatalf("seed runtime selected fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = 'state_json'`, []byte(`{"broken`))
		return err
	})

	snapshot, err := store.PollScheduleSnapshot(ctx)
	if err != nil {
		t.Fatalf("runtime/native selected read with corrupt cold JSON: %v", err)
	}
	if snapshot.ControlChat.TeamsChatID != "control-runtime" || snapshot.ServiceOwner == nil ||
		snapshot.Sessions["session-runtime"].ID == "" || snapshot.Turns["turn-runtime"].ID == "" ||
		snapshot.ChatPolls["chat-runtime"].ChatID == "" {
		t.Fatalf("runtime/native selected snapshot = %#v", snapshot)
	}
}

func TestJSONDashboardSnapshotUsesSelectedFields(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.DashboardViews["control-chat"] = DashboardViewRecord{
			ID:          "view-control",
			ChatID:      "control-chat",
			Kind:        "sessions",
			WorkspaceID: "workspace-1",
		}
		state.DashboardNumbers["number-session"] = DashboardNumberRecord{
			ID:        "number-session",
			ChatID:    "control-chat",
			Kind:      "session",
			Number:    1,
			SessionID: "session-1",
		}
		return nil
	}); err != nil {
		t.Fatalf("seed legacy dashboard state: %v", err)
	}

	state, err := store.DashboardStateSnapshot(ctx)
	if err != nil {
		t.Fatalf("DashboardStateSnapshot on legacy JSON store: %v", err)
	}
	if got := state.DashboardViews["control-chat"]; got.Kind != "sessions" || got.WorkspaceID != "workspace-1" {
		t.Fatalf("legacy dashboard view snapshot = %#v", got)
	}
	if got := state.DashboardNumbers["number-session"]; got.SessionID != "session-1" || got.Number != 1 {
		t.Fatalf("legacy dashboard number snapshot = %#v", got)
	}
}

// A liveness-only SQLite handle may be opened before the normal migration
// boundary. The explicit pre-owner preparation must upgrade the same database
// before lease claim/foreground use; an already-DB migration then only
// validates the prepared schema and must not rediscover the cold JSON path.
func TestSQLiteAlreadyDBMigrationPreparesSchemaAfterRuntimeHandleOpened(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	seedOfficialReleaseSQLiteStoreForTestWithOptions(t, store, "runtime-first-legacy", officialReleaseSQLiteFixtureOptions{
		LegacyOutboxColumns: true,
	})

	pointer, ok, err := store.currentSQLitePointerReadOnly()
	if err != nil || !ok {
		t.Fatalf("SQLite pointer: ok=%v err=%v", ok, err)
	}
	dbPath, err := store.storeSQLitePath(pointer)
	if err != nil {
		t.Fatalf("SQLite path: %v", err)
	}
	db, err := openSQLiteHandle(dbPath, false)
	if err != nil {
		t.Fatalf("open legacy SQLite fixture: %v", err)
	}
	columns := sqliteColumnSetForTest(t, db, "sessions")
	if columns["canonical_revision"] {
		_ = db.Close()
		t.Fatal("legacy fixture unexpectedly has current session projection column")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close legacy SQLite fixture: %v", err)
	}

	claimNow := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := store.ValidateControlLease(ctx, "runtime-first-probe", 1, claimNow); !errors.Is(err, ErrSQLiteSchemaPreparationRequired) {
		t.Fatalf("ValidateControlLease probe error = %v, want schema-preparation-required", err)
	}
	if err := store.PrepareSQLiteSchemaBeforeOwner(ctx); err != nil {
		t.Fatalf("PrepareSQLiteSchemaBeforeOwner: %v", err)
	}
	owner := testOwner("runtime-first-owner", "", claimNow)
	decision, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
		Scope:   ScopeIdentity{ID: "runtime-first-scope", AccountID: "runtime-first-account", Profile: "default"},
		Machine: MachineRecord{ID: "runtime-first-machine", ScopeID: "runtime-first-scope", Status: MachineStatusActive},
		Owner:   owner, Duration: time.Minute, Now: claimNow,
	})
	if err != nil {
		t.Fatalf("ClaimControlLease opened the runtime handle: %v", err)
	}
	if decision.Mode != LeaseModeActive {
		t.Fatalf("ClaimControlLease mode = %q, want active", decision.Mode)
	}
	// This is the startup order used by the listener: schema preparation is
	// explicit and complete before the lease claim; later owner-scoped work can
	// therefore use the foreground handle without hidden DDL.
	projectionOwner := owner
	projectionOwner.ScopeID = "runtime-first-scope"
	projectionOwner.MachineID = "runtime-first-machine"
	projectionOwner.LeaseGeneration = decision.Lease.Generation
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		rawOwner, err := json.Marshal(&projectionOwner)
		if err != nil {
			return err
		}
		for _, key := range []string{sqliteRuntimeKeyServiceOwner, sqliteRuntimeKeyLockOwner} {
			if _, err := tx.ExecContext(ctx, `UPDATE runtime_state SET json = ? WHERE key = ?`, rawOwner, key); err != nil {
				return err
			}
		}
		return nil
	})
	if err := store.PrepareOutboxProjectionForOwner(ctx, projectionOwner); err != nil {
		t.Fatalf("PrepareOutboxProjectionForOwner after runtime claim: %v", err)
	}
	db, err = openSQLiteHandle(dbPath, false)
	if err != nil {
		t.Fatalf("reopen SQLite fixture after normal operation: %v", err)
	}
	columns = sqliteColumnSetForTest(t, db, "sessions")
	if !columns["canonical_revision"] {
		_ = db.Close()
		t.Fatal("normal operation left runtime-opened SQLite handle on the legacy schema")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close SQLite fixture after normal operation: %v", err)
	}

	result, err := store.MigrateLargeStateToSQLite(ctx, 0)
	if err != nil {
		t.Fatalf("MigrateLargeStateToSQLite after runtime claim: %v", err)
	}
	if !result.AlreadyDB || result.Migrated {
		t.Fatalf("runtime-first migration result = %#v, want already-db", result)
	}

	db, err = openSQLiteHandle(dbPath, false)
	if err != nil {
		t.Fatalf("reopen upgraded SQLite fixture: %v", err)
	}
	defer db.Close()
	for table, want := range map[string][]string{
		"sessions":        {"canonical_revision", "projection_revision", "projection_trusted"},
		"turns":           {"canonical_revision", "projection_revision", "projection_trusted"},
		"chat_polls":      {"frontier_active", "admission_valid", "last_successful_poll_at"},
		"outbox_messages": {"turn_id", "post_send_effects_pending"},
	} {
		columns := sqliteColumnSetForTest(t, db, table)
		for _, column := range want {
			if !columns[column] {
				t.Fatalf("upgraded %s missing column %q; columns=%v", table, column, columns)
			}
		}
	}
	var marker string
	if err := db.QueryRow(`SELECT value FROM state_meta WHERE key = ?`, sqliteChatPollProjectionVersionKey).Scan(&marker); err != nil {
		t.Fatalf("read chat-poll projection marker: %v", err)
	}
	if marker != sqliteChatPollProjectionVersion {
		t.Fatalf("chat-poll projection marker = %q, want %q", marker, sqliteChatPollProjectionVersion)
	}
	var triggerCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name = 'outbox_projection_guard_insert'`).Scan(&triggerCount); err != nil {
		t.Fatalf("count upgraded outbox guard: %v", err)
	}
	if triggerCount != 1 {
		t.Fatalf("upgraded outbox guard trigger count = %d, want 1", triggerCount)
	}
}

// A legacy JSON migration must publish the structural capability before the
// pointer becomes visible to owner-scoped callers.  Otherwise a listener can
// observe the new SQLite pointer and immediately fail its first lease claim,
// even though the migration itself appeared to succeed.
func TestSQLiteLegacyMigrationPublishesSchemaPreparationBeforeClaim(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	seedLegacyStateFileForSQLiteMigrationTest(t, store)

	result := migrateStoreToSQLiteForTest(t, store)
	db, err := openSQLiteStore(result.Path, false)
	if err != nil {
		t.Fatalf("open migrated SQLite store: %v", err)
	}
	var marker string
	if err := db.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, sqliteSchemaPreparationVersionKey).Scan(&marker); err != nil {
		_ = db.Close()
		t.Fatalf("read migration schema marker: %v", err)
	}
	if marker != sqliteSchemaPreparationVersion {
		_ = db.Close()
		t.Fatalf("migration schema marker = %q, want %q", marker, sqliteSchemaPreparationVersion)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close migrated SQLite store: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	decision, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
		Scope:    ScopeIdentity{ID: "migration-claim-scope", AccountID: "migration-claim-account", Profile: "default"},
		Machine:  MachineRecord{ID: "migration-claim-machine", ScopeID: "migration-claim-scope", Status: MachineStatusActive},
		Owner:    testOwner("migration-claim-owner", "", now),
		Duration: time.Minute,
		Now:      now,
	})
	if err != nil {
		t.Fatalf("ClaimControlLease immediately after migration: %v", err)
	}
	if decision.Mode != LeaseModeActive {
		t.Fatalf("ClaimControlLease mode = %q, want active", decision.Mode)
	}
}

// Online legacy migration must not hold the state-file lock while building the
// sidecar.  A heartbeat can therefore keep the lease alive; the exact source
// comparison at the short publication boundary must then reject the stale
// snapshot instead of publishing a database that omits that heartbeat.
func TestSQLiteOwnerMigrationRejectsSourceChangeBeforePointerPublication(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	seedLegacyStateFileForSQLiteMigrationTest(t, store)
	now := time.Now().UTC().Truncate(time.Microsecond)
	scope := ScopeIdentity{ID: "online-migration-scope", AccountID: "online-migration-account", Profile: "default"}
	owner := testOwner("online-migration-owner", "", now)
	owner.ScopeID = scope.ID
	owner.MachineID = "online-migration-machine"
	decision, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
		Scope: scope, Machine: MachineRecord{ID: owner.MachineID, ScopeID: scope.ID, Status: MachineStatusActive},
		Owner: owner, Duration: time.Minute, Now: now,
	})
	if err != nil || decision.Mode != LeaseModeActive {
		t.Fatalf("claim online migration owner: decision=%#v err=%v", decision, err)
	}
	owner.LeaseGeneration = decision.Lease.Generation

	entered := make(chan struct{})
	release := make(chan struct{})
	withSQLiteMigrationTestHook(t, func(stage string) error {
		if stage != sqliteMigrationStageAfterTempVerified {
			return nil
		}
		close(entered)
		<-release
		return nil
	})

	type migrationResult struct {
		result StoreSQLiteMigrationResult
		err    error
	}
	resultCh := make(chan migrationResult, 1)
	go func() {
		result, err := store.MigrateLargeStateToSQLiteForOwner(ctx, owner, 0)
		resultCh <- migrationResult{result: result, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("online migration did not reach the unlocked materialization boundary")
	}

	heartbeatStarted := time.Now()
	if _, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, now.Add(time.Second)); err != nil {
		t.Fatalf("owner heartbeat while online migration materializes: %v", err)
	}
	if elapsed := time.Since(heartbeatStarted); elapsed >= time.Second {
		t.Fatalf("owner heartbeat waited %s behind online migration, want unlocked materialization", elapsed)
	}
	close(release)
	result := <-resultCh
	if !errors.Is(result.err, ErrSQLiteMigrationSourceChanged) {
		t.Fatalf("online migration result=%#v err=%v, want source-change rejection", result.result, result.err)
	}
	if _, ok, err := store.currentSQLitePointerReadOnly(); err != nil {
		t.Fatalf("read pointer after rejected online migration: %v", err)
	} else if ok {
		t.Fatal("stale online migration published an SQLite pointer after source changed")
	}
}

// The owner-aware path must also publish a stable legacy snapshot when the
// source remains unchanged.  The rejection test above protects the CAS
// boundary; this companion test protects the listener's normal startup path
// from silently falling back to JSON forever after the expensive materialize
// phase completes.
func TestSQLiteOwnerMigrationPublishesStableSnapshotForOwner(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	seedLegacyStateFileForSQLiteMigrationTest(t, store)
	now := time.Now().UTC().Truncate(time.Microsecond)
	scope := ScopeIdentity{ID: "online-migration-success-scope", AccountID: "online-migration-success-account", Profile: "default"}
	owner := testOwner("online-migration-success-owner", "", now)
	owner.ScopeID = scope.ID
	owner.MachineID = "online-migration-success-machine"
	decision, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
		Scope: scope, Machine: MachineRecord{ID: owner.MachineID, ScopeID: scope.ID, Status: MachineStatusActive},
		Owner: owner, Duration: time.Minute, Now: now,
	})
	if err != nil || decision.Mode != LeaseModeActive {
		t.Fatalf("claim online migration owner: decision=%#v err=%v", decision, err)
	}
	owner.LeaseGeneration = decision.Lease.Generation

	result, err := store.MigrateLargeStateToSQLiteForOwner(ctx, owner, 0)
	if err != nil {
		t.Fatalf("stable online migration: %v", err)
	}
	if !result.Migrated || result.Path == "" {
		t.Fatalf("stable online migration result=%#v, want published migration", result)
	}
	if _, ok, err := store.currentSQLitePointerReadOnly(); err != nil {
		t.Fatalf("read pointer after stable online migration: %v", err)
	} else if !ok {
		t.Fatal("stable online migration did not publish an SQLite pointer")
	}
	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load stable online migration: %v", err)
	}
	if loaded.Scope.ID != scope.ID || loaded.Scope.AccountID != scope.AccountID || loaded.Scope.Profile != scope.Profile {
		t.Fatalf("loaded scope after stable online migration=%#v, want identity %#v", loaded.Scope, scope)
	}
}

// An active owner must prevent an unowned schema/outbox preparation from
// opening a maintenance writer. The owner heartbeat remains available, while
// a later startup can perform the fenced repair after the lease expires.
func TestSQLiteUnownedSchemaPreparationDoesNotStarveOwnerHeartbeat(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	seedOfficialReleaseSQLiteStoreForTestWithOptions(t, store, "schema-heartbeat", officialReleaseSQLiteFixtureOptions{
		LegacyOutboxColumns: true,
	})
	if err := store.PrepareSQLiteSchemaBeforeOwner(ctx); err != nil {
		t.Fatalf("prepare legacy schema before claim: %v", err)
	}

	claimNow := time.Now().UTC().Truncate(time.Microsecond)
	owner := testOwner("schema-heartbeat-owner", "", claimNow)
	decision, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
		Scope:   ScopeIdentity{ID: "schema-heartbeat-scope", AccountID: "schema-heartbeat-account", Profile: "default"},
		Machine: MachineRecord{ID: "schema-heartbeat-machine", ScopeID: "schema-heartbeat-scope", Status: MachineStatusActive},
		Owner:   owner, Duration: time.Minute, Now: claimNow,
	})
	if err != nil {
		t.Fatalf("ClaimControlLease opened the runtime handle: %v", err)
	}
	if decision.Mode != LeaseModeActive {
		t.Fatalf("ClaimControlLease mode = %q, want active", decision.Mode)
	}
	owner.MachineID = "schema-heartbeat-machine"
	owner.ScopeID = "schema-heartbeat-scope"
	owner.LeaseGeneration = decision.Lease.Generation

	if err := store.PrepareSQLiteSchemaBeforeOwner(ctx); err != nil {
		t.Fatalf("pre-owner preparation with an active owner: %v", err)
	}
	if err := store.PrepareOutboxProjection(ctx); !errors.Is(err, ErrSQLiteOutboxProjectionAuditOwnerRequired) {
		t.Fatalf("unowned outbox preparation error = %v, want owner-required", err)
	}

	heartbeatCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	started := time.Now()
	if _, err := store.RecordOwnerHeartbeat(heartbeatCtx, owner, time.Minute, claimNow.Add(time.Second)); err != nil {
		t.Fatalf("owner heartbeat while schema setup is held: %v", err)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("owner heartbeat took %s while schema setup was held", elapsed)
	}

}

// RecordOwnerHeartbeat remains available to older callers, but it must not
// recreate a retired owner after a newer control-lease generation has taken
// over.  Exercise both outcomes: a visible replacement reports the traditional
// owner conflict, and a cleared replacement reports the lease capability error.
func TestSQLiteLegacyHeartbeatCannotResurrectRetiredLeaseGeneration(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	scope := ScopeIdentity{ID: "legacy-heartbeat-scope", AccountID: "legacy-heartbeat-account", Profile: "default"}

	ownerA := testOwner("legacy-heartbeat-a", "", now)
	ownerA.ScopeID = scope.ID
	ownerA.MachineID = "legacy-heartbeat-machine-a"
	decisionA, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
		Scope:   scope,
		Machine: MachineRecord{ID: ownerA.MachineID, ScopeID: scope.ID, Status: MachineStatusActive},
		Owner:   ownerA, Duration: time.Minute, Now: now,
	})
	if err != nil || decisionA.Mode != LeaseModeActive {
		t.Fatalf("claim generation A: decision=%#v err=%v", decisionA, err)
	}
	ownerA.LeaseGeneration = decisionA.Lease.Generation
	if _, err := store.RecordOwnerHeartbeat(ctx, ownerA, time.Minute, now.Add(time.Second)); err != nil {
		t.Fatalf("heartbeat generation A: %v", err)
	}
	if released, err := store.ReleaseControlLeaseIfHolder(ctx, ownerA.MachineID, ownerA.LeaseGeneration); err != nil || !released {
		t.Fatalf("release generation A: released=%v err=%v", released, err)
	}

	ownerB := testOwner("legacy-heartbeat-b", "", now.Add(2*time.Second))
	ownerB.ScopeID = scope.ID
	ownerB.MachineID = "legacy-heartbeat-machine-b"
	ownerB.PID++
	decisionB, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
		Scope:   scope,
		Machine: MachineRecord{ID: ownerB.MachineID, ScopeID: scope.ID, Status: MachineStatusActive},
		Owner:   ownerB, Duration: time.Minute, Now: now.Add(2 * time.Second),
	})
	if err != nil || decisionB.Mode != LeaseModeActive {
		t.Fatalf("claim generation B: decision=%#v err=%v", decisionB, err)
	}
	ownerB.LeaseGeneration = decisionB.Lease.Generation
	if _, err := store.RecordOwnerHeartbeat(ctx, ownerB, time.Minute, now.Add(3*time.Second)); err != nil {
		t.Fatalf("heartbeat generation B: %v", err)
	}

	if _, err := store.RecordOwnerHeartbeat(ctx, ownerA, time.Minute, now.Add(4*time.Second)); !errors.Is(err, ErrOwnerLive) {
		t.Fatalf("retired heartbeat with visible replacement error = %v, want ErrOwnerLive", err)
	}
	current, found, err := store.ReadOwner(ctx)
	if err != nil || !found {
		t.Fatalf("read replacement owner: found=%v err=%v", found, err)
	}
	if current.MachineID != ownerB.MachineID || current.LeaseGeneration != ownerB.LeaseGeneration {
		t.Fatalf("replacement owner changed after retired heartbeat: %#v", current)
	}

	if cleared, err := store.ClearOwnerIfSame(ctx, ownerB); err != nil || !cleared {
		t.Fatalf("clear replacement owner: cleared=%v err=%v", cleared, err)
	}
	if _, err := store.RecordOwnerHeartbeat(ctx, ownerA, time.Minute, now.Add(5*time.Second)); !errors.Is(err, ErrControlLeaseNotHeld) {
		t.Fatalf("retired heartbeat after replacement clear error = %v, want lease-not-held", err)
	}
	if _, err := store.ValidateControlLease(ctx, ownerB.MachineID, ownerB.LeaseGeneration, now.Add(5*time.Second)); err != nil {
		t.Fatalf("replacement lease was changed by retired heartbeat: %v", err)
	}
}

// Cleanup callbacks from older callers may not carry a lease generation.  They
// are safe only while the durable owner is also generation-less; generation
// zero must not act as a wildcard after a same-process replacement.
func TestClearOwnerIfSameRejectsGenerationZeroAfterReplacement(t *testing.T) {
	ctx := context.Background()
	for _, sqlite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[sqlite], func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now().UTC().Truncate(time.Microsecond)
			owner := testOwner("cleanup-replacement", "", now)
			owner.ScopeID = "cleanup-replacement-scope"
			owner.MachineID = "cleanup-replacement-machine"
			owner.LeaseGeneration = 23
			if err := store.Update(ctx, func(state *State) error {
				state.Scope = ScopeIdentity{ID: owner.ScopeID, AccountID: "cleanup-account", Profile: "default"}
				state.ServiceOwner = &owner
				state.LockOwner = &owner
				state.ControlLease = ControlLease{ScopeID: owner.ScopeID, Generation: owner.LeaseGeneration, UpdatedAt: now}
				return nil
			}); err != nil {
				t.Fatalf("seed replacement owner: %v", err)
			}
			if sqlite {
				migrateStoreToSQLiteForTest(t, store)
			}
			legacy := owner
			legacy.LeaseGeneration = 0
			cleared, err := store.ClearOwnerIfSame(ctx, legacy)
			if err != nil {
				t.Fatalf("ClearOwnerIfSame generation-zero cleanup: %v", err)
			}
			if cleared {
				t.Fatal("generation-zero cleanup cleared a positive-generation replacement")
			}
			current, found, err := store.ReadOwner(ctx)
			if err != nil || !found {
				t.Fatalf("read replacement owner: found=%v err=%v", found, err)
			}
			if current.MachineID != owner.MachineID || current.LeaseGeneration != owner.LeaseGeneration {
				t.Fatalf("replacement owner changed after generation-zero cleanup: %#v", current)
			}
		})
	}
}

// The inherited real-data store has a large outbox JSON population. Startup
// may need to audit that population to enable the indexed FIFO path, but the
// read-only audit must not hold the state-file lock or the runtime connection.
// An owner heartbeat must remain able to commit while the audit is paused.
func TestSQLiteOutboxProjectionPreparationDoesNotStarveOwnerHeartbeat(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC()
	owner := testOwner("audit-held-owner", "", now)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["audit-held-session"] = SessionContext{
			ID:          "audit-held-session",
			TeamsChatID: "audit-held-chat",
			Status:      SessionStatusActive,
		}
		state.writeOwner(owner)
		return nil
	}); err != nil {
		t.Fatalf("seed owner: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	auditOpened := make(chan struct{})
	releaseAudit := make(chan struct{})
	var once sync.Once
	previousHook := sqliteOutboxAuditTestHook
	sqliteOutboxAuditTestHook = func(stage string) {
		if stage != "opened" {
			return
		}
		once.Do(func() { close(auditOpened) })
		<-releaseAudit
	}
	t.Cleanup(func() {
		sqliteOutboxAuditTestHook = previousHook
		select {
		case <-releaseAudit:
		default:
			close(releaseAudit)
		}
	})
	prepareDone := make(chan error, 1)
	go func() { prepareDone <- store.PrepareOutboxProjection(ctx) }()
	select {
	case <-auditOpened:
	case err := <-prepareDone:
		t.Fatalf("PrepareOutboxProjection finished before audit boundary: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("outbox projection audit did not open")
	}

	heartbeatCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	started := time.Now()
	if _, err := store.RecordOwnerHeartbeat(heartbeatCtx, owner, time.Minute, now.Add(time.Second)); err != nil {
		t.Fatalf("owner heartbeat while audit is held: %v", err)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("owner heartbeat took %s while outbox audit was held", elapsed)
	}
	close(releaseAudit)
	select {
	case err := <-prepareDone:
		if err != nil {
			t.Fatalf("PrepareOutboxProjection: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("PrepareOutboxProjection did not finish after audit release")
	}
}

// The foreground SQLite handle can legitimately be occupied by a large read
// while a poll or history phase is decoding rows. Owner liveness must use its
// separate runtime connection in that situation; sharing database/sql's one
// physical connection turns a harmless read into a lease takeover.
func TestRecordOwnerHeartbeatUsesDedicatedRuntimeConnectionDuringForegroundRead(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC()
	owner := testOwner("dedicated-runtime-owner", "", now)
	if err := store.Update(ctx, func(state *State) error {
		state.writeOwner(owner)
		state.OutboxMessages["outbox:dedicated-runtime-read"] = OutboxMessage{
			ID: "outbox:dedicated-runtime-read", TeamsChatID: "chat:dedicated-runtime",
			Kind: "helper", Body: "hold foreground read", Status: OutboxStatusQueued,
			CreatedAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed owner and outbox: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	var foregroundDB *sql.DB
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return fmt.Errorf("SQLite pointer: ok=%t err=%v", ok, err)
		}
		foregroundDB, err = store.sqliteDBUnlocked(pointer)
		return err
	}); err != nil {
		t.Fatalf("open foreground SQLite handle: %v", err)
	}
	rows, err := foregroundDB.QueryContext(ctx, `SELECT json FROM outbox_messages`)
	if err != nil {
		t.Fatalf("hold foreground outbox read: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("foreground outbox read returned no row: %v", rows.Err())
	}

	heartbeatCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	started := time.Now()
	if _, err := store.RecordOwnerHeartbeat(heartbeatCtx, owner, time.Minute, now.Add(time.Second)); err != nil {
		t.Fatalf("owner heartbeat during foreground read: %v", err)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("owner heartbeat took %s while foreground read held the main connection", elapsed)
	}
	if store.sqliteRuntimeDB == nil || store.sqliteRuntimeDB == foregroundDB {
		t.Fatalf("heartbeat did not use a dedicated runtime database handle: runtime=%p foreground=%p", store.sqliteRuntimeDB, foregroundDB)
	}
	var autoCheckpoint int
	if err := store.sqliteRuntimeDB.QueryRowContext(ctx, `PRAGMA wal_autocheckpoint`).Scan(&autoCheckpoint); err != nil {
		t.Fatalf("read runtime connection auto-checkpoint setting: %v", err)
	}
	if autoCheckpoint != 0 {
		t.Fatalf("runtime connection wal_autocheckpoint = %d, want 0 so heartbeat commits cannot trigger an implicit checkpoint", autoCheckpoint)
	}
}

// A foreground write transaction can legitimately hold SQLite's single-writer
// slot while a poll worker is finishing durable admission.  The independent
// liveness handle must not remain inside BEGIN IMMEDIATE for the full default
// SQLite busy period: doing so also holds sqliteRuntimeMu and makes every poll
// worker wait behind the heartbeat.  The runtime handle uses a short busy
// timeout, so the heartbeat returns a retryable busy result and the bridge's
// existing complete transaction retry can make progress after the writer
// yields.
func TestRecordOwnerHeartbeatDoesNotHangBehindForegroundWriter(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC()
	owner := testOwner("foreground-writer-owner", "", now)
	if err := store.Update(ctx, func(state *State) error {
		state.writeOwner(owner)
		return nil
	}); err != nil {
		t.Fatalf("seed owner: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	var foregroundDB *sql.DB
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return fmt.Errorf("SQLite pointer: ok=%t err=%v", ok, err)
		}
		foregroundDB, err = store.sqliteDBUnlocked(pointer)
		return err
	}); err != nil {
		t.Fatalf("open foreground SQLite handle: %v", err)
	}
	// Prime the dedicated runtime handle before taking the foreground writer
	// reservation. This isolates the assertion to transaction contention rather
	// than first-use validation or schema preparation.
	if _, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, now); err != nil {
		t.Fatalf("prime owner heartbeat: %v", err)
	}

	tx, err := foregroundDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin foreground writer: %v", err)
	}
	released := false
	releaseWriter := func() {
		if released {
			return
		}
		released = true
		_ = tx.Rollback()
	}
	t.Cleanup(releaseWriter)
	if _, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = value WHERE key = 'state_json'`); err != nil {
		t.Fatalf("hold foreground writer reservation: %v", err)
	}

	heartbeatCtx, cancel := context.WithTimeout(ctx, 350*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, heartbeatErr := store.RecordOwnerHeartbeat(heartbeatCtx, owner, time.Minute, now.Add(time.Second))
		done <- heartbeatErr
	}()

	select {
	case heartbeatErr := <-done:
		if heartbeatErr == nil {
			t.Fatal("heartbeat unexpectedly committed while foreground writer was held")
		}
		if !IsSQLiteBusyError(heartbeatErr) && !errors.Is(heartbeatErr, context.DeadlineExceeded) {
			t.Fatalf("heartbeat under foreground writer = %v, want bounded SQLite busy/cancellation error", heartbeatErr)
		}
	case <-time.After(1500 * time.Millisecond):
		releaseWriter()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("heartbeat remained blocked after foreground writer was released")
		}
		t.Fatal("heartbeat remained blocked behind foreground writer")
	}

	releaseWriter()
	if _, err := store.RecordOwnerHeartbeat(context.Background(), owner, time.Minute, now.Add(2*time.Second)); err != nil {
		t.Fatalf("heartbeat after foreground writer release: %v", err)
	}
}

// A guard-triggered revocation must win over startup preparation. Preparation
// is allowed to publish only the conservative untrusted marker and must never
// replace an existing revocation with a stale trusted result.
func TestSQLiteOutboxProjectionPreparationPreservesGuardRevocation(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID: "outbox:audit-revocation-seed", TeamsChatID: "chat:audit-revocation",
		Kind: "helper", Body: "seed", Status: OutboxStatusQueued, Sequence: 1,
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("seed outbox: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	// The malformed sequence projection makes the existing guard trigger publish
	// the durable untrusted marker before startup preparation runs.
	pointer, ok, err := store.currentSQLitePointerReadOnly()
	if err != nil || !ok {
		t.Fatalf("SQLite pointer: ok=%v err=%v", ok, err)
	}
	dbPath, err := store.storeSQLitePath(pointer)
	if err != nil {
		t.Fatalf("SQLite path: %v", err)
	}
	db, err := openSQLiteHandle(dbPath, false)
	if err != nil {
		t.Fatalf("open concurrent SQLite handle: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`PRAGMA busy_timeout = 5000`); err != nil {
		t.Fatalf("set concurrent SQLite timeout: %v", err)
	}
	raw := []byte(`{"id":"outbox:audit-revocation-bad","teams_chat_id":"chat:audit-revocation","status":"queued","sequence":"not-an-integer","created_at":"2026-09-12T12:00:00Z","body":"bad"}`)
	if _, err := db.Exec(`INSERT INTO outbox_messages
(id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence, created_at, deliver_after, post_send_effects_pending, json)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, "outbox:audit-revocation-bad", "", "", "chat:audit-revocation", "", string(OutboxStatusQueued), 2, sqliteTime(now), 0, 0, raw); err != nil {
		t.Fatalf("insert malformed outbox row: %v", err)
	}

	if err := store.PrepareOutboxProjection(ctx); err != nil {
		t.Fatalf("PrepareOutboxProjection: %v", err)
	}

	var marker string
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		return db.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, sqliteOutboxProjectionTrustKey).Scan(&marker)
	}); err != nil {
		t.Fatalf("read projection marker: %v", err)
	}
	if marker != sqliteOutboxProjectionTrustUntrusted {
		t.Fatalf("projection marker = %q, want concurrent guard revocation %q", marker, sqliteOutboxProjectionTrustUntrusted)
	}
}

// The legacy post-send scalar repair must yield between committed pages. A
// large one-shot UPDATE here would hold the shared runtime connection long
// enough to make the owner look dead even though no Graph request is active.
func TestSQLiteOutboxPostSendEffectsBackfillYieldsToOwnerHeartbeat(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 12, 14, 0, 0, 0, time.UTC)
	owner := testOwner("post-send-backfill-owner", "", now)
	if err := store.Update(ctx, func(state *State) error {
		state.writeOwner(owner)
		for i := 0; i < sqliteProjectionBackfillBatchSize+1; i++ {
			id := "outbox:post-send-backfill:"
			if i == sqliteProjectionBackfillBatchSize {
				id += "pending"
			} else {
				id += fmt.Sprintf("%03d", i)
			}
			state.OutboxMessages[id] = OutboxMessage{
				ID: id, TeamsChatID: "chat:post-send-backfill", Kind: "helper",
				Body: "backfill", Status: OutboxStatusSent,
				PostSendEffectsPending: i == sqliteProjectionBackfillBatchSize,
				CreatedAt:              now.Add(time.Duration(i) * time.Second), UpdatedAt: now,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed post-send backfill state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET post_send_effects_pending = NULL`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM state_meta WHERE key IN (?, ?)`,
			sqliteOutboxPostSendEffectsProjectionVersionKey,
			sqliteBackfillCursorKey(sqliteOutboxPostSendEffectsProjectionVersionKey))
		return err
	})
	pointer, ok, err := store.currentSQLitePointerReadOnly()
	if err != nil || !ok {
		t.Fatalf("SQLite pointer: ok=%v err=%v", ok, err)
	}
	dbPath, err := store.storeSQLitePath(pointer)
	if err != nil {
		t.Fatalf("SQLite path: %v", err)
	}

	pageCommitted := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	previousHook := sqliteOutboxPostSendEffectsBackfillPageTestHook
	sqliteOutboxPostSendEffectsBackfillPageTestHook = func() {
		once.Do(func() { close(pageCommitted) })
		<-release
	}
	t.Cleanup(func() {
		sqliteOutboxPostSendEffectsBackfillPageTestHook = previousHook
		select {
		case <-release:
		default:
			close(release)
		}
	})

	prepareDone := make(chan error, 1)
	go func() { prepareDone <- prepareSQLiteOutboxPostSendEffectsBackfill(ctx, dbPath) }()
	select {
	case <-pageCommitted:
	case err := <-prepareDone:
		t.Fatalf("post-send backfill finished before first page: %v", err)
	case <-time.After(storeBackfillPageWaitTimeoutForTest()):
		t.Fatal("post-send backfill did not commit its first page")
	}

	heartbeatCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	started := time.Now()
	if _, err := store.RecordOwnerHeartbeat(heartbeatCtx, owner, time.Minute, now.Add(time.Second)); err != nil {
		t.Fatalf("owner heartbeat between post-send backfill pages: %v", err)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("owner heartbeat took %s between post-send backfill pages", elapsed)
	}
	close(release)
	select {
	case err := <-prepareDone:
		if err != nil {
			t.Fatalf("post-send backfill: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("post-send backfill did not finish after release")
	}

	var pending sql.NullInt64
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		return db.QueryRowContext(ctx, `SELECT post_send_effects_pending FROM outbox_messages WHERE id = ?`, "outbox:post-send-backfill:pending").Scan(&pending)
	}); err != nil {
		t.Fatalf("read repaired post-send scalar: %v", err)
	}
	if !pending.Valid || pending.Int64 != 1 {
		t.Fatalf("repaired pending scalar = %#v, want 1", pending)
	}
}

// A store can have all scalar values populated while the durable repair marker
// is still absent (for example after an older helper wrote the rows and was
// interrupted before publishing metadata). The repair must not decode the
// entire outbox in that case.
func TestSQLiteOutboxPostSendEffectsBackfillSkipsCompleteScalars(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 12, 15, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		for i := 0; i < 2; i++ {
			id := fmt.Sprintf("outbox:post-send-complete:%d", i)
			state.OutboxMessages[id] = OutboxMessage{
				ID: id, TeamsChatID: "chat:post-send-complete", Kind: "helper",
				Body: "complete", Status: OutboxStatusSent,
				PostSendEffectsPending: i == 1,
				CreatedAt:              now.Add(time.Duration(i) * time.Second), UpdatedAt: now,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed complete post-send state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM state_meta WHERE key IN (?, ?)`,
			sqliteOutboxPostSendEffectsProjectionVersionKey,
			sqliteBackfillCursorKey(sqliteOutboxPostSendEffectsProjectionVersionKey))
		return err
	})

	decodedRows := 0
	previousHook := sqliteOutboxPostSendEffectsBackfillRowTestHook
	sqliteOutboxPostSendEffectsBackfillRowTestHook = func() { decodedRows++ }
	t.Cleanup(func() { sqliteOutboxPostSendEffectsBackfillRowTestHook = previousHook })

	pointer, ok, err := store.currentSQLitePointerReadOnly()
	if err != nil || !ok {
		t.Fatalf("SQLite pointer: ok=%v err=%v", ok, err)
	}
	dbPath, err := store.storeSQLitePath(pointer)
	if err != nil {
		t.Fatalf("SQLite path: %v", err)
	}
	if err := prepareSQLiteOutboxPostSendEffectsBackfill(ctx, dbPath); err != nil {
		t.Fatalf("post-send backfill with complete scalar rows: %v", err)
	}
	if decodedRows != 0 {
		t.Fatalf("backfill decoded %d rows despite no missing scalar values", decodedRows)
	}
	var marker string
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		return db.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, sqliteOutboxPostSendEffectsProjectionVersionKey).Scan(&marker)
	}); err != nil {
		t.Fatalf("read post-send projection marker: %v", err)
	}
	if marker != sqliteOutboxPostSendEffectsProjectionVersion {
		t.Fatalf("post-send projection marker = %q, want %q", marker, sqliteOutboxPostSendEffectsProjectionVersion)
	}
}

// Startup must not synchronously decode a legacy outbox just to populate an
// optional scalar. The foreground send and side-effect paths retain their
// canonical JSON fallback while the bounded compatibility repair runs later.
func TestSQLiteOutboxProjectionPreparationDoesNotDecodeLegacyPostSendRows(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 12, 16, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		for i := 0; i < sqliteProjectionBackfillBatchSize+1; i++ {
			id := fmt.Sprintf("outbox:startup-lazy-post-send:%03d", i)
			state.OutboxMessages[id] = OutboxMessage{
				ID: id, TeamsChatID: "chat:startup-lazy-post-send", Kind: "helper",
				Body: "legacy", Status: OutboxStatusSent,
				CreatedAt: now.Add(time.Duration(i) * time.Second), UpdatedAt: now,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed legacy post-send rows: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET post_send_effects_pending = NULL`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM state_meta WHERE key = ?`, sqliteOutboxPostSendEffectsProjectionVersionKey)
		return err
	})

	decodedRows := 0
	previousHook := sqliteOutboxPostSendEffectsBackfillRowTestHook
	sqliteOutboxPostSendEffectsBackfillRowTestHook = func() { decodedRows++ }
	t.Cleanup(func() { sqliteOutboxPostSendEffectsBackfillRowTestHook = previousHook })
	if err := store.PrepareOutboxProjection(ctx); err != nil {
		t.Fatalf("PrepareOutboxProjection with legacy post-send rows: %v", err)
	}
	if decodedRows != 0 {
		t.Fatalf("startup decoded %d legacy post-send rows; want lazy repair", decodedRows)
	}

	var nullRows int
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		return db.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox_messages WHERE post_send_effects_pending IS NULL`).Scan(&nullRows)
	}); err != nil {
		t.Fatalf("count legacy post-send rows after startup: %v", err)
	}
	if nullRows != sqliteProjectionBackfillBatchSize+1 {
		t.Fatalf("startup repaired %d legacy post-send rows; want 0", sqliteProjectionBackfillBatchSize+1-nullRows)
	}
}
