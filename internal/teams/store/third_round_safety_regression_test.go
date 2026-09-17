package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestSQLitePreparationStateLockFencesOrdinaryWriter(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	seedLegacyStateFileForSQLiteMigrationTest(t, store)
	migrateStoreToSQLiteForTest(t, store)

	peer, err := Open(store.Path())
	if err != nil {
		t.Fatalf("Open peer store: %v", err)
	}
	t.Cleanup(func() { _ = peer.Close() })

	opened := make(chan struct{})
	release := make(chan struct{})
	previousHook := sqliteSchemaPreparationTestHook
	sqliteSchemaPreparationTestHook = func(stage string) {
		if stage != "opened" {
			return
		}
		select {
		case <-opened:
		default:
			close(opened)
		}
		<-release
	}
	t.Cleanup(func() {
		sqliteSchemaPreparationTestHook = previousHook
		select {
		case <-release:
		default:
			close(release)
		}
	})

	prepDone := make(chan error, 1)
	go func() { prepDone <- peer.PrepareSQLiteSchemaBeforeOwner(ctx) }()
	select {
	case <-opened:
	case <-time.After(2 * time.Second):
		t.Fatal("schema preparation did not reach maintenance boundary")
	}

	callbackCalled := false
	writeCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	err = store.UpdateIfChanged(writeCtx, func(state *State) (bool, error) {
		callbackCalled = true
		state.ServiceControl.Reason = "must not publish during preparation"
		return true, nil
	})
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ordinary writer during preparation error = %v, want context deadline", err)
	}
	if callbackCalled {
		t.Fatal("ordinary writer ran its mutation callback while preparation held the state lock")
	}

	close(release)
	if err := <-prepDone; err != nil {
		t.Fatalf("schema preparation after releasing writer fence: %v", err)
	}
	if err := store.UpdateIfChanged(ctx, func(state *State) (bool, error) {
		state.ServiceControl.Reason = "after preparation"
		return true, nil
	}); err != nil {
		t.Fatalf("ordinary writer after preparation: %v", err)
	}
}

func TestSQLiteSessionBackfillRejectsSourceChangeBeforeMarker(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["backfill-session"] = SessionContext{
			ID: "backfill-session", TeamsChatID: "backfill-chat", Status: SessionStatusActive,
			CreatedAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM state_meta WHERE key IN (?, ?)`,
			sqliteSessionProjectionVersionKey, sqliteBackfillCursorKey(sqliteSessionProjectionVersionKey)); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE sessions SET projection_trusted = 0 WHERE id = ?`, "backfill-session")
		return err
	})

	pointer, ok, err := store.currentSQLitePointerReadOnly()
	if err != nil || !ok {
		t.Fatalf("read SQLite pointer: ok=%v err=%v", ok, err)
	}
	dbPath, err := store.storeSQLitePath(pointer)
	if err != nil {
		t.Fatalf("resolve SQLite path: %v", err)
	}
	db, err := openExistingSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("open prepared SQLite store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	previousHook := sqliteSessionBackfillTestHook
	sqliteSessionBackfillTestHook = func(db *sql.DB) error {
		_, err := db.ExecContext(ctx, `UPDATE sessions SET json = ? WHERE id = ?`,
			[]byte(`{"id":"backfill-session","teams_chat_id":"changed-chat","status":"active"}`), "backfill-session")
		return err
	}
	t.Cleanup(func() { sqliteSessionBackfillTestHook = previousHook })

	err = backfillSQLiteSessionDerivedColumnsContext(ctx, db)
	if !errors.Is(err, errSQLiteBackfillSourceChanged) {
		t.Fatalf("session backfill after source change error = %v, want source-change fence", err)
	}
	for _, key := range []string{sqliteSessionProjectionVersionKey, sqliteBackfillCursorKey(sqliteSessionProjectionVersionKey)} {
		var value string
		err := db.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, key).Scan(&value)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("session backfill published %s: value=%q err=%v", key, value, err)
		}
	}
}

func TestSQLiteSessionBackfillRollsBackEarlierRowsAfterLaterSourceChange(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := store.Update(ctx, func(state *State) error {
		for _, id := range []string{"session-a", "session-b"} {
			state.Sessions[id] = SessionContext{
				ID: id, TeamsChatID: "chat-" + id, Status: SessionStatusActive,
				CreatedAt: now, UpdatedAt: now,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed sessions: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM state_meta WHERE key IN (?, ?)`,
			sqliteSessionProjectionVersionKey, sqliteBackfillCursorKey(sqliteSessionProjectionVersionKey)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE sessions SET projection_trusted = 0 WHERE id IN (?, ?)`, "session-a", "session-b"); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `CREATE TRIGGER session_backfill_mutates_later_row
AFTER UPDATE OF projection_trusted ON sessions
WHEN NEW.id = 'session-a'
BEGIN
  UPDATE sessions SET json = json || ' ' WHERE id = 'session-b';
END`)
		return err
	})

	pointer, ok, err := store.currentSQLitePointerReadOnly()
	if err != nil || !ok {
		t.Fatalf("read SQLite pointer: ok=%v err=%v", ok, err)
	}
	dbPath, err := store.storeSQLitePath(pointer)
	if err != nil {
		t.Fatalf("resolve SQLite path: %v", err)
	}
	db, err := openExistingSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("open prepared SQLite store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var beforeA, beforeB []byte
	for _, id := range []string{"session-a", "session-b"} {
		var raw []byte
		if err := db.QueryRowContext(ctx, `SELECT json FROM sessions WHERE id = ?`, id).Scan(&raw); err != nil {
			t.Fatalf("read %s before backfill: %v", id, err)
		}
		if id == "session-a" {
			beforeA = append([]byte(nil), raw...)
		} else {
			beforeB = append([]byte(nil), raw...)
		}
	}

	err = backfillSQLiteSessionDerivedColumnsContext(ctx, db)
	if !errors.Is(err, errSQLiteBackfillSourceChanged) {
		t.Fatalf("session backfill after later-row source change error = %v, want source-change fence", err)
	}
	var afterA, afterB []byte
	for _, id := range []string{"session-a", "session-b"} {
		var raw []byte
		if err := db.QueryRowContext(ctx, `SELECT json FROM sessions WHERE id = ?`, id).Scan(&raw); err != nil {
			t.Fatalf("read %s after backfill rollback: %v", id, err)
		}
		if id == "session-a" {
			afterA = raw
		} else {
			afterB = raw
		}
	}
	if string(afterA) != string(beforeA) || string(afterB) != string(beforeB) {
		t.Fatalf("session JSON changed despite rollback: beforeA=%q afterA=%q beforeB=%q afterB=%q", beforeA, afterA, beforeB, afterB)
	}
	for _, id := range []string{"session-a", "session-b"} {
		var trusted int
		if err := db.QueryRowContext(ctx, `SELECT projection_trusted FROM sessions WHERE id = ?`, id).Scan(&trusted); err != nil {
			t.Fatalf("read %s projection after rollback: %v", id, err)
		}
		if trusted != 0 {
			t.Fatalf("session %s projection_trusted=%d after rollback, want 0", id, trusted)
		}
	}
	for _, key := range []string{sqliteSessionProjectionVersionKey, sqliteBackfillCursorKey(sqliteSessionProjectionVersionKey)} {
		var value string
		err := db.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, key).Scan(&value)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("session backfill published %s after rollback: value=%q err=%v", key, value, err)
		}
	}
}

func TestSQLiteChatSequenceBackfillRejectsSourceChangeBeforeMarker(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := store.Update(ctx, func(state *State) error {
		state.ChatSequences["sequence-backfill-chat"] = ChatSequenceState{
			ChatID: "sequence-backfill-chat", Next: 23, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed chat sequence: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM chat_sequences`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM state_meta WHERE key IN (?, ?)`,
			sqliteChatSequenceProjectionVersionKey, sqliteBackfillCursorKey(sqliteChatSequenceProjectionVersionKey))
		return err
	})

	pointer, ok, err := store.currentSQLitePointerReadOnly()
	if err != nil || !ok {
		t.Fatalf("read SQLite pointer: ok=%v err=%v", ok, err)
	}
	dbPath, err := store.storeSQLitePath(pointer)
	if err != nil {
		t.Fatalf("resolve SQLite path: %v", err)
	}
	db, err := openExistingSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("open prepared SQLite store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	previousHook := sqliteChatSequenceBackfillTestHook
	sqliteChatSequenceBackfillTestHook = func(db *sql.DB) error {
		_, err := db.ExecContext(ctx, `UPDATE state_meta SET value = value || ' ' WHERE key = 'state_json'`)
		return err
	}
	t.Cleanup(func() { sqliteChatSequenceBackfillTestHook = previousHook })

	err = backfillSQLiteChatSequencesLegacy(db)
	if !errors.Is(err, errSQLiteBackfillSourceChanged) {
		t.Fatalf("chat sequence backfill after source change error = %v, want source-change fence", err)
	}
	for _, key := range []string{sqliteChatSequenceProjectionVersionKey, sqliteBackfillCursorKey(sqliteChatSequenceProjectionVersionKey)} {
		var value string
		err := db.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, key).Scan(&value)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("chat sequence backfill published %s: value=%q err=%v", key, value, err)
		}
	}
}

func TestSQLiteChatSequenceBackfillRollsBackAfterSourceRevisionChangesInTransaction(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := store.Update(ctx, func(state *State) error {
		for _, id := range []string{"sequence-a", "sequence-b"} {
			state.ChatSequences[id] = ChatSequenceState{ChatID: id, Next: 17, UpdatedAt: now}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed chat sequences: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM chat_sequences`); err != nil {
			return err
		}
		var raw []byte
		if err := tx.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = 'state_json'`).Scan(&raw); err != nil {
			return err
		}
		var document map[string]json.RawMessage
		if err := json.Unmarshal(raw, &document); err != nil {
			return err
		}
		sequences, err := json.Marshal(map[string]ChatSequenceState{
			"sequence-a": {ChatID: "sequence-a", Next: 17, UpdatedAt: now},
			"sequence-b": {ChatID: "sequence-b", Next: 17, UpdatedAt: now},
		})
		if err != nil {
			return err
		}
		document["chat_sequences"] = sequences
		canonical, err := json.Marshal(document)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = 'state_json'`, canonical); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM state_meta WHERE key IN (?, ?)`,
			sqliteChatSequenceProjectionVersionKey, sqliteBackfillCursorKey(sqliteChatSequenceProjectionVersionKey)); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `CREATE TRIGGER chat_sequence_backfill_mutates_source
AFTER INSERT ON chat_sequences
WHEN NEW.chat_id = 'sequence-a'
BEGIN
  UPDATE state_meta SET value = value || ' ' WHERE key = 'state_json';
END`)
		return err
	})

	pointer, ok, err := store.currentSQLitePointerReadOnly()
	if err != nil || !ok {
		t.Fatalf("read SQLite pointer: ok=%v err=%v", ok, err)
	}
	dbPath, err := store.storeSQLitePath(pointer)
	if err != nil {
		t.Fatalf("resolve SQLite path: %v", err)
	}
	db, err := openExistingSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("open prepared SQLite store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var beforeStateJSON []byte
	if err := db.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = 'state_json'`).Scan(&beforeStateJSON); err != nil {
		t.Fatalf("read state_json before sequence backfill: %v", err)
	}

	err = backfillSQLiteChatSequencesContext(ctx, db)
	if !errors.Is(err, errSQLiteBackfillSourceChanged) {
		t.Fatalf("chat sequence backfill after in-transaction source change error = %v, want source-change fence", err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM chat_sequences`).Scan(&count); err != nil {
		t.Fatalf("count chat sequences after rollback: %v", err)
	}
	if count != 0 {
		t.Fatalf("chat sequence rows after rollback = %d, want zero", count)
	}
	var afterStateJSON []byte
	if err := db.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = 'state_json'`).Scan(&afterStateJSON); err != nil {
		t.Fatalf("read state_json after sequence backfill rollback: %v", err)
	}
	if string(afterStateJSON) != string(beforeStateJSON) {
		t.Fatalf("state_json changed despite sequence backfill rollback: before=%q after=%q", beforeStateJSON, afterStateJSON)
	}
	for _, key := range []string{sqliteChatSequenceProjectionVersionKey, sqliteBackfillCursorKey(sqliteChatSequenceProjectionVersionKey)} {
		var value string
		err := db.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, key).Scan(&value)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("chat sequence backfill published %s after rollback: value=%q err=%v", key, value, err)
		}
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	if err := backfillSQLiteChatSequencesContext(cancelCtx, db); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled chat sequence backfill error = %v, want context.Canceled", err)
	}
}

func TestSQLiteCopiedPreparationClaimCanBeReclaimedByPhysicalIdentity(t *testing.T) {
	source := newTestStore(t)
	target := newTestStore(t)
	seedLegacyStateFileForSQLiteMigrationTest(t, source)
	seedLegacyStateFileForSQLiteMigrationTest(t, target)
	sourceResult := migrateStoreToSQLiteForTest(t, source)
	targetResult := migrateStoreToSQLiteForTest(t, target)

	sourceIdentity, err := sqliteReadOnlyFileIdentityForPath(sourceResult.Path)
	if err != nil {
		t.Fatalf("stat source SQLite database: %v", err)
	}
	targetIdentity, err := sqliteReadOnlyFileIdentityForPath(targetResult.Path)
	if err != nil {
		t.Fatalf("stat target SQLite database: %v", err)
	}
	if !sourceIdentity.Exists || !targetIdentity.Exists || sourceIdentity.Revision == targetIdentity.Revision {
		t.Fatalf("test databases do not have distinct physical identities: source=%#v target=%#v", sourceIdentity, targetIdentity)
	}

	claim := sqliteSchemaPreparationClaim{
		ClaimID: "copied-database-claim", DBPath: sourceResult.Path,
		ClaimedAt: time.Now().UTC(), PhysicalRevision: sourceIdentity.Revision,
	}
	writeSQLiteSchemaClaimRawForTest(t, target, []byte(mustMarshalSQLiteSchemaPreparationClaim(claim)))
	withSQLiteUnpreparedTxForTest(t, target, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(context.Background(), `DELETE FROM state_meta WHERE key = ?`, sqliteSchemaPreparationVersionKey)
		return err
	})

	if err := target.PrepareSQLiteSchemaBeforeOwner(context.Background()); err != nil {
		t.Fatalf("reclaim copied-database preparation claim: %v", err)
	}
	var marker string
	if err := withSQLiteRawQueryForTest(target, `SELECT value FROM state_meta WHERE key = ?`, func(raw []byte) error {
		marker = string(raw)
		return nil
	}, sqliteSchemaPreparationVersionKey); err != nil {
		t.Fatalf("read target schema marker after claim recovery: %v", err)
	}
	if marker != sqliteSchemaPreparationVersion {
		t.Fatalf("target schema marker = %q, want %q", marker, sqliteSchemaPreparationVersion)
	}
	if err := withSQLiteRawQueryForTest(target, `SELECT value FROM state_meta WHERE key = ?`, func(raw []byte) error {
		return fmt.Errorf("copied claim remains: %q", raw)
	}, sqliteSchemaPreparationClaimKey); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("copied claim lookup error = %v, want sql.ErrNoRows", err)
	}
}

func TestSQLiteOwnerMigrationAlreadyDBRequiresCurrentLease(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC()
	owner := testOwner("owner-migration-session", "owner-migration-turn", now)
	owner.ScopeID = "owner-migration-scope"
	owner.MachineID = "owner-migration-machine"
	owner.LeaseGeneration = 3
	if err := store.Update(ctx, func(state *State) error {
		state.Scope = ScopeIdentity{ID: owner.ScopeID, AccountID: "owner-migration-account", Profile: "default"}
		state.ServiceOwner = &owner
		state.LockOwner = &owner
		state.ControlLease = ControlLease{
			ScopeID: owner.ScopeID, HolderMachineID: owner.MachineID,
			Generation: owner.LeaseGeneration, Status: ControlLeaseStatusActive,
			LeaseUntil: now.Add(time.Hour), LastHeartbeat: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed owner migration state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	result, err := store.MigrateLargeStateToSQLiteForOwner(ctx, owner, 0)
	if err != nil || !result.AlreadyDB {
		t.Fatalf("owner migration already-DB result = %#v err=%v, want validated AlreadyDB", result, err)
	}
	stale := owner
	stale.LeaseGeneration++
	if _, err := store.MigrateLargeStateToSQLiteForOwner(ctx, stale, 0); !errors.Is(err, ErrControlLeaseNotHeld) {
		t.Fatalf("stale owner migration error = %v, want lease-not-held", err)
	}

	claim := sqliteSchemaPreparationClaim{
		ClaimID: "owner-migration-residual-claim", DBPath: result.Path,
		ClaimedAt: time.Now().UTC(),
	}
	writeSQLiteSchemaClaimRawForTest(t, store, []byte(mustMarshalSQLiteSchemaPreparationClaim(claim)))
	if _, err := store.MigrateLargeStateToSQLiteForOwner(ctx, owner, 0); !errors.Is(err, ErrSQLiteSchemaPreparationInProgress) {
		t.Fatalf("owner migration with residual schema claim error = %v, want preparation-in-progress", err)
	}
	withSQLiteUnpreparedTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM state_meta WHERE key = ?`, sqliteSchemaPreparationClaimKey)
		return err
	})

	// Prime the foreground handle, then revoke only the durable marker through a
	// peer connection. The cached ready bit is not authoritative for owner
	// migration; it must reject the revoked database rather than return
	// AlreadyDB.
	if _, err := store.Load(ctx); err != nil {
		t.Fatalf("prime cached foreground SQLite handle: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM state_meta WHERE key = ?`, sqliteSchemaPreparationVersionKey)
		return err
	})
	if _, err := store.MigrateLargeStateToSQLiteForOwner(ctx, owner, 0); !errors.Is(err, ErrSQLiteSchemaPreparationRequired) {
		t.Fatalf("owner migration with revoked durable marker error = %v, want preparation-required", err)
	}
}
