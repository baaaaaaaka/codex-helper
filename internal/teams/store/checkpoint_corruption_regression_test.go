package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestSQLiteMalformedCanonicalCheckpointIsIsolatedFromScopedReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store := newTestStore(t)
	badSession := testSession()
	badSession.ID = "malformed-checkpoint-session"
	badSession.TeamsChatID = "malformed-checkpoint-chat"
	goodSession := testSession()
	goodSession.ID = "healthy-checkpoint-session"
	goodSession.TeamsChatID = "healthy-checkpoint-chat"
	for _, session := range []SessionContext{badSession, goodSession} {
		if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
			t.Fatalf("CreateSession %s created=%v err=%v", session.ID, created, err)
		}
	}
	badID := sessionTranscriptCheckpointID(badSession.ID)
	goodID := sessionTranscriptCheckpointID(goodSession.ID)
	if err := store.Update(ctx, func(state *State) error {
		state.ImportCheckpoints[badID] = ImportCheckpoint{
			ID: badID, SessionID: badSession.ID, LastRecordID: "bad-session-last-record",
			LastOffset: 128, LastOffsetKnown: true, Status: importCheckpointStatusComplete, UpdatedAt: time.Now(),
		}
		state.ImportCheckpoints[goodID] = ImportCheckpoint{
			ID: goodID, SessionID: goodSession.ID, LastRecordID: "healthy-session-last-record",
			LastOffset: 256, LastOffsetKnown: true, Status: importCheckpointStatusComplete, UpdatedAt: time.Now(),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed checkpoints: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	badRaw, err := json.Marshal(ImportCheckpoint{ID: badID, SessionID: badSession.ID, LastRecordID: "bad-session-last-record", LastOffset: 128, LastOffsetKnown: true, Status: importCheckpointStatusComplete})
	if err != nil {
		t.Fatalf("marshal checkpoint: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(badRaw, &fields); err != nil {
		t.Fatalf("unmarshal checkpoint fields: %v", err)
	}
	fields["last_offset_known"] = json.RawMessage("1")
	badRaw, err = json.Marshal(fields)
	if err != nil {
		t.Fatalf("marshal malformed checkpoint: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE import_checkpoints SET json = ? WHERE id = ?`, badRaw, badID)
		return err
	})

	assertMalformedFallback := func(stage string, checkpoint ImportCheckpoint, found bool) {
		t.Helper()
		if !found {
			t.Fatalf("%s found=%v, want malformed checkpoint to remain addressable", stage, found)
		}
		if checkpoint.ID != badID || checkpoint.SessionID != badSession.ID {
			t.Fatalf("%s checkpoint identity = %#v, want id=%q session=%q", stage, checkpoint, badID, badSession.ID)
		}
		if checkpoint.TranscriptQuarantine == nil || checkpoint.TranscriptQuarantine.Kind != malformedCanonicalCheckpointKind {
			t.Fatalf("%s checkpoint quarantine = %#v, want %q", stage, checkpoint.TranscriptQuarantine, malformedCanonicalCheckpointKind)
		}
		if checkpoint.UnresolvedExecution == nil || !checkpoint.LegacySourceUnverified {
			t.Fatalf("%s checkpoint = %#v, want malformed unresolved fallback", stage, checkpoint)
		}
		if checkpoint.LastRecordID != "" || checkpoint.LastSourceLine != 0 || checkpoint.LastOffset != 0 || checkpoint.LastOffsetKnown ||
			checkpoint.SourcePath != "" || checkpoint.SourceGeneration != "" || checkpoint.SourceFingerprint != "" ||
			checkpoint.PendingHistoryRange != nil || checkpoint.ContextGap != nil || checkpoint.TerminalBoundary != nil {
			t.Fatalf("%s opaque fallback retained unproven cursor/proof: %#v", stage, checkpoint)
		}
	}

	checkpoint, found, err := store.ImportCheckpoint(ctx, badID)
	if err != nil {
		t.Fatalf("ImportCheckpoint malformed row: %v", err)
	}
	assertMalformedFallback("ImportCheckpoint", checkpoint, found)
	selected, err := store.ImportCheckpointsForSessions(ctx, []string{badSession.ID, goodSession.ID})
	if err != nil {
		t.Fatalf("ImportCheckpointsForSessions with one malformed row: %v", err)
	}
	assertMalformedFallback("ImportCheckpointsForSessions", selected[badID], true)
	if selected[goodID].LastRecordID != "healthy-session-last-record" {
		t.Fatalf("healthy selected checkpoint = %#v, want intact good row", selected[goodID])
	}
	snapshot, err := store.LinkedTranscriptSessionSnapshot(ctx, []string{badSession.ID, goodSession.ID})
	if err != nil {
		t.Fatalf("LinkedTranscriptSessionSnapshot with one malformed row: %v", err)
	}
	assertMalformedFallback("LinkedTranscriptSessionSnapshot", snapshot.Checkpoints[badID], true)
	if snapshot.Checkpoints[goodID].LastRecordID != "healthy-session-last-record" {
		t.Fatalf("healthy snapshot checkpoint = %#v, want intact good row", snapshot.Checkpoints[goodID])
	}
	state, err := store.TranscriptImportStateSnapshot(ctx)
	if err != nil {
		t.Fatalf("TranscriptImportStateSnapshot with one malformed row: %v", err)
	}
	assertMalformedFallback("TranscriptImportStateSnapshot", state.ImportCheckpoints[badID], true)
	if state.ImportCheckpoints[goodID].LastRecordID != "healthy-session-last-record" {
		t.Fatalf("healthy import state checkpoint = %#v, want intact good row", state.ImportCheckpoints[goodID])
	}
	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load with one malformed row: %v", err)
	}
	assertMalformedFallback("Load", loaded.ImportCheckpoints[badID], true)
	if loaded.ImportCheckpoints[goodID].LastRecordID != "healthy-session-last-record" {
		t.Fatalf("healthy loaded checkpoint = %#v, want intact good row", loaded.ImportCheckpoints[goodID])
	}
	rawBefore := sqliteRawImportCheckpointJSONForTest(t, store, badID)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[goodSession.ID] = goodSession
		return nil
	}); err != nil {
		t.Fatalf("unrelated update with opaque checkpoint: %v", err)
	}
	rawAfter := sqliteRawImportCheckpointJSONForTest(t, store, badID)
	if string(rawAfter) != string(rawBefore) {
		t.Fatalf("opaque checkpoint raw JSON changed across unrelated update: before=%q after=%q", rawBefore, rawAfter)
	}
}

func TestSQLiteOpaqueCheckpointRepairUsesRawCAS(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	session := testSession()
	session.ID = "opaque-repair-session"
	session.TeamsChatID = "opaque-repair-chat"
	if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
		t.Fatalf("CreateSession created=%v err=%v", created, err)
	}
	checkpointID := sessionTranscriptCheckpointID(session.ID)
	if err := store.Update(ctx, func(state *State) error {
		state.ImportCheckpoints[checkpointID] = ImportCheckpoint{
			ID: checkpointID, SessionID: session.ID, Status: importCheckpointStatusComplete,
			LastRecordID: "before-repair", LastOffset: 10, LastOffsetKnown: true,
			UpdatedAt: time.Date(2026, 8, 27, 1, 2, 3, 0, time.UTC),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	badRaw := []byte(`{"last_offset_known":1}`)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE import_checkpoints SET json = ? WHERE id = ?`, badRaw, checkpointID)
		return err
	})
	expectedRaw := sqliteRawImportCheckpointJSONForTest(t, store, checkpointID)
	if string(expectedRaw) != string(badRaw) {
		t.Fatalf("stored opaque raw = %q, want %q", expectedRaw, badRaw)
	}

	_, _, err := store.UpdateImportCheckpoint(ctx, checkpointID, func(current ImportCheckpoint, found bool, updateTime time.Time) (ImportCheckpoint, bool, error) {
		if !found || current.TranscriptQuarantine == nil {
			t.Fatalf("opaque update current=%#v found=%v, want deterministic fallback", current, found)
		}
		current.LastRecordID = "must-not-overwrite-raw"
		return current, true, nil
	})
	if !errors.Is(err, ErrOpaqueCheckpoint) {
		t.Fatalf("ordinary opaque update error = %v, want ErrOpaqueCheckpoint", err)
	}
	if got := sqliteRawImportCheckpointJSONForTest(t, store, checkpointID); string(got) != string(expectedRaw) {
		t.Fatalf("ordinary update changed opaque raw: got=%q want=%q", got, expectedRaw)
	}

	replacement := ImportCheckpoint{
		ID: checkpointID, SessionID: session.ID, Status: importCheckpointStatusComplete,
		SourcePath: "/tmp/opaque-repair.jsonl", LastRecordID: "after-repair",
		LastSourceLine: 4, LastOffset: 40, LastOffsetKnown: true, SourceSize: 40,
		UpdatedAt: time.Date(2026, 8, 27, 2, 3, 4, 0, time.UTC),
	}
	if err := store.RepairOpaqueImportCheckpoint(ctx, checkpointID, expectedRaw, replacement); err != nil {
		t.Fatalf("RepairOpaqueImportCheckpoint: %v", err)
	}
	got, found, err := store.ImportCheckpoint(ctx, checkpointID)
	if err != nil || !found || got.LastRecordID != replacement.LastRecordID || got.RecoveryProofUnusable {
		t.Fatalf("repaired checkpoint=%#v found=%v err=%v, want trusted replacement", got, found, err)
	}
	if err := store.RepairOpaqueImportCheckpoint(ctx, checkpointID, expectedRaw, replacement); err != nil {
		t.Fatalf("idempotent RepairOpaqueImportCheckpoint retry: %v", err)
	}
	staleReplacement := replacement
	staleReplacement.LastRecordID = "stale-replacement"
	if err := store.RepairOpaqueImportCheckpoint(ctx, checkpointID, []byte(`{"other":true}`), staleReplacement); !errors.Is(err, ErrCheckpointRepairConflict) {
		t.Fatalf("stale RepairOpaqueImportCheckpoint error = %v, want CAS conflict", err)
	}
}

func TestSQLiteOperationCheckpointUpdateAcceptsValidNonCanonicalRow(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	session := testSession()
	session.ID = "operation-checkpoint-session"
	session.TeamsChatID = "operation-checkpoint-chat"
	if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
		t.Fatalf("CreateSession created=%v err=%v", created, err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate store: %v", err)
	}
	operationID := sessionTranscriptCheckpointID(session.ID) + ":subagent:child"
	if _, changed, err := store.UpdateImportCheckpoint(ctx, operationID, func(current ImportCheckpoint, found bool, now time.Time) (ImportCheckpoint, bool, error) {
		if found {
			t.Fatalf("new operation checkpoint found=%v, want false", found)
		}
		return ImportCheckpoint{
			ID: operationID, SessionID: session.ID, Status: importCheckpointStatusComplete,
			LastRecordID: "marker-1", UpdatedAt: now,
		}, true, nil
	}); err != nil || !changed {
		t.Fatalf("create operation checkpoint changed=%v err=%v", changed, err)
	}
	if _, changed, err := store.UpdateImportCheckpoint(ctx, operationID, func(current ImportCheckpoint, found bool, now time.Time) (ImportCheckpoint, bool, error) {
		if !found || current.ID != operationID || current.SessionID != session.ID {
			t.Fatalf("existing operation checkpoint=%#v found=%v, want valid non-canonical row", current, found)
		}
		current.LastRecordID = "marker-2"
		current.UpdatedAt = now
		return current, true, nil
	}); err != nil || !changed {
		t.Fatalf("update operation checkpoint changed=%v err=%v", changed, err)
	}
	got, found, err := store.ImportCheckpoint(ctx, operationID)
	if err != nil || !found || got.LastRecordID != "marker-2" {
		t.Fatalf("operation checkpoint=%#v found=%v err=%v, want updated valid row", got, found, err)
	}
}

func TestSQLiteNullableCheckpointMetadataIsolatedAndPreserved(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	session := testSession()
	session.ID = "nullable-checkpoint-session"
	session.TeamsChatID = "nullable-checkpoint-chat"
	if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
		t.Fatalf("CreateSession created=%v err=%v", created, err)
	}
	migrateStoreToSQLiteForTest(t, store)
	canonicalID := sessionTranscriptCheckpointID("empty-identity-session")
	nullRaw := []byte(`{}`)
	canonicalRaw := []byte(`{}`)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO import_checkpoints(id, session_id, status, updated_at, json) VALUES(NULL, NULL, NULL, NULL, ?)`, nullRaw); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO import_checkpoints(id, session_id, status, updated_at, json) VALUES(?, ?, NULL, NULL, ?)`, canonicalID, "empty-identity-session", canonicalRaw)
		return err
	})

	checkpoint, found, err := store.ImportCheckpoint(ctx, canonicalID)
	if err != nil || !found || checkpoint.TranscriptQuarantine == nil || checkpoint.TranscriptQuarantine.Kind != invalidCanonicalCheckpointKind {
		t.Fatalf("empty-identity checkpoint=%#v found=%v err=%v, want deterministic opaque fallback", checkpoint, found, err)
	}
	if checkpoint.LastRecordID != "" || checkpoint.LastOffset != 0 || checkpoint.LastOffsetKnown || checkpoint.SourcePath != "" || checkpoint.SourceGeneration != "" {
		t.Fatalf("empty-identity fallback retained unproven cursor/proof: %#v", checkpoint)
	}
	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load with nullable checkpoint metadata: %v", err)
	}
	if got := loaded.ImportCheckpoints[canonicalID]; got.TranscriptQuarantine == nil || got.TranscriptQuarantine.Kind != invalidCanonicalCheckpointKind {
		t.Fatalf("loaded empty-identity checkpoint=%#v, want opaque fallback", got)
	}

	if err := store.Update(ctx, func(state *State) error {
		state.SchemaVersion++
		return nil
	}); err != nil {
		t.Fatalf("unrelated update with nullable checkpoint metadata: %v", err)
	}
	var gotID, gotSession, gotStatus sql.NullString
	var gotUpdated sql.NullInt64
	var gotRaw []byte
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		return db.QueryRowContext(ctx, `SELECT id, session_id, status, updated_at, json FROM import_checkpoints WHERE id IS NULL`).Scan(&gotID, &gotSession, &gotStatus, &gotUpdated, &gotRaw)
	}); err != nil {
		t.Fatalf("read nullable checkpoint row: %v", err)
	}
	if gotID.Valid || gotSession.Valid || gotStatus.Valid || gotUpdated.Valid || string(gotRaw) != string(nullRaw) {
		t.Fatalf("nullable checkpoint row changed: id=%#v session=%#v status=%#v updated=%#v raw=%q", gotID, gotSession, gotStatus, gotUpdated, gotRaw)
	}
	if got := sqliteRawImportCheckpointJSONForTest(t, store, canonicalID); string(got) != string(canonicalRaw) {
		t.Fatalf("empty-identity raw JSON changed: got=%q want=%q", got, canonicalRaw)
	}
}

func TestSQLiteCanonicalCheckpointSQLIdentityConflictIsolated(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	owner := testSession()
	owner.ID = "canonical-key-owner"
	owner.TeamsChatID = "canonical-key-owner-chat"
	foreign := testSession()
	foreign.ID = "canonical-sql-owner"
	foreign.TeamsChatID = "canonical-sql-owner-chat"
	for _, session := range []SessionContext{owner, foreign} {
		if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
			t.Fatalf("CreateSession %s created=%v err=%v", session.ID, created, err)
		}
	}
	keyID := sessionTranscriptCheckpointID(owner.ID)
	foreignID := sessionTranscriptCheckpointID(foreign.ID)
	if err := store.Update(ctx, func(state *State) error {
		state.ImportCheckpoints[keyID] = ImportCheckpoint{ID: keyID, SessionID: owner.ID, Status: importCheckpointStatusComplete}
		state.ImportCheckpoints[foreignID] = ImportCheckpoint{ID: foreignID, SessionID: foreign.ID, Status: importCheckpointStatusComplete}
		return nil
	}); err != nil {
		t.Fatalf("seed checkpoints: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	foreignPayload, err := json.Marshal(ImportCheckpoint{ID: sessionTranscriptCheckpointID(foreign.ID), SessionID: foreign.ID, Status: importCheckpointStatusComplete, LastRecordID: "foreign-payload"})
	if err != nil {
		t.Fatalf("marshal foreign payload: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE import_checkpoints SET session_id = ?, json = ? WHERE id = ?`, foreign.ID, foreignPayload, keyID)
		return err
	})

	if checkpoint, found, err := store.ImportCheckpoint(ctx, keyID); !errors.Is(err, ErrSessionStateProvenanceMismatch) || found || checkpoint.ID != "" {
		t.Fatalf("canonical SQL identity conflict = %#v found=%v err=%v, want isolated provenance error", checkpoint, found, err)
	}
	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load with canonical SQL identity conflict: %v", err)
	}
	if _, found := loaded.ImportCheckpoints[keyID]; found {
		t.Fatalf("Load exposed canonical SQL identity conflict as typed checkpoint: %#v", loaded.ImportCheckpoints[keyID])
	}
	importState, err := store.TranscriptImportStateSnapshot(ctx)
	if err != nil {
		t.Fatalf("TranscriptImportStateSnapshot with canonical SQL identity conflict: %v", err)
	}
	if _, found := importState.ImportCheckpoints[keyID]; found {
		t.Fatalf("TranscriptImportStateSnapshot exposed canonical SQL identity conflict: %#v", importState.ImportCheckpoints[keyID])
	}
	if got := loaded.ImportCheckpoints[foreignID].SessionID; got != foreign.ID {
		t.Fatalf("healthy foreign checkpoint session=%q, want %q", got, foreign.ID)
	}
}
