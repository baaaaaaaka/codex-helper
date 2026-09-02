package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	unknownSessionStatusForTest = "future_session_state"
	unknownTurnStatusForTest    = "future_turn_state"
	unknownOutboxStatusForTest  = "future_outbox_state"
)

func seedUnknownStatusFixture(t *testing.T, store *Store) (SessionContext, SessionContext, string, string, string) {
	t.Helper()
	ctx := context.Background()
	good := testSession()
	good.ID = "status-healthy-session"
	good.TeamsChatID = "status-healthy-chat"
	unknown := testSession()
	unknown.ID = "status-unknown-session"
	unknown.TeamsChatID = "status-unknown-chat"
	for _, session := range []SessionContext{good, unknown} {
		if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
			t.Fatalf("CreateSession %s created=%v err=%v", session.ID, created, err)
		}
	}
	turnID := "status-unknown-turn"
	healthyOutboxID := "status-healthy-outbox"
	unknownOutboxID := "status-unknown-outbox"
	now := time.Now().UTC().Add(-time.Minute)
	if err := store.Update(ctx, func(state *State) error {
		state.Turns[turnID] = Turn{
			ID: turnID, SessionID: unknown.ID, Status: TurnStatusQueued,
			QueuedAt: now, CreatedAt: now, UpdatedAt: now,
		}
		state.OutboxMessages[healthyOutboxID] = OutboxMessage{
			ID: healthyOutboxID, SessionID: good.ID, TeamsChatID: good.TeamsChatID,
			Kind: "status", Body: "healthy", Status: OutboxStatusQueued,
			CreatedAt: now, UpdatedAt: now,
		}
		state.OutboxMessages[unknownOutboxID] = OutboxMessage{
			ID: unknownOutboxID, SessionID: unknown.ID, TeamsChatID: unknown.TeamsChatID,
			Kind: "status", Body: "unknown", Status: OutboxStatusQueued,
			CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed unknown status fixture: %v", err)
	}
	return good, unknown, turnID, healthyOutboxID, unknownOutboxID
}

func assertUnknownStatusDisposition(t *testing.T, loaded State, unknown SessionContext, turnID string, unknownOutboxID string, healthyOutboxID string) {
	t.Helper()
	session, ok := loaded.Sessions[unknown.ID]
	if !ok || session.Status != SessionStatus(unknownSessionStatusForTest) {
		t.Fatalf("unknown session = %#v found=%v, want original unknown status retained", session, ok)
	}
	if !strings.Contains(session.QuarantineReason, unknownSessionStatusForTest) || session.QuarantineSource != "store_decoder" {
		t.Fatalf("unknown session diagnostic = %#v, want explicit status disposition", session)
	}
	turn, ok := loaded.Turns[turnID]
	if !ok || turn.Status != TurnStatusRunning {
		t.Fatalf("unknown turn = %#v found=%v, want running safety fence", turn, ok)
	}
	if !strings.Contains(turn.RecoveryReason, unknownTurnStatusForTest) {
		t.Fatalf("unknown turn recovery reason = %q, want original status", turn.RecoveryReason)
	}
	message, ok := loaded.OutboxMessages[unknownOutboxID]
	if !ok || message.Status != OutboxStatus(unknownOutboxStatusForTest) {
		t.Fatalf("unknown outbox = %#v found=%v, want held original status", message, ok)
	}
	healthy, ok := loaded.OutboxMessages[healthyOutboxID]
	if !ok || healthy.Status != OutboxStatusQueued {
		t.Fatalf("healthy outbox = %#v found=%v, want queued", healthy, ok)
	}
}

func TestUnknownPersistedStatusesAreExplicitlyHeldInJSON(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	good, unknown, turnID, healthyOutboxID, unknownOutboxID := seedUnknownStatusFixture(t, store)
	data, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read JSON state: %v", err)
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatalf("decode JSON state: %v", err)
	}
	var sessions map[string]json.RawMessage
	if err := json.Unmarshal(root["sessions"], &sessions); err != nil {
		t.Fatalf("decode sessions: %v", err)
	}
	sessions[unknown.ID] = json.RawMessage(fmt.Sprintf(`{"id":%q,"teams_chat_id":%q,"status":%q}`, unknown.ID, unknown.TeamsChatID, unknownSessionStatusForTest))
	root["sessions"], err = json.Marshal(sessions)
	if err != nil {
		t.Fatalf("encode sessions: %v", err)
	}
	var turns map[string]json.RawMessage
	if err := json.Unmarshal(root["turns"], &turns); err != nil {
		t.Fatalf("decode turns: %v", err)
	}
	turns[turnID] = json.RawMessage(fmt.Sprintf(`{"id":%q,"session_id":%q,"status":%q}`, turnID, unknown.ID, unknownTurnStatusForTest))
	root["turns"], err = json.Marshal(turns)
	if err != nil {
		t.Fatalf("encode turns: %v", err)
	}
	var outbox map[string]json.RawMessage
	if err := json.Unmarshal(root["outbox_messages"], &outbox); err != nil {
		t.Fatalf("decode outbox: %v", err)
	}
	outbox[unknownOutboxID] = json.RawMessage(fmt.Sprintf(`{"id":%q,"session_id":%q,"teams_chat_id":%q,"kind":"status","body":"unknown","status":%q}`, unknownOutboxID, unknown.ID, unknown.TeamsChatID, unknownOutboxStatusForTest))
	root["outbox_messages"], err = json.Marshal(outbox)
	if err != nil {
		t.Fatalf("encode outbox: %v", err)
	}
	corrupted, err := json.Marshal(root)
	if err != nil {
		t.Fatalf("encode unknown-status state: %v", err)
	}
	path := store.Path()
	if err := store.Close(); err != nil {
		t.Fatalf("close before JSON reopen: %v", err)
	}
	writeRawStoreStateForTest(t, store, corrupted)
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen JSON store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	loaded, err := reopened.Load(ctx)
	if err != nil {
		t.Fatalf("Load unknown-status JSON store: %v", err)
	}
	assertUnknownStatusDisposition(t, loaded, unknown, turnID, unknownOutboxID, healthyOutboxID)
	pending, err := reopened.PendingOutboxAt(ctx, time.Now().UTC().Add(time.Minute))
	if err != nil {
		t.Fatalf("PendingOutboxAt unknown-status JSON store: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != healthyOutboxID {
		t.Fatalf("pending unknown-status JSON rows = %#v, want only healthy row", pending)
	}
	execution, err := reopened.LinkedTranscriptExecutionSnapshot(ctx, []string{unknown.ID})
	if err != nil {
		t.Fatalf("LinkedTranscriptExecutionSnapshot unknown-status JSON store: %v", err)
	}
	if !execution.Running[unknown.ID] {
		t.Fatalf("unknown-status JSON turn was not an execution safety fence: %#v", execution)
	}
	if _, ok := loaded.Sessions[good.ID]; !ok {
		t.Fatal("healthy session disappeared while holding unknown session status")
	}
}

func TestUnknownPersistedStatusesAreExplicitlyHeldInSQLite(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	good, unknown, turnID, healthyOutboxID, unknownOutboxID := seedUnknownStatusFixture(t, store)
	migrateStoreToSQLiteForTest(t, store)
	unknownSessionRaw := []byte(fmt.Sprintf(`{"id":%q,"teams_chat_id":%q,"status":%q}`, unknown.ID, unknown.TeamsChatID, unknownSessionStatusForTest))
	unknownTurnRaw := []byte(fmt.Sprintf(`{"id":%q,"session_id":%q,"status":%q}`, turnID, unknown.ID, unknownTurnStatusForTest))
	unknownOutboxRaw := []byte(fmt.Sprintf(`{"id":%q,"session_id":%q,"teams_chat_id":%q,"kind":"status","body":"unknown","status":%q}`, unknownOutboxID, unknown.ID, unknown.TeamsChatID, unknownOutboxStatusForTest))
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE sessions SET status = ?, json = ? WHERE id = ?`, unknownSessionStatusForTest, unknownSessionRaw, unknown.ID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE turns SET status = ?, json = ? WHERE id = ?`, unknownTurnStatusForTest, unknownTurnRaw, turnID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET status = ?, json = ? WHERE id = ?`, unknownOutboxStatusForTest, unknownOutboxRaw, unknownOutboxID)
		return err
	})
	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load unknown-status SQLite store: %v", err)
	}
	assertUnknownStatusDisposition(t, loaded, unknown, turnID, unknownOutboxID, healthyOutboxID)
	pending, err := store.PendingOutboxAt(ctx, time.Now().UTC().Add(time.Minute))
	if err != nil {
		t.Fatalf("PendingOutboxAt unknown-status SQLite store: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != healthyOutboxID {
		t.Fatalf("pending unknown-status SQLite rows = %#v, want only healthy row", pending)
	}
	execution, err := store.LinkedTranscriptExecutionSnapshot(ctx, []string{unknown.ID})
	if err != nil {
		t.Fatalf("LinkedTranscriptExecutionSnapshot unknown-status SQLite store: %v", err)
	}
	if !execution.Running[unknown.ID] {
		t.Fatalf("unknown-status SQLite turn was not an execution safety fence: %#v", execution)
	}
	if _, ok := loaded.Sessions[good.ID]; !ok {
		t.Fatal("healthy SQLite session disappeared while holding unknown session status")
	}
}
