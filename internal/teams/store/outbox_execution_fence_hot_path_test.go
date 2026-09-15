package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestOutboxRequiresExecutionFenceIsConservative(t *testing.T) {
	tests := []struct {
		name string
		msg  OutboxMessage
		want bool
	}{
		{name: "unscoped helper", msg: OutboxMessage{Kind: "helper-status"}, want: false},
		{name: "turn-bound helper remains cheap", msg: OutboxMessage{TurnID: "turn-1", Kind: "helper-status"}, want: false},
		{name: "turn-bound final", msg: OutboxMessage{TurnID: "turn-1", Kind: "final"}, want: true},
		{name: "turn completion notification", msg: OutboxMessage{NotificationKind: "turn_completed"}, want: true},
		{name: "automatic import namespace", msg: OutboxMessage{Kind: "import-title"}, want: true},
		{name: "codex status with turn", msg: OutboxMessage{TurnID: "turn-1", Kind: "codex-status-progress"}, want: true},
		{name: "source proof", msg: OutboxMessage{TranscriptSourcePath: "/workspace/transcript.jsonl"}, want: true},
		{name: "parent fence", msg: OutboxMessage{ParentFenceSessionID: "session-parent"}, want: true},
		{name: "durable source rewrite fence", msg: OutboxMessage{BlockedBySourceRewrite: true}, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := outboxRequiresExecutionFence(test.msg); got != test.want {
				t.Fatalf("outboxRequiresExecutionFence(%#v) = %v, want %v", test.msg, got, test.want)
			}
		})
	}
}

// Ordinary helper/status delivery only needs the session admission status. It
// must not decode unrelated cold session fields while settling the outbox row:
// a malformed optional field in that JSON row is local evidence, not a reason
// to stop an otherwise durable Graph receipt. Transcript/final rows retain the
// full execution-fence load and therefore fail closed on the same corruption.
func TestSQLiteOrdinaryOutboxMutationUsesNarrowSessionStatus(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	session := testSession()
	session.ID = "narrow-session"
	session.TeamsChatID = "narrow-chat"
	session.Status = SessionStatusActive
	if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
		t.Fatalf("CreateSession created=%v err=%v", created, err)
	}
	msg, created, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:          "outbox:narrow-helper",
		SessionID:   session.ID,
		TeamsChatID: session.TeamsChatID,
		Kind:        "helper-status",
		Body:        "ordinary helper receipt",
		Status:      OutboxStatusQueued,
		CreatedAt:   now,
		UpdatedAt:   now,
	})
	if err != nil || !created {
		t.Fatalf("QueueOutbox created=%v err=%v", created, err)
	}
	fenced, created, err := store.QueueOutbox(ctx, OutboxMessage{
		ID:               "outbox:narrow-final",
		SessionID:        session.ID,
		TurnID:           "turn:narrow-final",
		TeamsChatID:      session.TeamsChatID,
		Kind:             "final",
		NotificationKind: "turn_completed",
		Body:             "fenced transcript receipt",
		Status:           OutboxStatusQueued,
		CreatedAt:        now.Add(time.Second),
		UpdatedAt:        now.Add(time.Second),
	})
	if err != nil || !created {
		t.Fatalf("QueueOutbox fenced created=%v err=%v", created, err)
	}
	migrateStoreToSQLiteForTest(t, store)

	// Cwd is not part of the narrow runtime gate. The scalar session projection
	// remains valid, while a full SessionContext decode must reject this row.
	corruptSessionJSON := []byte(fmt.Sprintf(`{"id":%q,"status":%q,"teams_chat_id":%q,"cwd":17}`,
		session.ID, string(SessionStatusActive), session.TeamsChatID))
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE sessions SET json = ? WHERE id = ?`, corruptSessionJSON, session.ID)
		return err
	})

	claimed, err := store.MarkOutboxSendAttempt(ctx, msg.ID)
	if err != nil {
		t.Fatalf("ordinary MarkOutboxSendAttempt with malformed cold session: %v", err)
	}
	if claimed.Status != OutboxStatusSending || claimed.SendAttemptToken == "" {
		t.Fatalf("claimed ordinary outbox = %#v", claimed)
	}
	sent, err := store.MarkOutboxSent(ctx, msg.ID, "teams:narrow-helper")
	if err != nil {
		t.Fatalf("ordinary MarkOutboxSent with malformed cold session: %v", err)
	}
	if sent.Status != OutboxStatusSent || sent.TeamsMessageID != "teams:narrow-helper" {
		t.Fatalf("sent ordinary outbox = %#v", sent)
	}

	// A final row in the same session must not use the cheap path. Its execution
	// fence is still authoritative, so the malformed full session is surfaced
	// and the row remains queued rather than being published without proof.
	if _, err := store.MarkOutboxSendAttempt(ctx, fenced.ID); err == nil {
		t.Fatal("fenced MarkOutboxSendAttempt succeeded through malformed session JSON")
	}
	stored, err := store.OutboxMessageByID(ctx, fenced.ID)
	if err != nil {
		t.Fatalf("OutboxMessageByID fenced row: %v", err)
	}
	if stored.Status != OutboxStatusQueued || stored.SendAttemptToken != "" {
		t.Fatalf("fenced row after rejected claim = %#v, want untouched queued row", stored)
	}
}

func TestSQLiteOrdinaryOutboxMutationKeepsExistingLinkedDelivery(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 9, 12, 30, 0, 0, time.UTC)
	session := testSession()
	session.ID = "linked-session"
	session.TeamsChatID = "linked-chat"
	session.Status = SessionStatusActive
	if _, created, err := store.CreateSession(ctx, session); err != nil || !created {
		t.Fatalf("CreateSession created=%v err=%v", created, err)
	}
	msg := OutboxMessage{
		ID: "outbox:linked-helper", SessionID: session.ID, TeamsChatID: session.TeamsChatID,
		Kind: "helper-status", Body: "linked helper", Status: OutboxStatusQueued,
		CreatedAt: now, UpdatedAt: now,
	}
	if _, created, err := store.QueueOutbox(ctx, msg); err != nil || !created {
		t.Fatalf("QueueOutbox created=%v err=%v", created, err)
	}
	// Replace the automatically generated record with a stable custom record so
	// the existence guard is exercised and the linked JSON is proven to survive
	// both claim and sent transitions.
	if err := store.Update(ctx, func(state *State) error {
		for id, delivery := range state.HelperDeliveries {
			if delivery.OutboxID == msg.ID {
				delete(state.HelperDeliveries, id)
			}
		}
		state.HelperDeliveries["helper:linked-helper"] = HelperDeliveryRecord{
			ID: "helper:linked-helper", SessionID: session.ID, OutboxID: msg.ID,
			TeamsChatID: session.TeamsChatID, Kind: "helper-status",
			Status: HelperDeliveryStatusQueued, CreatedAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("replace helper delivery: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	if _, err := store.MarkOutboxSendAttempt(ctx, msg.ID); err != nil {
		t.Fatalf("MarkOutboxSendAttempt with existing linked row: %v", err)
	}
	if _, err := store.MarkOutboxSent(ctx, msg.ID, "teams:linked-helper"); err != nil {
		t.Fatalf("MarkOutboxSent with existing linked row: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load after linked helper delivery: %v", err)
	}
	delivery, ok := state.HelperDeliveries["helper:linked-helper"]
	if !ok {
		t.Fatalf("existing helper delivery was lost: %#v", state.HelperDeliveries)
	}
	if delivery.Status != HelperDeliveryStatusSent || delivery.TeamsMessageID != "teams:linked-helper" || delivery.SentAt.IsZero() {
		t.Fatalf("existing helper delivery after sent = %#v", delivery)
	}
	if _, ok := state.HelperDeliveries["helper:linked-helper"]; !ok {
		t.Fatal("linked helper delivery disappeared after durable completion")
	}
	if _, err := store.MarkOutboxSent(ctx, msg.ID, "teams:linked-helper"); err != nil && !errors.Is(err, ErrOutboxSendNotClaimed) {
		t.Fatalf("idempotent linked completion error = %v", err)
	}
}
