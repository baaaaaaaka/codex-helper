package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

func seedSQLiteDeliveredLookupFixture(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages["outbox:lookup-projection"] = OutboxMessage{
			ID:             "outbox:lookup-projection",
			TeamsChatID:    "lookup-chat",
			TeamsMessageID: "lookup-message",
			Kind:           "answer",
			Body:           "already delivered",
			Status:         OutboxStatusSent,
			CreatedAt:      now,
			UpdatedAt:      now,
			SentAt:         now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed delivered lookup fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	return store
}

func TestSQLiteMessageLookupCanonicalDeliveredOutboxSurvivesStaleScalarProjection(t *testing.T) {
	ctx := context.Background()
	store := seedSQLiteDeliveredLookupFixture(t)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages
SET teams_chat_id = ?, teams_message_id = ?, status = ?
WHERE id = ?`, "stale-chat", "stale-message", string(OutboxStatusQueued), "outbox:lookup-projection")
		return err
	})

	single, err := store.MessageLookup(ctx, "lookup-chat", "lookup-message")
	if err != nil {
		t.Fatalf("single MessageLookup with stale scalar projection: %v", err)
	}
	if !single.HasDeliveredOutbox {
		t.Fatalf("single MessageLookup = %#v, want canonical sent outbox", single)
	}
	batch, err := store.MessageLookupBatch(ctx, "lookup-chat", []string{"lookup-message"})
	if err != nil {
		t.Fatalf("batch MessageLookup with stale scalar projection: %v", err)
	}
	if !batch["lookup-message"].HasDeliveredOutbox {
		t.Fatalf("batch MessageLookup = %#v, want canonical sent outbox", batch["lookup-message"])
	}
}

func TestSQLiteMessageLookupRejectsDuplicateDeliveredOutboxInsteadOfReportingMissing(t *testing.T) {
	ctx := context.Background()
	store := seedSQLiteDeliveredLookupFixture(t)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = ? WHERE id = ?`,
			`{"id":"outbox:lookup-projection","teams_chat_id":"lookup-chat","teams_message_id":"lookup-message","teams_message_id":"other-message","status":"sent","created_at":"2026-09-14T12:00:00Z","updated_at":"2026-09-14T12:00:00Z","sent_at":"2026-09-14T12:00:00Z","body":"already delivered"}`,
			"outbox:lookup-projection")
		return err
	})

	if _, err := store.MessageLookup(ctx, "lookup-chat", "lookup-message"); !errors.Is(err, ErrSQLiteOutboxProjectionUntrusted) {
		t.Fatalf("single duplicate-key lookup error = %v, want ErrSQLiteOutboxProjectionUntrusted", err)
	}
	if _, err := store.MessageLookupBatch(ctx, "lookup-chat", []string{"lookup-message"}); !errors.Is(err, ErrSQLiteOutboxProjectionUntrusted) {
		t.Fatalf("batch duplicate-key lookup error = %v, want ErrSQLiteOutboxProjectionUntrusted", err)
	}
}

func TestSQLiteSentOutboxLookupUsesCanonicalIdentityWhenScalarIsStale(t *testing.T) {
	ctx := context.Background()
	store := seedSQLiteDeliveredLookupFixture(t)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages
SET teams_chat_id = ?, status = ?
WHERE id = ?`, "stale-chat", string(OutboxStatusQueued), "outbox:lookup-projection")
		return err
	})

	messages, err := store.SentOutboxMessagesForChat(ctx, "lookup-chat")
	if err != nil {
		t.Fatalf("SentOutboxMessagesForChat with stale scalar projection: %v", err)
	}
	if len(messages) != 1 || messages[0].ID != "outbox:lookup-projection" || messages[0].Status != OutboxStatusSent {
		t.Fatalf("sent outbox lookup = %#v, want canonical sent row", messages)
	}
}

func TestSQLiteSentOutboxLookupRejectsDuplicateCandidateInsteadOfEnqueueingDuplicateNotice(t *testing.T) {
	ctx := context.Background()
	store := seedSQLiteDeliveredLookupFixture(t)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = ? WHERE id = ?`,
			`{"id":"outbox:lookup-projection","teams_chat_id":"lookup-chat","teams_message_id":"lookup-message","teams_message_id":"other-message","status":"sent","created_at":"2026-09-14T12:00:00Z","updated_at":"2026-09-14T12:00:00Z","sent_at":"2026-09-14T12:00:00Z","body":"already delivered"}`,
			"outbox:lookup-projection")
		return err
	})

	if _, err := store.SentOutboxMessagesForChat(ctx, "lookup-chat"); !errors.Is(err, ErrSQLiteOutboxProjectionUntrusted) {
		t.Fatalf("duplicate sent outbox lookup error = %v, want ErrSQLiteOutboxProjectionUntrusted", err)
	}
}

func TestSQLiteMessageLookupRejectsCanonicalDeliveredRowWithNullPhysicalID(t *testing.T) {
	ctx := context.Background()
	store := seedSQLiteDeliveredLookupFixture(t)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET id = NULL WHERE id = ?`, "outbox:lookup-projection")
		return err
	})

	if _, err := store.MessageLookup(ctx, "lookup-chat", "lookup-message"); !errors.Is(err, ErrSQLiteOutboxProjectionUntrusted) {
		t.Fatalf("single lookup with null physical id error = %v, want ErrSQLiteOutboxProjectionUntrusted", err)
	}
	if _, err := store.MessageLookupBatch(ctx, "lookup-chat", []string{"lookup-message"}); !errors.Is(err, ErrSQLiteOutboxProjectionUntrusted) {
		t.Fatalf("batch lookup with null physical id error = %v, want ErrSQLiteOutboxProjectionUntrusted", err)
	}
}
