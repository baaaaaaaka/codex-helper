package store

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// A nanosecond-level scalar tear can leave the durable native marker trusted:
// SQLite's trigger-side julianday guard intentionally has a small tolerance.
// The row must still be returned when the caller resumes from a canonical
// cursor that falls between the stale scalar value and the JSON value. Keep
// this test outside the !race budget file so the exact cursor safety contract
// is exercised by the race-enabled store suite too.
func TestSQLiteNativeOutboxCursorDoesNotSkipSubMillisecondProjectionTear(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 14, 12, 30, 0, 123456789, time.UTC)
	target := OutboxMessage{
		ID: "outbox:cursor-projection-tear", SessionID: "session:cursor-projection-tear",
		TeamsChatID: "chat:cursor-projection-tear", Kind: "helper-status",
		Body: "canonical cursor target", Status: OutboxStatusQueued, Sequence: 1,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages[target.ID] = target
		return nil
	}); err != nil {
		t.Fatalf("seed cursor projection tear: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	if err := store.PrepareOutboxProjection(ctx); err != nil {
		t.Fatalf("PrepareOutboxProjection: %v", err)
	}
	if got := sqliteMetaValueForTest(t, store, sqliteOutboxProjectionTrustKey); got != sqliteOutboxProjectionTrustTrusted {
		t.Fatalf("initial outbox projection marker = %q, want trusted", got)
	}

	// Keep the trigger marker trusted while placing the scalar key just before
	// the canonical JSON key. The scalar SQL predicate with this cursor would
	// exclude the row because its ID sorts before the cursor ID.
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET created_at = ? WHERE id = ?`, sqliteTime(now.Add(-time.Nanosecond)), target.ID)
		return err
	})
	if got := sqliteMetaValueForTest(t, store, sqliteOutboxProjectionTrustKey); got != sqliteOutboxProjectionTrustTrusted {
		t.Fatalf("sub-millisecond cursor tear revoked marker = %q, want trusted tolerance", got)
	}
	query := PendingOutboxQuery{
		Now: now.Add(time.Hour), Limit: 1,
		After: PendingOutboxCursor{CreatedAt: now.Add(-time.Nanosecond), ID: "zzz"},
	}
	fallbacks := 0
	previousHook := sqliteOutboxPendingPageCanonicalFallbackTestHook
	sqliteOutboxPendingPageCanonicalFallbackTestHook = func() { fallbacks++ }
	t.Cleanup(func() { sqliteOutboxPendingPageCanonicalFallbackTestHook = previousHook })
	page, err := store.PendingOutboxPageAt(ctx, query)
	if err != nil {
		t.Fatalf("PendingOutboxPageAt after scalar cursor tear: %v", err)
	}
	if len(page.Messages) != 1 || page.Messages[0].ID != target.ID {
		t.Fatalf("page after scalar cursor tear = %#v, want canonical target %q", page.Messages, target.ID)
	}
	if fallbacks == 0 {
		t.Fatal("scalar cursor tear did not use the canonical fallback")
	}
}
