package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

func TestSQLiteOutboxProjectionTriggersUseBoundedWriteFence(t *testing.T) {
	ctx := context.Background()
	store := seedSQLiteDedupeProjectionFixture(t)

	const query = `SELECT name, COALESCE(sql, '') FROM sqlite_master
WHERE type = 'trigger' AND name IN (?, ?, ?, ?, ?, ?)
ORDER BY name`
	args := []any{
		"outbox_projection_guard_insert", "outbox_projection_guard_update",
		"outbox_session_projection_guard_insert", "outbox_session_projection_guard_update",
		"outbox_turn_projection_guard_insert", "outbox_turn_projection_guard_update",
	}
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			t.Fatal("store is not backed by SQLite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		seen := 0
		for rows.Next() {
			var name, definition string
			if err := rows.Scan(&name, &definition); err != nil {
				return err
			}
			seen++
			if len(definition) > 16000 {
				t.Fatalf("%s trigger is %d bytes; write fence must remain bounded", name, len(definition))
			}
			lower := strings.ToLower(definition)
			if strings.Contains(lower, "json_each(") {
				t.Fatalf("%s trigger still embeds a collection scan: %s", name, definition)
			}
			if !strings.Contains(definition, sqliteOutboxProjectionWriteFenceKey) ||
				!strings.Contains(definition, sqliteOutboxProjectionWriteFenceValue) {
				t.Fatalf("%s trigger does not use the typed writer fence: %s", name, definition)
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if seen != len(args) {
			return fmt.Errorf("outbox projection trigger count = %d, want %d", seen, len(args))
		}
		return nil
	}); err != nil {
		t.Fatalf("inspect bounded outbox triggers: %v", err)
	}

	if err := store.Update(ctx, func(state *State) error {
		message := state.OutboxMessages["outbox-dedupe-target"]
		message.Body = "typed writer update"
		state.OutboxMessages[message.ID] = message
		return nil
	}); err != nil {
		t.Fatalf("typed outbox update: %v", err)
	}
	for _, key := range []string{
		sqliteOutboxProjectionTrustKey,
		sqliteOutboxSessionProjectionTrustKey,
		sqliteOutboxTurnProjectionTrustKey,
	} {
		if got := sqliteMetaValueForTest(t, store, key); got != sqliteOutboxProjectionTrustTrusted {
			t.Fatalf("typed outbox update marker %s = %q, want trusted", key, got)
		}
	}

	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = json_set(json, '$.math_spans', json(?)) WHERE id = ?`,
			`[{"start":1,"start":2}]`, "outbox-dedupe-target")
		return err
	})
	for _, key := range []string{
		sqliteOutboxProjectionTrustKey,
		sqliteOutboxSessionProjectionTrustKey,
		sqliteOutboxTurnProjectionTrustKey,
	} {
		if got := sqliteMetaValueForTest(t, store, key); got != sqliteOutboxProjectionTrustUntrusted {
			t.Fatalf("raw outbox update marker %s = %q, want untrusted", key, got)
		}
	}
}
