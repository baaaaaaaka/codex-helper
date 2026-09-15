package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestSQLiteTurnProjectionNativeLookupUsesTurnIndex(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 9, 18, 0, 0, 0, time.UTC)
	target := OutboxMessage{
		ID: "outbox:native-turn-target", SessionID: "session:native-turn",
		TurnID: "turn:native-turn", TeamsChatID: "chat:native-turn",
		Kind: "final", Body: "target", Status: OutboxStatusQueued,
		Sequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	other := OutboxMessage{
		ID: "outbox:native-turn-other", SessionID: "session:native-other",
		TeamsChatID: "chat:native-other", Kind: "helper", Body: "other",
		Status: OutboxStatusSent, Sequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages[target.ID] = target
		state.OutboxMessages[other.ID] = other
		return nil
	}); err != nil {
		t.Fatalf("seed outbox rows: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	if err := store.PrepareOutboxProjection(ctx); err != nil {
		t.Fatalf("PrepareOutboxProjection: %v", err)
	}

	var details []string
	loaded := make(map[string]OutboxMessage)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		marker, err := sqliteReadMetaValueContext(ctx, tx, sqliteOutboxTurnProjectionTrustKey)
		if err != nil {
			return err
		}
		if marker != sqliteOutboxTurnProjectionTrustTrusted {
			return errors.New("turn projection marker is not trusted")
		}
		query := `EXPLAIN QUERY PLAN SELECT ` + sqliteOutboxProjectionSelect("o") + ` FROM outbox_messages o WHERE o.turn_id = ? AND ` + sqliteOutboxProjectionValidSQL("o")
		rows, err := tx.QueryContext(ctx, query, target.TurnID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var selectID, parent, unused int
			var detail string
			if err := rows.Scan(&selectID, &parent, &unused, &detail); err != nil {
				return err
			}
			details = append(details, detail)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return loadSQLiteOutboxForTurnTx(ctx, tx, target.TurnID, loaded)
	})
	if len(details) == 0 {
		t.Fatal("native turn query returned no query-plan details")
	}
	usesIndex := false
	for _, detail := range details {
		if strings.Contains(detail, "outbox_turn_idx") {
			usesIndex = true
			break
		}
	}
	if !usesIndex {
		t.Fatalf("native turn query plan = %v, want outbox_turn_idx", details)
	}
	if got, ok := loaded[target.ID]; !ok || got.TurnID != target.TurnID {
		t.Fatalf("native turn lookup = %#v, want target %#v", loaded, target)
	}
	if _, ok := loaded[other.ID]; ok {
		t.Fatalf("native turn lookup admitted unrelated row %#v", loaded[other.ID])
	}
}

func TestSQLiteTurnProjectionGuardRevokesOnNativePredicateChanges(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		mutate func(context.Context, *sql.Tx, OutboxMessage) error
	}{
		{
			name: "id",
			mutate: func(ctx context.Context, tx *sql.Tx, target OutboxMessage) error {
				_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET id = ? WHERE id = ?`, target.ID+":changed", target.ID)
				return err
			},
		},
		{
			name: "session",
			mutate: func(ctx context.Context, tx *sql.Tx, target OutboxMessage) error {
				_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET session_id = ? WHERE id = ?`, "different-session", target.ID)
				return err
			},
		},
		{
			name: "chat",
			mutate: func(ctx context.Context, tx *sql.Tx, target OutboxMessage) error {
				_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET teams_chat_id = ? WHERE id = ?`, "different-chat", target.ID)
				return err
			},
		},
		{
			name: "status",
			mutate: func(ctx context.Context, tx *sql.Tx, target OutboxMessage) error {
				_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET status = ? WHERE id = ?`, string(OutboxStatusSent), target.ID)
				return err
			},
		},
		{
			name: "sequence",
			mutate: func(ctx context.Context, tx *sql.Tx, target OutboxMessage) error {
				_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET sequence = ? WHERE id = ?`, target.Sequence+100, target.ID)
				return err
			},
		},
		{
			name: "created-at",
			mutate: func(ctx context.Context, tx *sql.Tx, target OutboxMessage) error {
				_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET created_at = ? WHERE id = ?`, sqliteTime(target.CreatedAt.Add(time.Second)), target.ID)
				return err
			},
		},
		{
			name: "turn-json",
			mutate: func(ctx context.Context, tx *sql.Tx, target OutboxMessage) error {
				_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = json_set(json, '$.turn_id', 'different-turn') WHERE id = ?`, target.ID)
				return err
			},
		},
		{
			name: "decode-shape",
			mutate: func(ctx context.Context, tx *sql.Tx, target OutboxMessage) error {
				_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = json_set(json, '$.body', 17) WHERE id = ?`, target.ID)
				return err
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now().UTC().Truncate(time.Microsecond)
			target := OutboxMessage{
				ID: "outbox:turn-guard-" + tc.name, SessionID: "session:turn-guard",
				TurnID: "turn:turn-guard", TeamsChatID: "chat:turn-guard",
				Kind: "final", Body: "target", Status: OutboxStatusQueued,
				Sequence: 1, CreatedAt: now, UpdatedAt: now,
			}
			if err := store.Update(ctx, func(state *State) error {
				state.OutboxMessages[target.ID] = target
				return nil
			}); err != nil {
				t.Fatalf("seed outbox row: %v", err)
			}
			migrateStoreToSQLiteForTest(t, store)
			if err := store.PrepareOutboxProjection(ctx); err != nil {
				t.Fatalf("prepare projection: %v", err)
			}
			if got := sqliteMetaValueForTest(t, store, sqliteOutboxTurnProjectionTrustKey); got != sqliteOutboxTurnProjectionTrustTrusted {
				t.Fatalf("initial turn marker = %q, want trusted", got)
			}
			withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
				return tc.mutate(ctx, tx, target)
			})
			if got := sqliteMetaValueForTest(t, store, sqliteOutboxTurnProjectionTrustKey); got != sqliteOutboxTurnProjectionTrustUntrusted {
				t.Fatalf("turn marker after %s mutation = %q, want untrusted", tc.name, got)
			}
		})
	}
}

func TestSQLiteTurnProjectionFallsBackWithoutOmittingCanonicalTurn(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 9, 18, 5, 0, 0, time.UTC)
	target := OutboxMessage{
		ID: "outbox:turn-fallback-target", SessionID: "session:turn-fallback",
		TurnID: "turn:turn-fallback", TeamsChatID: "chat:turn-fallback",
		Kind: "final", Body: "canonical target", Status: OutboxStatusQueued,
		Sequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages[target.ID] = target
		return nil
	}); err != nil {
		t.Fatalf("seed fallback target: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	if err := store.PrepareOutboxProjection(ctx); err != nil {
		t.Fatalf("PrepareOutboxProjection: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET turn_id = '' WHERE id = ?`, target.ID)
		return err
	})

	var loaded map[string]OutboxMessage
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		marker, err := sqliteReadMetaValueContext(ctx, tx, sqliteOutboxTurnProjectionTrustKey)
		if err != nil {
			return err
		}
		if marker != sqliteOutboxTurnProjectionTrustUntrusted {
			return errors.New("turn projection marker was not revoked")
		}
		loaded = make(map[string]OutboxMessage)
		return loadSQLiteOutboxForTurnTx(ctx, tx, target.TurnID, loaded)
	})
	if got, ok := loaded[target.ID]; !ok || got.TurnID != target.TurnID {
		t.Fatalf("canonical fallback lookup = %#v, want target %#v", loaded, target)
	}
}

func TestSQLiteTurnProjectionAuditRejectsMissingScalarAndNormalizesSafeWrites(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 9, 18, 10, 0, 0, time.UTC)
	target := OutboxMessage{
		ID: "outbox:turn-audit-missing", SessionID: "session:turn-audit",
		TurnID: "turn:turn-audit", TeamsChatID: "chat:turn-audit",
		Kind: "final", Body: "canonical target", Status: OutboxStatusQueued,
		Sequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages[target.ID] = target
		return nil
	}); err != nil {
		t.Fatalf("seed audit target: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	// Make the row unsafe before the audit. The canonical JSON still carries
	// the turn identity, but an indexed query would now omit it.
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET turn_id = '' WHERE id = ?`, target.ID)
		return err
	})
	if err := store.PrepareOutboxProjection(ctx); err != nil {
		t.Fatalf("PrepareOutboxProjection with missing scalar: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		marker, err := sqliteReadMetaValueContext(ctx, tx, sqliteOutboxTurnProjectionTrustKey)
		if err != nil {
			return err
		}
		if marker != sqliteOutboxTurnProjectionTrustUntrusted {
			return errors.New("missing scalar was incorrectly trusted")
		}
		return nil
	})

	// A normal writer that keeps the canonical turn and scalar projection in
	// sync must not force every completion back to the O(N) fallback. Use a
	// separate clean store because an explicitly untrusted marker is sticky
	// until a deliberate repair/audit boundary; safe writes must not silently
	// upgrade a marker that was previously revoked.
	safeStore := newTestStore(t)
	safeTarget := target
	safeTarget.ID = "outbox:turn-audit-safe"
	safeTarget.TurnID = "turn:turn-audit-safe"
	if err := safeStore.Update(ctx, func(state *State) error {
		state.OutboxMessages[safeTarget.ID] = safeTarget
		return nil
	}); err != nil {
		t.Fatalf("seed safe audit target: %v", err)
	}
	migrateStoreToSQLiteForTest(t, safeStore)
	if err := safeStore.PrepareOutboxProjection(ctx); err != nil {
		t.Fatalf("PrepareOutboxProjection for safe store: %v", err)
	}
	safeExtra := safeTarget
	safeExtra.ID = "outbox:turn-audit-safe-extra"
	safeExtra.Sequence = 2
	data, err := json.Marshal(safeExtra)
	if err != nil {
		t.Fatalf("marshal safe target: %v", err)
	}
	withSQLiteTxForTest(t, safeStore, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO outbox_messages(id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence, created_at, deliver_after, post_send_effects_pending, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			safeExtra.ID, safeExtra.SessionID, safeExtra.TurnID, safeExtra.TeamsChatID, "", string(safeExtra.Status), safeExtra.Sequence, sqliteTime(safeExtra.CreatedAt), 0, 0, data)
		return err
	})
	withSQLiteTxForTest(t, safeStore, func(tx *sql.Tx) error {
		marker, err := sqliteReadMetaValueContext(ctx, tx, sqliteOutboxTurnProjectionTrustKey)
		if err != nil {
			return err
		}
		if marker != sqliteOutboxTurnProjectionTrustTrusted {
			return errors.New("safe turn projection was not trusted after audit")
		}
		return nil
	})
}
