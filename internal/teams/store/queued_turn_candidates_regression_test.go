package store

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

func TestSQLiteQueuedTurnSessionIDsUsesStableTypedPages(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions = map[string]SessionContext{}
		state.Turns = map[string]Turn{}
		for _, id := range []string{"queued-session-00", "queued-session-01", "queued-session-02", "queued-session-03"} {
			state.Sessions[id] = SessionContext{ID: id, Status: SessionStatusActive, TeamsChatID: "chat-" + id, UpdatedAt: now}
		}
		state.Turns["queued-turn-00"] = Turn{ID: "queued-turn-00", SessionID: "queued-session-00", Status: TurnStatusQueued, QueuedAt: now, CreatedAt: now, UpdatedAt: now}
		state.Turns["queued-turn-01-running"] = Turn{ID: "queued-turn-01-running", SessionID: "queued-session-01", Status: TurnStatusRunning, CreatedAt: now, UpdatedAt: now}
		state.Turns["queued-turn-01"] = Turn{ID: "queued-turn-01", SessionID: "queued-session-01", Status: TurnStatusQueued, QueuedAt: now, CreatedAt: now, UpdatedAt: now}
		state.Turns["queued-turn-02"] = Turn{ID: "queued-turn-02", SessionID: "queued-session-02", Status: TurnStatusQueued, QueuedAt: now, CreatedAt: now, UpdatedAt: now}
		state.Turns["queued-turn-03"] = Turn{ID: "queued-turn-03", SessionID: "queued-session-03", Status: TurnStatusQueued, QueuedAt: now, CreatedAt: now, UpdatedAt: now}
		return nil
	}); err != nil {
		t.Fatalf("seed queued candidate fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	first, more, handled, err := store.queuedTurnSessionIDsSQLite(ctx, "", 2)
	if err != nil || !handled {
		t.Fatalf("first native candidate page=%#v more=%v handled=%v err=%v", first, more, handled, err)
	}
	if !more || len(first) != 2 || first[0] != "queued-session-00" || first[1] != "queued-session-02" {
		t.Fatalf("first native candidate page=%#v more=%v, want session-00/session-02 and more", first, more)
	}
	second, more, handled, err := store.queuedTurnSessionIDsSQLite(ctx, first[len(first)-1], 2)
	if err != nil || !handled {
		t.Fatalf("second native candidate page=%#v more=%v handled=%v err=%v", second, more, handled, err)
	}
	if more || len(second) != 1 || second[0] != "queued-session-03" {
		t.Fatalf("second native candidate page=%#v more=%v, want final session-03", second, more)
	}
}

func TestSQLiteQueuedTurnSessionIDsFallsBackOnUntrustedTurnProjection(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 15, 12, 30, 0, 0, time.UTC)
	const sessionID = "queued-fallback-session"
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive, TeamsChatID: "queued-fallback-chat", UpdatedAt: now}
		state.Turns["queued-fallback-turn"] = Turn{ID: "queued-fallback-turn", SessionID: sessionID, Status: TurnStatusQueued, QueuedAt: now, CreatedAt: now, UpdatedAt: now}
		return nil
	}); err != nil {
		t.Fatalf("seed fallback fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	// A stale marker is a capability miss, not an empty candidate result. The
	// public API must use the canonical JSON oracle and still find the work.
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = ?`, "0", sqliteTurnProjectionVersionKey)
		return err
	})
	if ids, more, handled, err := store.queuedTurnSessionIDsSQLite(ctx, "", 64); err != nil || handled || more || len(ids) != 0 {
		t.Fatalf("stale-marker native result=%#v more=%v handled=%v err=%v, want unhandled", ids, more, handled, err)
	}
	ids, more, err := store.QueuedTurnSessionIDs(ctx, "", 64)
	if err != nil || more || len(ids) != 1 || ids[0] != sessionID {
		t.Fatalf("stale-marker public result=%#v more=%v err=%v", ids, more, err)
	}

	// A current marker does not override a row-local generation fence. This
	// models a mixed-version writer touching one turn between backfill pages.
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = ?`, sqliteTurnProjectionVersion, sqliteTurnProjectionVersionKey)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE turns SET projection_trusted = 0 WHERE id = ?`, "queued-fallback-turn")
		return err
	})
	if ids, _, handled, err := store.queuedTurnSessionIDsSQLite(ctx, "", 64); err != nil || handled || len(ids) != 0 {
		t.Fatalf("untrusted-row native result=%#v handled=%v err=%v, want unhandled", ids, handled, err)
	}
	ids, more, err = store.QueuedTurnSessionIDs(ctx, "", 64)
	if err != nil || more || len(ids) != 1 || ids[0] != sessionID {
		t.Fatalf("untrusted-row public result=%#v more=%v err=%v", ids, more, err)
	}
}

func TestSQLiteQueuedTurnSessionIDsNativePagesCoverLargeCandidateSet(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 15, 13, 0, 0, 0, time.UTC)
	const total = 65
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions = make(map[string]SessionContext, total)
		state.Turns = make(map[string]Turn, total)
		for i := 0; i < total; i++ {
			sessionID := fmt.Sprintf("queued-page-%03d", i)
			state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive, TeamsChatID: "chat-" + sessionID, UpdatedAt: now}
			turnID := fmt.Sprintf("queued-page-turn-%03d", i)
			state.Turns[turnID] = Turn{ID: turnID, SessionID: sessionID, Status: TurnStatusQueued, QueuedAt: now.Add(time.Duration(i) * time.Second), CreatedAt: now, UpdatedAt: now}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed large candidate fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	var got []string
	after := ""
	for {
		page, more, handled, err := store.queuedTurnSessionIDsSQLite(ctx, after, 64)
		if err != nil || !handled {
			t.Fatalf("candidate page after=%q page=%#v more=%v handled=%v err=%v", after, page, more, handled, err)
		}
		got = append(got, page...)
		if !more {
			break
		}
		after = page[len(page)-1]
	}
	if len(got) != total {
		t.Fatalf("candidate pages returned %d rows, want %d ordered rows", len(got), total)
	}
	if got[0] != "queued-page-000" || got[len(got)-1] != "queued-page-064" {
		t.Fatalf("candidate page boundaries first=%q last=%q, want queued-page-000/queued-page-064", got[0], got[len(got)-1])
	}
}
