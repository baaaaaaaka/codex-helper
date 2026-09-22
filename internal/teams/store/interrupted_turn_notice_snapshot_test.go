package store

import (
	"context"
	"testing"
	"time"
)

func TestSQLiteInterruptedTurnNoticeStateSnapshotIsSessionScoped(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	reason := "notice-test-reason"

	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["s1"] = SessionContext{ID: "s1", TeamsChatID: "chat-1", Status: SessionStatusActive, UpdatedAt: now}
		state.Sessions["s2"] = SessionContext{ID: "s2", TeamsChatID: "chat-2", Status: SessionStatusActive, UpdatedAt: now}
		state.Sessions["s3"] = SessionContext{ID: "s3", TeamsChatID: "chat-3", Status: SessionStatusActive, UpdatedAt: now}
		state.Turns["notice-candidate"] = Turn{
			ID: "notice-candidate", SessionID: "s1", Status: TurnStatusInterrupted,
			RecoveryReason: reason, CreatedAt: now, UpdatedAt: now,
		}
		state.Turns["same-session-queued"] = Turn{
			ID: "same-session-queued", SessionID: "s1", Status: TurnStatusQueued,
			CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
		}
		state.Turns["same-session-completed"] = Turn{
			ID: "same-session-completed", SessionID: "s1", Status: TurnStatusCompleted,
			CreatedAt: now.Add(2 * time.Second), UpdatedAt: now.Add(2 * time.Second),
		}
		state.Turns["other-session-candidate"] = Turn{
			ID: "other-session-candidate", SessionID: "s2", Status: TurnStatusInterrupted,
			RecoveryReason: "other-reason", CreatedAt: now.Add(3 * time.Second), UpdatedAt: now.Add(3 * time.Second),
		}
		state.Turns["other-session-running"] = Turn{
			ID: "other-session-running", SessionID: "s2", Status: TurnStatusRunning,
			CreatedAt: now.Add(4 * time.Second), UpdatedAt: now.Add(4 * time.Second),
		}
		state.Turns["unrelated-terminal"] = Turn{
			ID: "unrelated-terminal", SessionID: "s3", Status: TurnStatusCompleted,
			CreatedAt: now.Add(5 * time.Second), UpdatedAt: now.Add(5 * time.Second),
		}
		state.ImportCheckpoints["checkpoint-unrelated"] = ImportCheckpoint{
			ID: "checkpoint-unrelated", SessionID: "s3", Status: importCheckpointStatusComplete, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed notice snapshot state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	full, err := store.QueuedTurnStateSnapshot(ctx)
	if err != nil {
		t.Fatalf("QueuedTurnStateSnapshot: %v", err)
	}

	fullLoads := 0
	previousHook := sqliteStateLoadTestHook
	sqliteStateLoadTestHook = func() { fullLoads++ }
	t.Cleanup(func() { sqliteStateLoadTestHook = previousHook })

	got, err := store.InterruptedTurnNoticeStateSnapshot(ctx, reason)
	if err != nil {
		t.Fatalf("InterruptedTurnNoticeStateSnapshot: %v", err)
	}
	if fullLoads != 0 {
		t.Fatalf("bounded SQLite notice snapshot invoked full state loader %d time(s)", fullLoads)
	}
	if len(got.Sessions) != 1 || got.Sessions["s1"].TeamsChatID != "chat-1" {
		t.Fatalf("session scope = %#v, want only s1", got.Sessions)
	}
	if len(got.ImportCheckpoints) != 0 {
		t.Fatalf("notice snapshot loaded unrelated checkpoints: %#v", got.ImportCheckpoints)
	}
	if len(got.Turns) != 2 {
		t.Fatalf("turn scope = %#v, want candidate plus same-session queued turn", got.Turns)
	}
	if got.Turns["notice-candidate"].RecoveryReason != reason || got.Turns["same-session-queued"].Status != TurnStatusQueued {
		t.Fatalf("bounded turns = %#v", got.Turns)
	}
	if _, ok := got.Turns["other-session-candidate"]; ok {
		t.Fatalf("loaded candidate from unrelated session: %#v", got.Turns)
	}

	// The bounded result must preserve exactly the fields the notice pass uses
	// from the historical snapshot, while terminal/unrelated rows stay out.
	if got.Turns["notice-candidate"] != full.Turns["notice-candidate"] ||
		got.Turns["same-session-queued"] != full.Turns["same-session-queued"] {
		t.Fatalf("bounded turns changed notice inputs: got=%#v full=%#v", got.Turns, full.Turns)
	}
}

func TestSQLiteInterruptedTurnNoticeStateSnapshotHasNoCandidateWithoutFullLoad(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["s1"] = SessionContext{ID: "s1", TeamsChatID: "chat-1", Status: SessionStatusActive}
		state.Turns["terminal"] = Turn{ID: "terminal", SessionID: "s1", Status: TurnStatusCompleted}
		return nil
	}); err != nil {
		t.Fatalf("seed terminal state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	fullLoads := 0
	previousHook := sqliteStateLoadTestHook
	sqliteStateLoadTestHook = func() { fullLoads++ }
	t.Cleanup(func() { sqliteStateLoadTestHook = previousHook })

	got, err := store.InterruptedTurnNoticeStateSnapshot(ctx, "missing-reason")
	if err != nil {
		t.Fatalf("InterruptedTurnNoticeStateSnapshot: %v", err)
	}
	if fullLoads != 0 {
		t.Fatalf("no-candidate notice snapshot invoked full state loader %d time(s)", fullLoads)
	}
	if len(got.Turns) != 0 || len(got.Sessions) != 0 {
		t.Fatalf("no-candidate snapshot = %#v, want empty selected maps", got)
	}
}
