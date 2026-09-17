package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSQLiteHotPollSelectedHydrationUsesTrustedSessionIndexAndResidualOracle(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t)
	const selectedChatID = "selected-index-chat"
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["selected-index-session"] = SessionContext{
			ID: "selected-index-session", Status: SessionStatusActive,
			TeamsChatID: selectedChatID, CodexThreadID: "selected-thread", UpdatedAt: now,
		}
		state.Sessions["selected-index-shared"] = SessionContext{
			ID: "selected-index-shared", Status: SessionStatusActive,
			TeamsChatID: selectedChatID, UpdatedAt: now,
		}
		for i := 0; i < 512; i++ {
			id := fmt.Sprintf("unselected-index-session-%04d", i)
			state.Sessions[id] = SessionContext{
				ID: id, Status: SessionStatusActive,
				TeamsChatID: fmt.Sprintf("unselected-index-chat-%04d", i), UpdatedAt: now,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed selected-index fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return sql.ErrNoRows
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		clause, args := sqliteSelectionInClause("s.teams_chat_id", []string{selectedChatID})
		rows, err := db.QueryContext(ctx, `EXPLAIN QUERY PLAN SELECT s.id, s.teams_chat_id, s.status, s.updated_at, s.json
FROM sessions s WHERE `+sqliteProjectionTrustedSQL("s")+` AND `+clause, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		var details []string
		for rows.Next() {
			var selectID, parentID, detailID int
			var detail string
			if err := rows.Scan(&selectID, &parentID, &detailID, &detail); err != nil {
				return err
			}
			details = append(details, detail)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		plan := strings.Join(details, "\n")
		if !strings.Contains(plan, "sessions_chat_idx") && !strings.Contains(plan, "sessions_trusted_admission_idx") {
			return fmt.Errorf("trusted selected-session query did not use a session chat/trusted index: %s", plan)
		}
		if strings.Contains(strings.ToUpper(plan), "SCAN S") && !strings.Contains(plan, "USING INDEX") {
			return fmt.Errorf("trusted selected-session query unexpectedly scans sessions without an index: %s", plan)
		}

		checkpointSessionClause, checkpointSessionArgs := sqliteSelectionInClause("i.session_id", []string{"selected-index-session"})
		checkpointIDClause, checkpointIDArgs := sqliteSelectionInClause("i.id", []string{transcriptCheckpointIDForSession("selected-index-session")})
		checkpointArgs := append(append([]any{}, checkpointSessionArgs...), checkpointIDArgs...)
		checkpointRows, err := db.QueryContext(ctx, `EXPLAIN QUERY PLAN SELECT i.id, i.session_id, i.status, i.updated_at, i.json
FROM import_checkpoints i WHERE (`+checkpointSessionClause+` OR `+checkpointIDClause+`)
`, checkpointArgs...)
		if err != nil {
			return err
		}
		defer checkpointRows.Close()
		var checkpointDetails []string
		for checkpointRows.Next() {
			var selectID, parentID, detailID int
			var detail string
			if err := checkpointRows.Scan(&selectID, &parentID, &detailID, &detail); err != nil {
				return err
			}
			checkpointDetails = append(checkpointDetails, detail)
		}
		if err := checkpointRows.Err(); err != nil {
			return err
		}
		checkpointPlan := strings.Join(checkpointDetails, "\n")
		if !strings.Contains(checkpointPlan, "import_checkpoints_session_idx") {
			return fmt.Errorf("selected checkpoint query did not use session index: %s", checkpointPlan)
		}
		if !strings.Contains(checkpointPlan, "sqlite_autoindex_import_checkpoints_1") && !strings.Contains(checkpointPlan, "PRIMARY KEY") {
			return fmt.Errorf("selected checkpoint query did not use canonical ID lookup: %s", checkpointPlan)
		}
		if strings.Contains(strings.ToUpper(checkpointPlan), "SCAN I") && !strings.Contains(checkpointPlan, "USING INDEX") {
			return fmt.Errorf("selected checkpoint query unexpectedly scans import_checkpoints: %s", checkpointPlan)
		}
		return nil
	}); err != nil {
		t.Fatalf("trusted selected-session query plan: %v", err)
	}

	// Revoke only the sibling's projection and make its JSON unreadable. The
	// residual lane must still find it through the scalar compatibility chat
	// value and return the same non-runnable witness as the canonical reader.
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE sessions SET json = ?, projection_trusted = 0 WHERE id = ?`, []byte(`[]`), "selected-index-shared")
		return err
	})
	selected, handled, err := store.HotPollSelectedStateForChatsAndSessions(ctx,
		[]string{selectedChatID}, []string{"selected-index-session"})
	if err != nil || !handled {
		t.Fatalf("selected residual refresh handled=%v err=%v", handled, err)
	}
	if got := selected.Sessions["selected-index-session"].CodexThreadID; got != "selected-thread" {
		t.Fatalf("trusted selected session thread=%q, want canonical value", got)
	}
	witness, ok := selected.Sessions["selected-index-shared"]
	if !ok || witness.Status != SessionStatusClosed || witness.TeamsChatID != selectedChatID {
		t.Fatalf("residual malformed shared session=%#v ok=%v, want closed witness for %q", witness, ok, selectedChatID)
	}
}

func TestSQLiteHotPollSelectedStateForChatsAndSessionsIsBoundedAndFresh(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t)
	const (
		controlChatID       = "control-chat"
		selectedChatID      = "selected-chat"
		unselectedChatID    = "unselected-chat"
		selectedSessionID   = "selected-session"
		sharedSessionID     = "shared-session"
		unselectedSessionID = "unselected-session"
	)
	if err := store.Update(ctx, func(state *State) error {
		state.ControlChat = ControlChatBinding{TeamsChatID: controlChatID, UpdatedAt: now}
		state.Sessions[selectedSessionID] = SessionContext{
			ID: selectedSessionID, Status: SessionStatusActive, TeamsChatID: selectedChatID,
			CodexThreadID: "selected-thread-hydrated", UpdatedAt: now,
		}
		state.Sessions[sharedSessionID] = SessionContext{
			ID: sharedSessionID, Status: SessionStatusActive, TeamsChatID: selectedChatID, UpdatedAt: now,
		}
		state.Sessions[unselectedSessionID] = SessionContext{
			ID: unselectedSessionID, Status: SessionStatusActive, TeamsChatID: unselectedChatID, UpdatedAt: now,
		}
		for _, chatID := range []string{controlChatID, selectedChatID, unselectedChatID} {
			state.ChatPolls[chatID] = ChatPollState{
				ChatID: chatID, Seeded: true, PollState: "warm", NextPollAt: now, UpdatedAt: now,
			}
		}
		state.Turns["selected-turn"] = Turn{
			ID: "selected-turn", SessionID: selectedSessionID, Status: TurnStatusQueued, QueuedAt: now, UpdatedAt: now,
		}
		state.Turns["shared-turn"] = Turn{
			ID: "shared-turn", SessionID: sharedSessionID, Status: TurnStatusRunning, StartedAt: now, UpdatedAt: now,
		}
		state.Turns["unselected-turn"] = Turn{
			ID: "unselected-turn", SessionID: unselectedSessionID, Status: TurnStatusRunning, StartedAt: now, UpdatedAt: now,
		}
		state.ImportCheckpoints["selected-checkpoint"] = ImportCheckpoint{
			ID: "selected-checkpoint", SessionID: selectedSessionID, Status: importCheckpointStatusImporting, UpdatedAt: now,
		}
		state.ImportCheckpoints["shared-checkpoint"] = ImportCheckpoint{
			ID: "shared-checkpoint", SessionID: sharedSessionID, Status: importCheckpointStatusImporting, UpdatedAt: now,
		}
		state.ImportCheckpoints["unselected-checkpoint"] = ImportCheckpoint{
			ID: "unselected-checkpoint", SessionID: unselectedSessionID, Status: importCheckpointStatusImporting, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed selected refresh fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	var lockHolds atomic.Int32
	store.SetTimingObserver(func(event StoreTimingEvent) {
		if event.Stage == "state-lock-hold" {
			lockHolds.Add(1)
		}
	})
	selected, handled, err := store.HotPollSelectedStateForChatsAndSessions(ctx,
		[]string{controlChatID, selectedChatID, selectedChatID},
		[]string{selectedSessionID, selectedSessionID},
	)
	store.SetTimingObserver(nil)
	if err != nil || !handled {
		t.Fatalf("selected refresh handled=%v err=%v", handled, err)
	}
	if got := lockHolds.Load(); got != 1 {
		t.Fatalf("selected refresh state-lock holds=%d, want one", got)
	}
	if len(selected.ChatPolls) != 2 {
		t.Fatalf("selected refresh polls=%#v, want only control and selected chats", selected.ChatPolls)
	}
	if _, ok := selected.ChatPolls[unselectedChatID]; ok {
		t.Fatalf("selected refresh returned unselected poll: %#v", selected.ChatPolls[unselectedChatID])
	}
	if got := selected.Sessions[selectedSessionID].CodexThreadID; got != "selected-thread-hydrated" {
		t.Fatalf("selected refresh session thread=%q, want canonical hydrated value", got)
	}
	if _, ok := selected.Turns["selected-turn"]; !ok {
		t.Fatalf("selected refresh omitted selected active turn: %#v", selected.Turns)
	}
	if _, ok := selected.Turns["shared-turn"]; !ok {
		t.Fatalf("selected refresh omitted active turn from shared chat: %#v", selected.Turns)
	}
	if _, ok := selected.Turns["unselected-turn"]; ok {
		t.Fatalf("selected refresh returned unselected active turn: %#v", selected.Turns["unselected-turn"])
	}
	if _, ok := selected.ImportCheckpoints["selected-checkpoint"]; !ok {
		t.Fatalf("selected refresh omitted selected importing checkpoint: %#v", selected.ImportCheckpoints)
	}
	if _, ok := selected.ImportCheckpoints["shared-checkpoint"]; !ok {
		t.Fatalf("selected refresh omitted importing checkpoint from shared chat: %#v", selected.ImportCheckpoints)
	}
	if _, ok := selected.ImportCheckpoints["unselected-checkpoint"]; ok {
		t.Fatalf("selected refresh returned unselected checkpoint: %#v", selected.ImportCheckpoints["unselected-checkpoint"])
	}

	changedAt := now.Add(time.Minute)
	if _, err := store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
		ChatID: selectedChatID, PollState: "cool", NextPollAt: changedAt, LastActivityAt: changedAt,
	}); err != nil {
		t.Fatalf("update selected poll: %v", err)
	}
	if err := store.Update(ctx, func(state *State) error {
		turn := state.Turns["selected-turn"]
		turn.Status = TurnStatusCompleted
		turn.CompletedAt = changedAt
		turn.UpdatedAt = changedAt
		state.Turns[turn.ID] = turn
		turn = state.Turns["shared-turn"]
		turn.Status = TurnStatusCompleted
		turn.CompletedAt = changedAt
		turn.UpdatedAt = changedAt
		state.Turns[turn.ID] = turn
		checkpoint := state.ImportCheckpoints["selected-checkpoint"]
		checkpoint.Status = importCheckpointStatusComplete
		checkpoint.UpdatedAt = changedAt
		state.ImportCheckpoints[checkpoint.ID] = checkpoint
		checkpoint = state.ImportCheckpoints["shared-checkpoint"]
		checkpoint.Status = importCheckpointStatusComplete
		checkpoint.UpdatedAt = changedAt
		state.ImportCheckpoints[checkpoint.ID] = checkpoint
		return nil
	}); err != nil {
		t.Fatalf("complete selected rows: %v", err)
	}

	refreshed, handled, err := store.HotPollSelectedStateForChatsAndSessions(ctx,
		[]string{controlChatID, selectedChatID}, []string{selectedSessionID})
	if err != nil || !handled {
		t.Fatalf("fresh selected refresh handled=%v err=%v", handled, err)
	}
	poll := refreshed.ChatPolls[selectedChatID]
	if poll.PollState != "cool" || !poll.NextPollAt.Equal(changedAt) {
		t.Fatalf("fresh selected poll=%#v, want the post-control schedule", poll)
	}
	if _, ok := refreshed.Turns["selected-turn"]; ok {
		t.Fatalf("completed selected turn remained in active refresh: %#v", refreshed.Turns["selected-turn"])
	}
	if _, ok := refreshed.Turns["shared-turn"]; ok {
		t.Fatalf("completed shared-chat turn remained in active refresh: %#v", refreshed.Turns["shared-turn"])
	}
	if _, ok := refreshed.ImportCheckpoints["selected-checkpoint"]; ok {
		t.Fatalf("completed selected checkpoint remained in importing refresh: %#v", refreshed.ImportCheckpoints["selected-checkpoint"])
	}
	if _, ok := refreshed.ImportCheckpoints["shared-checkpoint"]; ok {
		t.Fatalf("completed shared-chat checkpoint remained in importing refresh: %#v", refreshed.ImportCheckpoints["shared-checkpoint"])
	}
}
