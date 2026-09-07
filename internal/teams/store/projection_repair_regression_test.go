package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// A mixed-version writer can leave the canonical JSON row newer than the
// indexed SQLite columns. Startup must repair only those narrow projections;
// otherwise the scalar due/identity predicates used by the hot schedule can
// hide a healthy chat, its active turn, and its importing checkpoint forever.
func TestSQLiteProjectionBackfillRepairsStaleSessionAndChatPollScalarsAcrossRestart(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	oldActivity := now.Add(-2 * time.Hour)
	session := SessionContext{
		ID: "projection-repair-session", Status: SessionStatusActive,
		TeamsChatID: "projection-repair-chat", UpdatedAt: oldActivity,
	}
	poll := ChatPollState{
		ChatID: session.TeamsChatID, Seeded: true, PollState: chatPollStateCold,
		NextPollAt: now.Add(-time.Minute), LastActivityAt: oldActivity, UpdatedAt: oldActivity,
	}
	turn := Turn{
		ID: "projection-repair-running-turn", SessionID: session.ID,
		Status: TurnStatusRunning, CreatedAt: oldActivity, UpdatedAt: oldActivity,
	}
	checkpoint := ImportCheckpoint{
		ID: "projection-repair-checkpoint", SessionID: session.ID,
		Status: importCheckpointStatusImporting, UpdatedAt: oldActivity,
	}
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[session.ID] = session
		state.ChatPolls[poll.ChatID] = poll
		state.Turns[turn.ID] = turn
		state.ImportCheckpoints[checkpoint.ID] = checkpoint
		return nil
	}); err != nil {
		t.Fatalf("seed projection-repair state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE sessions
SET teams_chat_id = ?, status = ?, updated_at = ? WHERE id = ?`,
			"projection-repair-wrong-chat", string(SessionStatusClosed), sqliteTime(now.Add(time.Hour)), session.ID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE chat_polls
SET next_poll_at = ?, blocked_until = ?, poll_state = ?, last_activity_at = ?,
    park_notice_sent_at = ?, parked_skip_eligible = ?, frontier_active = ?, updated_at = ?
WHERE chat_id = ?`,
			sqliteTime(now.Add(time.Hour)), sqliteTime(now.Add(time.Hour)), chatPollStateParked,
			sqliteTime(now.Add(time.Hour)), sqliteTime(now.Add(time.Hour)), 1, 1,
			sqliteTime(now.Add(time.Hour)), poll.ChatID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key IN (?, ?)`,
			"0", sqliteSessionProjectionVersionKey, sqliteChatPollProjectionVersionKey); err != nil {
			return err
		}
		return nil
	})

	path := store.Path()
	if err := store.Close(); err != nil {
		t.Fatalf("close before projection-repair restart: %v", err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen for projection repair: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	candidates, handled, err := reopened.HotPollWorkCandidates(ctx, "projection-repair-control")
	if err != nil || !handled {
		t.Fatalf("HotPollWorkCandidates after projection repair = handled:%v err:%v", handled, err)
	}
	found := false
	for _, candidate := range candidates {
		if candidate.ID == session.ID && candidate.TeamsChatID == session.TeamsChatID && candidate.Status == SessionStatusActive {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("repaired session was hidden from hot candidates: %#v", candidates)
	}

	schedule, err := reopened.HotPollReadyScheduleState(ctx, "projection-repair-control", now)
	if err != nil {
		t.Fatalf("HotPollReadyScheduleState after projection repair: %v", err)
	}
	if got, ok := schedule.ChatPolls[poll.ChatID]; !ok || got.PollState != chatPollStateCold || !got.NextPollAt.Equal(poll.NextPollAt) {
		t.Fatalf("repaired poll schedule = %#v present=%v, want %#v", got, ok, poll)
	}
	if got, ok := schedule.Turns[turn.ID]; !ok || got.Status != TurnStatusRunning || got.SessionID != session.ID {
		t.Fatalf("repaired active turn = %#v present=%v, want %#v", got, ok, turn)
	}
	if got, ok := schedule.ImportCheckpoints[checkpoint.ID]; !ok || got.Status != importCheckpointStatusImporting || got.SessionID != session.ID {
		t.Fatalf("repaired importing checkpoint = %#v present=%v, want %#v", got, ok, checkpoint)
	}

	var scalarChat, scalarStatus string
	var scalarNext, scalarBlocked, scalarLast, scalarParked, scalarFrontier, scalarUpdated int64
	if err := reopened.withStateLock(ctx, func() error {
		pointer, ok, err := reopened.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			if err == nil {
				err = sql.ErrNoRows
			}
			return err
		}
		db, err := reopened.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		return db.QueryRowContext(ctx, `SELECT s.teams_chat_id, s.status,
       p.next_poll_at, p.blocked_until, p.last_activity_at,
       p.parked_skip_eligible, p.frontier_active, p.updated_at
FROM sessions s JOIN chat_polls p ON p.chat_id = ? WHERE s.id = ?`, poll.ChatID, session.ID).
			Scan(&scalarChat, &scalarStatus, &scalarNext, &scalarBlocked, &scalarLast, &scalarParked, &scalarFrontier, &scalarUpdated)
	}); err != nil {
		t.Fatalf("read repaired scalar projections: %v", err)
	}
	if scalarChat != session.TeamsChatID || scalarStatus != string(SessionStatusActive) ||
		scalarNext != sqliteTime(poll.NextPollAt) || scalarBlocked != 0 ||
		scalarLast != sqliteTime(poll.LastActivityAt) || scalarParked != 0 ||
		scalarFrontier != 0 || scalarUpdated != sqliteTime(poll.UpdatedAt) {
		t.Fatalf("repaired scalar projections = chat:%q status:%q next:%d blocked:%d last:%d parked:%d frontier:%d updated:%d, want canonical session/poll",
			scalarChat, scalarStatus, scalarNext, scalarBlocked, scalarLast, scalarParked, scalarFrontier, scalarUpdated)
	}
}

// Old/manual SQLite writers can leave a numeric compatibility column as TEXT
// instead of INTEGER. Opening the store and rewriting an unrelated state row
// must quarantine that projection locally; it must not abort before the
// canonical JSON session/poll can be recovered.
func TestSQLiteMalformedNumericCompatibilityColumnsDoNotAbortRecovery(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	session := SessionContext{
		ID: "malformed-number-session", Status: SessionStatusActive,
		TeamsChatID: "malformed-number-chat", UpdatedAt: now,
	}
	poll := ChatPollState{
		ChatID: session.TeamsChatID, Seeded: true, PollState: chatPollStateCold,
		NextPollAt: now.Add(-time.Minute), LastActivityAt: now.Add(-time.Hour), UpdatedAt: now,
	}
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[session.ID] = session
		state.ChatPolls[poll.ChatID] = poll
		return nil
	}); err != nil {
		t.Fatalf("seed malformed-number state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		for _, column := range []string{"updated_at"} {
			if _, err := tx.ExecContext(ctx, `UPDATE sessions SET `+column+` = ? WHERE id = ?`, "not-a-number", session.ID); err != nil {
				return err
			}
		}
		for _, column := range []string{"next_poll_at", "blocked_until", "last_activity_at", "park_notice_sent_at", "parked_skip_eligible", "frontier_active", "updated_at"} {
			if _, err := tx.ExecContext(ctx, `UPDATE chat_polls SET `+column+` = ? WHERE chat_id = ?`, "not-a-number", poll.ChatID); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key IN (?, ?)`,
			"0", sqliteSessionProjectionVersionKey, sqliteChatPollProjectionVersionKey); err != nil {
			return err
		}
		return nil
	})

	path := store.Path()
	if err := store.Close(); err != nil {
		t.Fatalf("close before malformed-number recovery: %v", err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen with malformed numeric projections: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	loaded, err := reopened.Load(ctx)
	if err != nil {
		t.Fatalf("Load with malformed numeric projections: %v", err)
	}
	if got := loaded.Sessions[session.ID]; got.TeamsChatID != session.TeamsChatID || got.Status != SessionStatusActive {
		t.Fatalf("loaded session after numeric recovery = %#v, want %#v", got, session)
	}
	if got := loaded.ChatPolls[poll.ChatID]; got.ChatID != poll.ChatID || got.PollState != poll.PollState {
		t.Fatalf("loaded poll after numeric recovery = %#v, want %#v", got, poll)
	}
	if _, handled, err := reopened.HotPollWorkCandidates(ctx, "malformed-number-control"); err != nil || !handled {
		t.Fatalf("HotPollWorkCandidates after numeric recovery = handled:%v err:%v", handled, err)
	}

	// Exercise the full-state opaque-row capture path as well. The malformed
	// scalar is reintroduced after the one-time backfill marker is current; a
	// later unrelated update must still complete and keep canonical state.
	withSQLiteTxForTest(t, reopened, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE sessions SET updated_at = ? WHERE id = ?`, "still-not-a-number", session.ID)
		return err
	})
	if err := reopened.Update(ctx, func(state *State) error {
		state.ControlChat.TeamsChatTopic = "numeric projection recovery"
		return nil
	}); err != nil {
		t.Fatalf("unrelated update with malformed numeric projection: %v", err)
	}
}

// Projection markers are migration metadata, not fields owned by the cold
// state document. A later unrelated Store.Update must preserve them; otherwise
// every save reopens an O(number-of-chats) repair pass and can race an older
// writer's partial projection refresh.
func TestSQLiteProjectionMarkersSurviveFullStateRewrite(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.ControlChat.TeamsChatID = "chat:marker-preservation"
		return nil
	}); err != nil {
		t.Fatalf("seed marker-preservation state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	assertMarkers := func(label string) {
		t.Helper()
		if err := store.withStateLock(ctx, func() error {
			pointer, ok, err := store.currentSQLitePointerUnlocked()
			if err != nil || !ok {
				if err == nil {
					err = sql.ErrNoRows
				}
				return err
			}
			db, err := store.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			for key, want := range map[string]string{
				sqliteChatPollFrontierHintVersionKey: sqliteChatPollFrontierHintVersion,
				sqliteSessionProjectionVersionKey:    sqliteSessionProjectionVersion,
				sqliteChatPollProjectionVersionKey:   sqliteChatPollProjectionVersion,
			} {
				var got string
				if err := db.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, key).Scan(&got); err != nil {
					return err
				}
				if got != want {
					return fmt.Errorf("%s marker %q = %q, want %q", label, key, got, want)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	assertMarkers("after migration")
	if err := store.Update(ctx, func(state *State) error {
		state.ControlChat.TeamsChatTopic = "unrelated update"
		return nil
	}); err != nil {
		t.Fatalf("unrelated full-state rewrite: %v", err)
	}
	assertMarkers("after full-state rewrite")
}

func TestSQLiteUnknownLegacyTurnStatusRemainsLinkedTranscriptSafetyFence(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	session := SessionContext{
		ID: "unknown-turn-status-session", Status: SessionStatusActive,
		TeamsChatID: "unknown-turn-status-chat", UpdatedAt: now,
	}
	turn := Turn{
		ID: "unknown-turn-status-turn", SessionID: session.ID,
		Status: TurnStatusCompleted, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[session.ID] = session
		state.Turns[turn.ID] = turn
		return nil
	}); err != nil {
		t.Fatalf("seed unknown-turn-status state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE turns SET status = ? WHERE id = ?`, "future-execution-state", turn.ID)
		return err
	})

	var snapshot LinkedTranscriptExecutionSnapshot
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return sql.ErrNoRows
		}
		snapshot, err = store.loadSQLiteLinkedTranscriptExecutionSnapshotUnlocked(ctx, pointer, map[string]struct{}{session.ID: {}})
		return err
	}); err != nil {
		t.Fatalf("load linked execution snapshot with unknown scalar status: %v", err)
	}
	if !snapshot.Running[session.ID] {
		t.Fatalf("unknown scalar turn status was not retained as a safety fence: %#v", snapshot)
	}
}

func TestSQLiteLegacyInterruptedProbeUsesCanonicalJSONWhenScalarIsStale(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	sessionID := "canonical-interrupted-stale-scalar-session"
	turnID := "canonical-interrupted-stale-scalar-turn"
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive}
		state.Turns[turnID] = Turn{
			ID: turnID, SessionID: sessionID, Status: TurnStatusInterrupted,
			RecoveryReason: "ambiguous Codex execution: stale-scalar regression",
			InterruptedAt:  now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed canonical interrupted state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		// Simulate a partial JSON-first write: canonical JSON records the
		// unresolved execution while the compatibility status still says terminal.
		_, err := tx.ExecContext(ctx, `UPDATE turns SET status = ? WHERE id = ?`, string(TurnStatusCompleted), turnID)
		return err
	})

	fenced := false
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
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		state := newState()
		if err := markSQLiteLegacyUnresolvedSessionTx(ctx, tx, &state, sessionID); err != nil {
			return err
		}
		fenced = state.legacyUnresolvedSessions[sessionID]
		return tx.Commit()
	}); err != nil {
		t.Fatalf("run canonical interrupted legacy probe: %v", err)
	}
	if !fenced {
		t.Fatal("canonical interrupted JSON with stale completed scalar was not retained as a safety fence")
	}
}

// Startup repair cannot help a writer that updates canonical JSON while the
// current process is already serving hot queries. The admission expressions
// therefore re-read canonical chat/schedule/frontier fields when the indexed
// projections disagree. This test intentionally mutates only one side while
// keeping the store open and proves both the ready schedule and work-candidate
// paths still find the same chat.
func TestSQLiteHotAdmissionUsesCanonicalProjectionsWithoutRestart(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	session := SessionContext{
		ID: "same-open-projection-session", Status: SessionStatusActive,
		TeamsChatID: "same-open-projection-chat", UpdatedAt: now,
	}
	poll := ChatPollState{
		ChatID: session.TeamsChatID, Seeded: true, PollState: chatPollStateCold,
		NextPollAt: now.Add(time.Hour), LastActivityAt: now, UpdatedAt: now,
	}
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[session.ID] = session
		state.ChatPolls[poll.ChatID] = poll
		return nil
	}); err != nil {
		t.Fatalf("seed same-open projection state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	// The JSON writer makes the chat due and opens a continuation, but leaves
	// the old future schedule and frontier hint in SQLite. A separate partial
	// writer also leaves a stale non-empty session chat binding.
	poll.NextPollAt = now.Add(-time.Minute)
	poll.BlockedUntil = time.Time{}
	poll.ContinuationPath = "/v1/chats/same-open-projection-chat/messages?$skiptoken=continuation"
	pollJSON, err := json.Marshal(poll)
	if err != nil {
		t.Fatalf("marshal same-open poll: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE chat_polls SET json = ? WHERE chat_id = ?`, pollJSON, poll.ChatID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE sessions SET teams_chat_id = ? WHERE id = ?`, "same-open-wrong-chat", session.ID)
		return err
	})

	schedule, err := store.HotPollReadyScheduleState(ctx, "same-open-control", now)
	if err != nil {
		t.Fatalf("hot schedule with same-open stale projections: %v", err)
	}
	gotPoll, ok := schedule.ChatPolls[poll.ChatID]
	if !ok || gotPoll.ChatID != poll.ChatID || gotPoll.ContinuationPath != poll.ContinuationPath {
		t.Fatalf("hot schedule poll = %#v present=%v, want canonical poll %#v", gotPoll, ok, poll)
	}

	candidates, handled, err := store.HotPollWorkCandidatesExcludingIdleAt(ctx, "same-open-control", time.Time{}, now)
	if err != nil || !handled {
		t.Fatalf("hot work candidates with same-open stale projections = handled:%v err:%v", handled, err)
	}
	found := false
	for _, candidate := range candidates {
		if candidate.ID == session.ID && candidate.TeamsChatID == session.TeamsChatID && candidate.Status == SessionStatusActive {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("same-open canonical session was hidden from work candidates: %#v", candidates)
	}
}

// A stale turn.status projection must not make a running canonical turn
// disappear from the hot schedule or allow a second queued turn for the same
// session to start. The claim check is deliberately exercised in addition to
// the read-only schedule so this regression cannot pass while only the
// diagnostic snapshot is correct.
func TestSQLiteCanonicalActiveTurnFencesClaimWhenScalarStatusIsStale(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	old := now.Add(-2 * time.Hour)
	session := SessionContext{
		ID: "canonical-turn-status-session", Status: SessionStatusActive,
		TeamsChatID: "canonical-turn-status-chat", CreatedAt: old, UpdatedAt: old,
	}
	poll := ChatPollState{
		ChatID: session.TeamsChatID, Seeded: true, PollState: chatPollStateCold,
		NextPollAt: now.Add(-time.Minute), LastActivityAt: old, UpdatedAt: old,
	}
	running := Turn{
		ID: "canonical-running-turn", SessionID: session.ID, Status: TurnStatusRunning,
		CreatedAt: old, StartedAt: old, UpdatedAt: old,
	}
	queued := Turn{
		ID: "canonical-queued-turn", SessionID: session.ID, Status: TurnStatusQueued,
		CreatedAt: old.Add(time.Minute), QueuedAt: old.Add(time.Minute), UpdatedAt: old.Add(time.Minute),
	}
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[session.ID] = session
		state.ChatPolls[poll.ChatID] = poll
		state.Turns[running.ID] = running
		state.Turns[queued.ID] = queued
		return nil
	}); err != nil {
		t.Fatalf("seed canonical turn status state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE turns SET status = ? WHERE id = ?`, string(TurnStatusCompleted), running.ID)
		return err
	})

	schedule, err := store.HotPollReadyScheduleState(ctx, "", now)
	if err != nil {
		t.Fatalf("HotPollReadyScheduleState with stale turn status: %v", err)
	}
	got, ok := schedule.Turns[running.ID]
	if !ok || got.Status != TurnStatusRunning {
		t.Fatalf("canonical running turn in hot schedule = %#v present=%v, want running turn", got, ok)
	}

	claimed, ok, err := store.ClaimNextQueuedTurn(ctx, session.ID)
	if err != nil {
		t.Fatalf("ClaimNextQueuedTurn with canonical active turn: %v", err)
	}
	if ok {
		t.Fatalf("ClaimNextQueuedTurn claimed %q while canonical turn %q is running", claimed.ID, running.ID)
	}
}

// The hot checkpoint lane must use canonical session identity/status. A
// partial writer can leave both indexed values stale at once; waiting for a
// restart-only projection repair would otherwise hide an importing checkpoint
// from the listener's bounded schedule forever.
func TestSQLiteHotCheckpointAdmissionUsesCanonicalIdentityAndStatus(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	session := SessionContext{
		ID: "canonical-checkpoint-session", Status: SessionStatusActive,
		TeamsChatID: "canonical-checkpoint-chat", UpdatedAt: now.Add(-time.Hour),
	}
	poll := ChatPollState{
		ChatID: session.TeamsChatID, Seeded: true, PollState: chatPollStateCold,
		NextPollAt: now.Add(-time.Minute), UpdatedAt: now.Add(-time.Hour),
	}
	checkpoint := ImportCheckpoint{
		ID: sessionTranscriptCheckpointID(session.ID), SessionID: session.ID,
		Status: importCheckpointStatusImporting, UpdatedAt: now.Add(-time.Hour),
	}
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[session.ID] = session
		state.ChatPolls[poll.ChatID] = poll
		state.ImportCheckpoints[checkpoint.ID] = checkpoint
		return nil
	}); err != nil {
		t.Fatalf("seed canonical checkpoint state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE sessions SET teams_chat_id = ?, status = ? WHERE id = ?`,
			"canonical-checkpoint-wrong-chat", string(SessionStatusClosed), session.ID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE import_checkpoints SET session_id = ?, status = ? WHERE id = ?`,
			"canonical-checkpoint-wrong-session", importCheckpointStatusComplete, checkpoint.ID)
		return err
	})

	schedule, err := store.HotPollReadyScheduleState(ctx, "", now)
	if err != nil {
		t.Fatalf("HotPollReadyScheduleState with stale checkpoint projections: %v", err)
	}
	got, ok := schedule.ImportCheckpoints[checkpoint.ID]
	if !ok || got.SessionID != session.ID || got.Status != importCheckpointStatusImporting {
		t.Fatalf("canonical importing checkpoint = %#v present=%v, want %#v", got, ok, checkpoint)
	}

	full, err := store.HotPollScheduleState(ctx)
	if err != nil {
		t.Fatalf("HotPollScheduleState with stale checkpoint projections: %v", err)
	}
	if got, ok := full.ImportCheckpoints[checkpoint.ID]; !ok || got.SessionID != session.ID || got.Status != importCheckpointStatusImporting {
		t.Fatalf("full hot canonical importing checkpoint = %#v present=%v, want %#v", got, ok, checkpoint)
	}
}

// An unscoped claim has no durable owner capability. It must not claim a
// generation-bound row (or skip that FIFO head to claim a later row), even
// when the store has not materialized the current control lease locally.
// Listener code uses ClaimNextQueuedTurnForOwner for this adoption/binding
// step; the legacy API is deliberately read-only at that boundary.
func TestUnscopedQueuedTurnClaimDoesNotCrossOwnerGenerationAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"json", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newTestStore(t)
			now := time.Now().UTC().Truncate(time.Microsecond)
			const sessionID = "unscoped-claim-owner-session"
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive}
				state.Turns["queued-a-bound"] = Turn{
					ID: "queued-a-bound", SessionID: sessionID, Status: TurnStatusQueued,
					MachineID: "machine-other", LeaseGeneration: 7,
					QueuedAt: now, CreatedAt: now, UpdatedAt: now,
				}
				state.Turns["queued-z-unbound"] = Turn{
					ID: "queued-z-unbound", SessionID: sessionID, Status: TurnStatusQueued,
					QueuedAt: now.Add(time.Second), CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
				}
				return nil
			}); err != nil {
				t.Fatalf("seed queued turns: %v", err)
			}
			if backend == "sqlite" {
				migrateStoreToSQLiteForTest(t, store)
			}

			claimed, ok, err := store.ClaimNextQueuedTurn(ctx, sessionID)
			if err != nil {
				t.Fatalf("unscoped claim: %v", err)
			}
			if ok || claimed.ID != "" {
				t.Fatalf("unscoped claim = %#v ok=%v, crossed another owner's FIFO head", claimed, ok)
			}

			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("reload after unscoped claim: %v", err)
			}
			if got := state.Turns["queued-a-bound"]; got.Status != TurnStatusQueued || got.LeaseGeneration != 7 || got.MachineID != "machine-other" {
				t.Fatalf("bound FIFO head changed after unscoped claim: %#v", got)
			}
			if got := state.Turns["queued-z-unbound"]; got.Status != TurnStatusQueued {
				t.Fatalf("later unbound turn was claimed past bound FIFO head: %#v", got)
			}
		})
	}
}
