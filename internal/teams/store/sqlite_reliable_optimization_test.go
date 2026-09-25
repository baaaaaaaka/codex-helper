package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func seedSQLiteDedupeProjectionFixture(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions["session-dedupe-target"] = SessionContext{
			ID: "session-dedupe-target", Status: SessionStatusActive,
			TeamsChatID: "chat-dedupe-target", UpdatedAt: now,
		}
		state.Sessions["session-dedupe-other"] = SessionContext{
			ID: "session-dedupe-other", Status: SessionStatusActive,
			TeamsChatID: "chat-dedupe-other", UpdatedAt: now,
		}
		state.OutboxMessages["outbox-dedupe-target"] = OutboxMessage{
			ID: "outbox-dedupe-target", SessionID: "session-dedupe-target",
			TeamsChatID: "chat-dedupe-target", Kind: "progress", Body: "target",
			Status: OutboxStatusSent, Sequence: 1, CreatedAt: now, UpdatedAt: now,
		}
		state.OutboxMessages["outbox-dedupe-other"] = OutboxMessage{
			ID: "outbox-dedupe-other", SessionID: "session-dedupe-other",
			TeamsChatID: "chat-dedupe-other", Kind: "progress", Body: "other",
			Status: OutboxStatusSent, Sequence: 1, CreatedAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed dedupe fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	if err := store.PrepareOutboxProjection(ctx); err != nil {
		t.Fatalf("PrepareOutboxProjection: %v", err)
	}
	return store
}

func sqliteMetaValueForTest(t *testing.T, store *Store, key string) string {
	t.Helper()
	var value string
	if err := store.withStateLock(context.Background(), func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		value, err = sqliteReadMetaValueContext(context.Background(), db, key)
		return err
	}); err != nil {
		t.Fatalf("read SQLite meta %q: %v", key, err)
	}
	return value
}

func TestSQLiteSessionTranscriptDedupeUsesDedicatedSessionProjection(t *testing.T) {
	ctx := context.Background()
	store := seedSQLiteDedupeProjectionFixture(t)
	if got := sqliteMetaValueForTest(t, store, sqliteOutboxSessionProjectionTrustKey); got != sqliteOutboxSessionProjectionTrustTrusted {
		t.Fatalf("session projection trust = %q, want %q", got, sqliteOutboxSessionProjectionTrustTrusted)
	}

	state, err := store.SessionTranscriptDedupeSnapshot(ctx, "session-dedupe-target", "")
	if err != nil {
		t.Fatalf("SessionTranscriptDedupeSnapshot: %v", err)
	}
	if _, ok := state.OutboxMessages["outbox-dedupe-target"]; !ok {
		t.Fatalf("target outbox missing from dedupe snapshot: %#v", state.OutboxMessages)
	}
	if _, ok := state.OutboxMessages["outbox-dedupe-other"]; ok {
		t.Fatalf("other-session outbox leaked into dedupe snapshot: %#v", state.OutboxMessages)
	}

	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		rows, err := tx.Query(`EXPLAIN QUERY PLAN SELECT `+sqliteOutboxProjectionSelect("o")+`
FROM outbox_messages o WHERE o.session_id = ?`, "session-dedupe-target")
		if err != nil {
			return err
		}
		defer rows.Close()
		var details []string
		for rows.Next() {
			var selectID, parentID, notUsed int
			var detail string
			if err := rows.Scan(&selectID, &parentID, &notUsed, &detail); err != nil {
				return err
			}
			details = append(details, detail)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		plan := strings.Join(details, "\n")
		if !strings.Contains(plan, "outbox_session_idx") {
			return errors.New("session dedupe plan did not use outbox_session_idx: " + plan)
		}
		if strings.Contains(plan, "SCAN outbox_messages") {
			return errors.New("session dedupe plan unexpectedly scans outbox_messages: " + plan)
		}
		return nil
	})
}

func TestSQLiteSessionTranscriptDedupeDiscardsNativePrefixWhenMarkerChanges(t *testing.T) {
	ctx := context.Background()
	store := seedSQLiteDedupeProjectionFixture(t)
	changed := make(chan struct{})
	previousHook := sqliteSessionTranscriptDedupeSnapshotTestHook
	sqliteSessionTranscriptDedupeSnapshotTestHook = func(stage string) {
		if stage != "native-committed" {
			return
		}
		withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
			if err := sqliteWriteMetaValueContext(ctx, tx, sqliteOutboxSessionProjectionTrustKey, sqliteOutboxSessionProjectionTrustUntrusted); err != nil {
				return err
			}
			return nil
		})
		close(changed)
	}
	t.Cleanup(func() { sqliteSessionTranscriptDedupeSnapshotTestHook = previousHook })

	state, err := store.SessionTranscriptDedupeSnapshot(ctx, "session-dedupe-target", "")
	if err != nil {
		t.Fatalf("SessionTranscriptDedupeSnapshot after marker race: %v", err)
	}
	select {
	case <-changed:
	default:
		t.Fatal("native marker race hook was not reached")
	}
	if _, ok := state.OutboxMessages["outbox-dedupe-target"]; !ok {
		t.Fatalf("target outbox missing after canonical fallback: %#v", state.OutboxMessages)
	}
	if _, ok := state.OutboxMessages["outbox-dedupe-other"]; ok {
		t.Fatalf("other-session outbox leaked after canonical fallback: %#v", state.OutboxMessages)
	}
}

func bumpSQLiteDedupeOutboxGenerationForTest(t *testing.T, store *Store, outboxID string) {
	t.Helper()
	if err := store.Update(context.Background(), func(state *State) error {
		message, ok := state.OutboxMessages[outboxID]
		if !ok {
			return errors.New("dedupe outbox is missing")
		}
		message.UpdatedAt = message.UpdatedAt.Add(time.Nanosecond)
		state.OutboxMessages[message.ID] = message
		return nil
	}); err != nil {
		t.Fatalf("bump dedupe outbox generation: %v", err)
	}
}

func TestSQLiteSessionTranscriptDedupeKeepsCoherentSnapshotAcrossConcurrentOutboxUpdate(t *testing.T) {
	ctx := context.Background()
	store := seedSQLiteDedupeProjectionFixture(t)
	before, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load pre-race dedupe state: %v", err)
	}
	beforeOutbox := before.OutboxMessages["outbox-dedupe-target"]
	var attempts int
	previousHook := sqliteSessionTranscriptDedupeSnapshotTestHook
	sqliteSessionTranscriptDedupeSnapshotTestHook = func(stage string) {
		switch stage {
		case "transaction-opened":
			attempts++
		case "runtime-read":
			if attempts == 1 {
				bumpSQLiteDedupeOutboxGenerationForTest(t, store, "outbox-dedupe-target")
			}
		}
	}
	t.Cleanup(func() { sqliteSessionTranscriptDedupeSnapshotTestHook = previousHook })

	state, err := store.SessionTranscriptDedupeSnapshot(ctx, "session-dedupe-target", "")
	if err != nil {
		t.Fatalf("SessionTranscriptDedupeSnapshot after concurrent outbox update: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("dedupe snapshot attempts = %d, want one coherent read transaction", attempts)
	}
	gotOutbox, ok := state.OutboxMessages["outbox-dedupe-target"]
	if !ok {
		t.Fatalf("stable retry omitted target outbox: %#v", state.OutboxMessages)
	}
	if !gotOutbox.UpdatedAt.Equal(beforeOutbox.UpdatedAt) {
		t.Fatalf("snapshot outbox updated_at = %s, want point-in-time value %s", gotOutbox.UpdatedAt, beforeOutbox.UpdatedAt)
	}
	current, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load current dedupe state: %v", err)
	}
	if currentOutbox := current.OutboxMessages["outbox-dedupe-target"]; !currentOutbox.UpdatedAt.After(beforeOutbox.UpdatedAt) {
		t.Fatalf("concurrent durable outbox update was not committed: before=%s after=%s", beforeOutbox.UpdatedAt, currentOutbox.UpdatedAt)
	}
}

func TestSQLiteSessionTranscriptDedupeDoesNotRetryForUnrelatedOutboxGenerationChurn(t *testing.T) {
	store := seedSQLiteDedupeProjectionFixture(t)
	var attempts int
	previousHook := sqliteSessionTranscriptDedupeSnapshotTestHook
	sqliteSessionTranscriptDedupeSnapshotTestHook = func(stage string) {
		if stage == "opened" {
			attempts++
			bumpSQLiteDedupeOutboxGenerationForTest(t, store, "outbox-dedupe-other")
		}
	}
	t.Cleanup(func() { sqliteSessionTranscriptDedupeSnapshotTestHook = previousHook })

	state, err := store.SessionTranscriptDedupeSnapshot(context.Background(), "session-dedupe-target", "")
	if err != nil {
		t.Fatalf("SessionTranscriptDedupeSnapshot during unrelated outbox churn: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("dedupe snapshot attempts = %d, want one attempt despite unrelated generation churn", attempts)
	}
	if _, ok := state.OutboxMessages["outbox-dedupe-target"]; !ok {
		t.Fatalf("unrelated outbox churn hid target row: %#v", state.OutboxMessages)
	}
	if _, ok := state.OutboxMessages["outbox-dedupe-other"]; ok {
		t.Fatalf("unrelated session outbox leaked into dedupe snapshot: %#v", state.OutboxMessages)
	}
}

func TestSQLiteSessionTranscriptDedupeStaleSnapshotCannotDuplicateConcurrentOutbox(t *testing.T) {
	ctx := context.Background()
	store := seedSQLiteDedupeProjectionFixture(t)
	request := TranscriptDeliveryQueueRequest{
		Message: OutboxMessage{
			ID: "outbox-dedupe-race", SessionID: "session-dedupe-target",
			TurnID: "sync:session-dedupe-target", TeamsChatID: "chat-dedupe-target",
			Kind: "status-progress", Body: "concurrent transcript row",
			Status: OutboxStatusQueued, Sequence: 7,
			CreatedAt: time.Date(2026, 9, 12, 12, 1, 0, 0, time.UTC),
		},
		Delivery: TranscriptDeliveryRecord{
			ID: "delivery-dedupe-race", SessionID: "session-dedupe-target",
			OutboxID: "outbox-dedupe-race", Status: TranscriptDeliveryStatusQueued,
		},
	}
	var attempts int
	previousHook := sqliteSessionTranscriptDedupeSnapshotTestHook
	sqliteSessionTranscriptDedupeSnapshotTestHook = func(stage string) {
		switch stage {
		case "transaction-opened":
			attempts++
		case "runtime-read":
			if attempts == 1 {
				if err := store.Update(ctx, func(state *State) error {
					state.OutboxMessages[request.Message.ID] = request.Message
					return nil
				}); err != nil {
					t.Fatalf("insert concurrent deterministic transcript outbox: %v", err)
				}
			}
		}
	}
	t.Cleanup(func() { sqliteSessionTranscriptDedupeSnapshotTestHook = previousHook })

	snapshot, err := store.SessionTranscriptDedupeSnapshot(ctx, "session-dedupe-target", "")
	if err != nil {
		t.Fatalf("SessionTranscriptDedupeSnapshot during concurrent insert: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("dedupe snapshot attempts = %d, want one coherent read transaction", attempts)
	}
	if _, found := snapshot.OutboxMessages[request.Message.ID]; found {
		t.Fatalf("snapshot included a row committed after its SQLite read transaction began: %#v", snapshot.OutboxMessages[request.Message.ID])
	}

	queued, created, alreadyDelivered, err := store.QueueTranscriptDeliveryOutbox(ctx, request)
	if err != nil {
		t.Fatalf("QueueTranscriptDeliveryOutbox after stale preflight: %v", err)
	}
	if queued.ID != request.Message.ID || created || alreadyDelivered {
		t.Fatalf("QueueTranscriptDeliveryOutbox = %#v created=%t alreadyDelivered=%t; want existing deterministic row", queued, created, alreadyDelivered)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load after stale-preflight queue: %v", err)
	}
	if got := state.OutboxMessages[request.Message.ID]; got.ID != request.Message.ID {
		t.Fatalf("durable deterministic outbox after queue = %#v", got)
	}
	if got := state.TranscriptDeliveries[request.Delivery.ID]; got.OutboxID != request.Message.ID {
		t.Fatalf("durable delivery after stale-preflight queue = %#v", got)
	}
	count := 0
	for id := range state.OutboxMessages {
		if id == request.Message.ID {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("deterministic outbox row count = %d, want 1", count)
	}
}

func TestSQLiteSessionTranscriptDedupeReadsLatestSessionChangeBeforeSnapshot(t *testing.T) {
	ctx := context.Background()
	store := seedSQLiteDedupeProjectionFixture(t)
	var attempts int
	previousHook := sqliteSessionTranscriptDedupeSnapshotTestHook
	sqliteSessionTranscriptDedupeSnapshotTestHook = func(stage string) {
		if stage != "transaction-opened" {
			return
		}
		attempts++
		if attempts != 1 {
			return
		}
		withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `UPDATE sessions SET json = json_set(json, '$.status', 'closed') WHERE id = ?`, "session-dedupe-target")
			return err
		})
	}
	t.Cleanup(func() { sqliteSessionTranscriptDedupeSnapshotTestHook = previousHook })

	state, err := store.SessionTranscriptDedupeSnapshot(ctx, "session-dedupe-target", "")
	if err != nil {
		t.Fatalf("SessionTranscriptDedupeSnapshot after session change: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("dedupe snapshot attempts = %d, want one coherent read after pre-snapshot write", attempts)
	}
	if session := state.Sessions["session-dedupe-target"]; session.Status != SessionStatusClosed {
		t.Fatalf("dedupe snapshot missed session change committed before its first read: %#v", session)
	}
}

func TestSQLiteSessionTranscriptDedupeKeepsSnapshotAcrossConcurrentSessionWrite(t *testing.T) {
	ctx := context.Background()
	store := seedSQLiteDedupeProjectionFixture(t)
	var attempts int
	previousHook := sqliteSessionTranscriptDedupeSnapshotTestHook
	sqliteSessionTranscriptDedupeSnapshotTestHook = func(stage string) {
		if stage != "runtime-read" {
			return
		}
		attempts++
		if attempts != 1 {
			return
		}
		withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `UPDATE sessions SET status = ?, json = json_set(json, '$.status', ?) WHERE id = ?`, SessionStatusClosed, SessionStatusClosed, "session-dedupe-target")
			return err
		})
	}
	t.Cleanup(func() { sqliteSessionTranscriptDedupeSnapshotTestHook = previousHook })

	snapshot, err := store.SessionTranscriptDedupeSnapshot(ctx, "session-dedupe-target", "")
	if err != nil {
		t.Fatalf("SessionTranscriptDedupeSnapshot after concurrent non-outbox write: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("dedupe snapshot attempts = %d, want one stable SQLite snapshot", attempts)
	}
	if got := snapshot.Sessions["session-dedupe-target"].Status; got != SessionStatusActive {
		t.Fatalf("snapshot session status = %q, want the pre-write value from its pinned transaction", got)
	}
	current, err := store.SessionsByID(ctx, []string{"session-dedupe-target"})
	session, ok := current["session-dedupe-target"]
	if err != nil || !ok || session.Status != SessionStatusClosed {
		t.Fatalf("durable session after concurrent write = %#v err=%v; want closed", current, err)
	}
}

func TestSQLiteSessionTranscriptDedupeRejectsReplacedDatabase(t *testing.T) {
	ctx := context.Background()
	store := seedSQLiteDedupeProjectionFixture(t)
	statePath := store.Path()
	var snapshot sqliteOutboxReadSnapshot
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		snapshot.path, err = store.storeSQLitePath(pointer)
		if err != nil {
			return err
		}
		snapshot.identity, err = sqliteReadOnlyFileIdentityForPath(snapshot.path)
		return err
	}); err != nil {
		t.Fatalf("capture SQLite dedupe database identity: %v", err)
	}
	if !snapshot.identity.Exists {
		t.Fatalf("captured SQLite dedupe database identity is missing: %#v", snapshot.identity)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close fixture store before replacing database file: %v", err)
	}

	store, err := Open(statePath)
	if err != nil {
		t.Fatalf("reopen fixture store after closing SQLite handles: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close reopened fixture store: %v", err)
		}
	})

	replacementPath := snapshot.path + ".identity-test-replacement"
	originalPath := snapshot.path + ".identity-test-original"
	if err := os.WriteFile(replacementPath, []byte("replacement database file"), 0o600); err != nil {
		t.Fatalf("write replacement SQLite file: %v", err)
	}
	if err := os.Rename(snapshot.path, originalPath); err != nil {
		_ = os.Remove(replacementPath)
		t.Fatalf("move original SQLite database aside: %v", err)
	}
	if err := os.Rename(replacementPath, snapshot.path); err != nil {
		_ = os.Rename(originalPath, snapshot.path)
		t.Fatalf("install replacement SQLite database file: %v", err)
	}
	restored := false
	defer func() {
		if restored {
			return
		}
		_ = os.Remove(snapshot.path)
		_ = os.Rename(originalPath, snapshot.path)
	}()

	stable, err := store.sqliteOutboxReadDatabaseIdentityStable(ctx, snapshot)
	if err != nil {
		t.Fatalf("check replaced SQLite dedupe database identity: %v", err)
	}
	if stable {
		t.Fatalf("database replacement was accepted as the captured dedupe database: before=%#v", snapshot.identity)
	}

	if err := os.Remove(snapshot.path); err != nil {
		t.Fatalf("remove disposable replacement SQLite file: %v", err)
	}
	if err := os.Rename(originalPath, snapshot.path); err != nil {
		t.Fatalf("restore original SQLite database file: %v", err)
	}
	restored = true
}

func TestSQLiteStoredInt64RejectsFractionalReal(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages["fractional-sequence"] = OutboxMessage{
			ID: "fractional-sequence", Status: OutboxStatusQueued, Sequence: 1,
			CreatedAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed fractional sequence fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	var got int64
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, `UPDATE outbox_messages SET sequence = 1.5 WHERE id = ?`, "fractional-sequence"); err != nil {
			return err
		}
		return db.QueryRowContext(ctx, `SELECT `+sqliteStoredInt64SQL("sequence")+` FROM outbox_messages WHERE id = ?`, "fractional-sequence").Scan(&got)
	}); err != nil {
		t.Fatalf("read fractional sequence projection: %v", err)
	}
	if got != -1 {
		t.Fatalf("fractional REAL sequence decoded as %d, want fail-closed -1", got)
	}
}

func TestSQLiteSessionTranscriptDedupeFallsBackForMissingSessionScalar(t *testing.T) {
	ctx := context.Background()
	store := seedSQLiteDedupeProjectionFixture(t)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE outbox_messages SET session_id = NULL WHERE id = ?`, "outbox-dedupe-target")
		return err
	})
	if got := sqliteMetaValueForTest(t, store, sqliteOutboxSessionProjectionTrustKey); got != sqliteOutboxSessionProjectionTrustUntrusted {
		t.Fatalf("session projection trust after missing scalar = %q, want %q", got, sqliteOutboxSessionProjectionTrustUntrusted)
	}

	state, err := store.SessionTranscriptDedupeSnapshot(ctx, "session-dedupe-target", "")
	if err != nil {
		t.Fatalf("SessionTranscriptDedupeSnapshot after scalar contradiction: %v", err)
	}
	if _, ok := state.OutboxMessages["outbox-dedupe-target"]; !ok {
		t.Fatalf("canonical JSON fallback omitted target with missing scalar: %#v", state.OutboxMessages)
	}
}

func TestSQLiteSessionTranscriptDedupeScanDoesNotHoldStoreMutex(t *testing.T) {
	ctx := context.Background()
	store := seedSQLiteDedupeProjectionFixture(t)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		return sqliteWriteMetaValueContext(ctx, tx, sqliteOutboxSessionProjectionTrustKey, sqliteOutboxSessionProjectionTrustUntrusted)
	})

	opened := make(chan struct{})
	release := make(chan struct{})
	var openedOnce sync.Once
	var releaseOnce sync.Once
	previousHook := sqliteSessionTranscriptDedupeSnapshotTestHook
	sqliteSessionTranscriptDedupeSnapshotTestHook = func(stage string) {
		if stage != "opened" {
			return
		}
		openedOnce.Do(func() { close(opened) })
		<-release
	}
	snapshotDone := make(chan error, 1)
	snapshotFinished := false
	go func() {
		_, err := store.SessionTranscriptDedupeSnapshot(ctx, "session-dedupe-target", "")
		snapshotDone <- err
	}()
	defer func() {
		releaseOnce.Do(func() { close(release) })
		if !snapshotFinished {
			select {
			case err := <-snapshotDone:
				if err != nil {
					t.Errorf("SessionTranscriptDedupeSnapshot cleanup: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("SessionTranscriptDedupeSnapshot cleanup timed out")
			}
		}
		sqliteSessionTranscriptDedupeSnapshotTestHook = previousHook
	}()

	select {
	case <-opened:
	case err := <-snapshotDone:
		snapshotFinished = true
		t.Fatalf("dedupe snapshot ended before its independent reader opened: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("dedupe snapshot did not open its independent reader")
	}

	// The canonical fallback is deliberately held. A foreground durable write
	// must still acquire Store.mu while the read-only compatibility scan is in
	// progress; otherwise one legacy session lookup can serialize all poll and
	// completion writes for the duration of the full outbox scan.
	updateDone := make(chan error, 1)
	go func() {
		_, err := store.UpdateChatPollSchedule(ctx, ChatPollScheduleUpdate{
			ChatID:     "chat-dedupe-target",
			PollState:  chatPollStateWarm,
			NextPollAt: time.Now().Add(time.Minute),
		})
		updateDone <- err
	}()
	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("durable schedule update while dedupe scans: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("durable schedule update waited for the canonical dedupe scan to finish")
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-snapshotDone; err != nil {
		t.Fatalf("SessionTranscriptDedupeSnapshot: %v", err)
	}
	snapshotFinished = true
}

func TestSQLiteSessionTranscriptDedupeFailsClosedForUndecodableCanonicalOutbox(t *testing.T) {
	ctx := context.Background()
	store := seedSQLiteDedupeProjectionFixture(t)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		// Keep the session identity and scalar projections unchanged. The body is
		// not part of the indexed FIFO contract, so this specifically verifies
		// that a row selected by the canonical session predicate cannot be
		// silently dropped when its typed payload is no longer decodable.
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = json_set(json, '$.body', 17) WHERE id = ?`, "outbox-dedupe-target")
		return err
	})

	state, err := store.SessionTranscriptDedupeSnapshot(ctx, "session-dedupe-target", "")
	if !errors.Is(err, ErrSQLiteOutboxProjectionUntrusted) {
		t.Fatalf("undecodable canonical outbox error = %v, want ErrSQLiteOutboxProjectionUntrusted", err)
	}
	if len(state.OutboxMessages) != 0 {
		t.Fatalf("failed-closed dedupe snapshot returned a partial map: %#v", state.OutboxMessages)
	}
}

func TestSQLiteSessionProjectionTrustSurvivesFullStateWrite(t *testing.T) {
	ctx := context.Background()
	store := seedSQLiteDedupeProjectionFixture(t)
	if err := store.Update(ctx, func(state *State) error {
		state.ChatRateLimits["chat-unrelated"] = ChatRateLimitState{
			ChatID: "chat-unrelated", BlockedUntil: time.Now().Add(time.Minute),
		}
		return nil
	}); err != nil {
		t.Fatalf("unrelated full-state update: %v", err)
	}
	if got := sqliteMetaValueForTest(t, store, sqliteOutboxSessionProjectionTrustKey); got != sqliteOutboxSessionProjectionTrustTrusted {
		t.Fatalf("session projection trust after full-state update = %q, want %q", got, sqliteOutboxSessionProjectionTrustTrusted)
	}
	if state, err := store.SessionTranscriptDedupeSnapshot(ctx, "session-dedupe-target", ""); err != nil {
		t.Fatalf("dedupe snapshot after full-state update: %v", err)
	} else if _, ok := state.OutboxMessages["outbox-dedupe-target"]; !ok {
		t.Fatalf("target outbox missing after preserved session trust marker: %#v", state.OutboxMessages)
	}
}

func TestSQLiteOutboxProjectionGuardReplacesStaleTrigger(t *testing.T) {
	ctx := context.Background()
	store := seedSQLiteDedupeProjectionFixture(t)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DROP TRIGGER outbox_session_projection_guard_update`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `CREATE TRIGGER outbox_session_projection_guard_update AFTER UPDATE OF json ON outbox_messages BEGIN SELECT 1; END`)
		return err
	})
	var stages []string
	previousAuditHook := sqliteOutboxAuditTestHook
	sqliteOutboxAuditTestHook = func(stage string) { stages = append(stages, stage) }
	t.Cleanup(func() { sqliteOutboxAuditTestHook = previousAuditHook })
	if err := store.PrepareOutboxProjection(ctx); err != nil {
		t.Fatalf("repair stale outbox trigger during PrepareOutboxProjection: %v", err)
	}
	if len(stages) == 0 || stages[0] != "opened" {
		t.Fatalf("stale-trigger Prepare audit stages = %v, want an explicit audit after trigger repair", stages)
	}
	for key, marker := range readOutboxProjectionTrustMarkersForTest(t, store) {
		if marker != sqliteOutboxProjectionTrustTrusted {
			t.Fatalf("stale-trigger repaired marker %s = %q, want trusted after Prepare audit", key, marker)
		}
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET session_id = NULL WHERE id = ?`, "outbox-dedupe-target")
		return err
	})
	if got := sqliteMetaValueForTest(t, store, sqliteOutboxSessionProjectionTrustKey); got != sqliteOutboxSessionProjectionTrustUntrusted {
		t.Fatalf("repaired session trigger did not revoke trust: %q", got)
	}
}

func TestSQLiteHotPollAdmissionReportsOnlyCorruptDueSession(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 12, 14, 0, 0, 0, time.UTC)
	store := newTestStore(t)
	const (
		sessionID = "session-only-corrupt"
		chatID    = "chat-only-corrupt"
	)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[sessionID] = SessionContext{
			ID: sessionID, Status: SessionStatusActive, TeamsChatID: chatID, UpdatedAt: now,
		}
		state.ChatPolls[chatID] = ChatPollState{
			ChatID: chatID, Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed only-corrupt fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	// Leave the indexed identity/schedule columns intact but make the canonical
	// session payload unreadable. This is the exact case where an empty result
	// must not be reported as an ordinary authoritative admission: a registry
	// session still exists, but it cannot safely be used for routing.
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE sessions SET json = ? WHERE id = ?`, []byte(`{"id":`), sessionID)
		return err
	})

	admission, err := store.HotPollWorkCandidatesWithDispositionExcludingIdleAt(ctx, "control-chat", time.Time{}, now)
	if err != nil {
		t.Fatalf("hot-poll admission: %v", err)
	}
	if admission.Disposition != HotPollWorkAdmissionDurableCorrupt {
		t.Fatalf("admission disposition = %q, want %q", admission.Disposition, HotPollWorkAdmissionDurableCorrupt)
	}
	if len(admission.Candidates) != 0 {
		t.Fatalf("corrupt session became runnable candidate: %#v", admission.Candidates)
	}
	if len(admission.CorruptSessions) != 1 {
		t.Fatalf("corrupt evidence = %#v, want one bounded row", admission.CorruptSessions)
	}
	evidence := admission.CorruptSessions[0]
	if evidence.SessionID != sessionID || evidence.TeamsChatID != chatID || evidence.SourceHash == "" || !evidence.HasPoll {
		t.Fatalf("corrupt evidence = %#v, want session/chat/hash/poll fence", evidence)
	}

	// The compatibility boolean remains handled=true for durable corruption so
	// old callers cannot silently revive a stale registry session. They can use
	// the disposition-aware API above to persist the explicit recovery gate.
	candidates, handled, err := store.HotPollWorkCandidatesExcludingIdleAt(ctx, "control-chat", time.Time{}, now)
	if err != nil {
		t.Fatalf("compatibility hot-poll admission: %v", err)
	}
	if !handled || len(candidates) != 0 {
		t.Fatalf("compatibility admission candidates=%#v handled=%v, want handled empty durable result", candidates, handled)
	}
}

func TestSQLiteHotPollAdmissionReportsTypedCorruptSessionField(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 12, 14, 1, 0, 0, time.UTC)
	store := newTestStore(t)
	const (
		sessionID = "session-typed-corrupt"
		chatID    = "chat-typed-corrupt"
	)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[sessionID] = SessionContext{
			ID: sessionID, Status: SessionStatusActive, TeamsChatID: chatID, UpdatedAt: now,
		}
		state.ChatPolls[chatID] = ChatPollState{
			ChatID: chatID, Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed typed-corrupt fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	// The identity and status remain readable, but model_generation has a JSON
	// type that encoding/json cannot unmarshal into SessionContext. The recovery
	// predicate must reject the row before the legacy lane can silently drop it.
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE sessions SET json = ? WHERE id = ?`,
			[]byte(`{"id":"session-typed-corrupt","teams_chat_id":"chat-typed-corrupt","status":"active","model_generation":"bad"}`), sessionID)
		return err
	})

	admission, err := store.HotPollWorkCandidatesWithDispositionExcludingIdleAt(ctx, "control-chat", time.Time{}, now)
	if err != nil {
		t.Fatalf("typed-corrupt hot-poll admission: %v", err)
	}
	if admission.Disposition != HotPollWorkAdmissionDurableCorrupt || len(admission.Candidates) != 0 {
		t.Fatalf("typed-corrupt admission = %#v, want durable-corrupt with no runnable candidate", admission)
	}
	if len(admission.CorruptSessions) != 1 || admission.CorruptSessions[0].SessionID != sessionID || admission.CorruptSessions[0].TeamsChatID != chatID {
		t.Fatalf("typed-corrupt evidence = %#v, want one session-local recovery fence", admission.CorruptSessions)
	}
}

func TestSQLiteHotPollAdmissionReportsCorruptSessionAlongsideHealthyCandidate(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 12, 14, 2, 0, 0, time.UTC)
	store := newTestStore(t)
	const (
		healthySessionID = "session-healthy-with-corrupt-sibling"
		healthyChatID    = "chat-healthy-with-corrupt-sibling"
		corruptSessionID = "session-corrupt-with-healthy-sibling"
		corruptChatID    = "chat-corrupt-with-healthy-sibling"
	)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[healthySessionID] = SessionContext{
			ID: healthySessionID, Status: SessionStatusActive, TeamsChatID: healthyChatID, UpdatedAt: now,
		}
		state.Sessions[corruptSessionID] = SessionContext{
			ID: corruptSessionID, Status: SessionStatusActive, TeamsChatID: corruptChatID, UpdatedAt: now,
		}
		for _, chatID := range []string{healthyChatID, corruptChatID} {
			state.ChatPolls[chatID] = ChatPollState{
				ChatID: chatID, Seeded: true, PollState: chatPollStateWarm,
				NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: now,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed mixed healthy/corrupt fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE sessions SET json = ? WHERE id = ?`, []byte(`{"id":`), corruptSessionID)
		return err
	})

	admission, err := store.HotPollWorkCandidatesWithDispositionExcludingIdleAt(ctx, "control-chat", time.Time{}, now)
	if err != nil {
		t.Fatalf("mixed healthy/corrupt hot-poll admission: %v", err)
	}
	if admission.Disposition != HotPollWorkAdmissionDurableCorrupt {
		t.Fatalf("mixed admission disposition = %q, want %q", admission.Disposition, HotPollWorkAdmissionDurableCorrupt)
	}
	if len(admission.Candidates) != 1 || admission.Candidates[0].ID != healthySessionID {
		t.Fatalf("mixed admission candidates = %#v, want only healthy session", admission.Candidates)
	}
	if len(admission.CorruptSessions) != 1 || admission.CorruptSessions[0].SessionID != corruptSessionID || admission.CorruptSessions[0].TeamsChatID != corruptChatID {
		t.Fatalf("mixed admission corrupt evidence = %#v, want corrupt sibling", admission.CorruptSessions)
	}
}

func TestSQLiteHotPollAdmissionReportsContradictorySessionIdentity(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 12, 14, 5, 0, 0, time.UTC)
	store := newTestStore(t)
	const (
		sessionID     = "session-contradictory"
		chatID        = "chat-contradictory"
		canonicalChat = "chat-from-another-row"
	)
	if err := store.Update(ctx, func(state *State) error {
		state.Sessions[sessionID] = SessionContext{ID: sessionID, Status: SessionStatusActive, TeamsChatID: chatID, UpdatedAt: now}
		state.ChatPolls[chatID] = ChatPollState{ChatID: chatID, Seeded: true, PollState: chatPollStateWarm, NextPollAt: now.Add(-time.Minute), UpdatedAt: now}
		state.ChatPolls[canonicalChat] = ChatPollState{ChatID: canonicalChat, Seeded: true, PollState: chatPollStateWarm, NextPollAt: now.Add(-time.Minute), UpdatedAt: now}
		return nil
	}); err != nil {
		t.Fatalf("seed contradictory fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		var raw []byte
		if err := tx.QueryRowContext(ctx, `SELECT json FROM sessions WHERE id = ?`, sessionID).Scan(&raw); err != nil {
			return err
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil {
			return err
		}
		encodedChat, err := json.Marshal("chat-from-another-row")
		if err != nil {
			return err
		}
		encodedTime, err := json.Marshal("not-a-time")
		if err != nil {
			return err
		}
		object["teams_chat_id"] = encodedChat
		// Keep the canonical identity parseable while making the whole session
		// undecodable. This forces the bounded recovery lane to choose between
		// the contradictory scalar chat and the canonical JSON chat.
		object["created_at"] = encodedTime
		changed, err := json.Marshal(object)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE sessions SET json = ? WHERE id = ?`, changed, sessionID)
		return err
	})

	admission, err := store.HotPollWorkCandidatesWithDispositionExcludingIdleAt(ctx, "control-chat", time.Time{}, now)
	if err != nil {
		t.Fatalf("contradictory admission: %v", err)
	}
	if admission.Disposition != HotPollWorkAdmissionDurableCorrupt || len(admission.Candidates) != 0 || len(admission.CorruptSessions) != 1 {
		t.Fatalf("contradictory admission = %#v, want one durable-corrupt disposition", admission)
	}
	if admission.CorruptSessions[0].SessionID != sessionID || admission.CorruptSessions[0].TeamsChatID != canonicalChat || !admission.CorruptSessions[0].HasPoll || !admission.CorruptSessions[0].HasPollJSON {
		t.Fatalf("contradictory evidence = %#v", admission.CorruptSessions[0])
	}
}

func TestSQLiteSessionTranscriptDedupePropagatesCancellation(t *testing.T) {
	store := seedSQLiteDedupeProjectionFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var loadErr error
	err := store.withStateLock(context.Background(), func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		_, loadErr = store.loadSQLiteSessionTranscriptDedupeStateUnlocked(ctx, pointer, "session-dedupe-target", "")
		return nil
	})
	if err != nil {
		t.Fatalf("invoke canceled dedupe snapshot: %v", err)
	}
	if !errors.Is(loadErr, context.Canceled) {
		t.Fatalf("dedupe loader error = %v, want context.Canceled", loadErr)
	}
}

func TestSQLiteSessionTranscriptDedupePropagatesCancellationAfterReadTransactionStarts(t *testing.T) {
	store := seedSQLiteDedupeProjectionFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var reachedSnapshot bool
	previousHook := sqliteSessionTranscriptDedupeSnapshotTestHook
	sqliteSessionTranscriptDedupeSnapshotTestHook = func(stage string) {
		if stage == "runtime-read" {
			reachedSnapshot = true
			cancel()
		}
	}
	t.Cleanup(func() { sqliteSessionTranscriptDedupeSnapshotTestHook = previousHook })

	state, err := store.SessionTranscriptDedupeSnapshot(ctx, "session-dedupe-target", "")
	if !reachedSnapshot {
		t.Fatal("dedupe snapshot did not establish its SQLite read transaction before cancellation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("dedupe snapshot error after in-flight cancellation = %v, want context.Canceled", err)
	}
	if len(state.OutboxMessages) != 0 || len(state.Sessions) != 0 || len(state.Turns) != 0 {
		t.Fatalf("canceled dedupe snapshot returned partial state: sessions=%d turns=%d outbox=%d", len(state.Sessions), len(state.Turns), len(state.OutboxMessages))
	}
}

func TestSQLiteChatPollScheduleBackfillPreservesOmittedScalarDeadlines(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 12, 13, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.ChatPolls["chat-backfill-omitted"] = ChatPollState{
			ChatID: "chat-backfill-omitted", Seeded: true, PollState: "warm",
			NextPollAt: now.Add(11 * time.Minute), BlockedUntil: now.Add(13 * time.Minute),
			UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed chat poll: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	var raw []byte
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if err := tx.QueryRow(`SELECT json FROM chat_polls WHERE chat_id = ?`, "chat-backfill-omitted").Scan(&raw); err != nil {
			return err
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil {
			return err
		}
		delete(object, "next_poll_at")
		delete(object, "blocked_until")
		legacyNext := sqliteTime(now.Add(11 * time.Minute))
		legacyBlocked := sqliteTime(now.Add(13 * time.Minute))
		encoded, err := json.Marshal(object)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE chat_polls SET json = ?, next_poll_at = ?, blocked_until = ? WHERE chat_id = ?`, encoded, legacyNext, legacyBlocked, "chat-backfill-omitted"); err != nil {
			return err
		}
		_, err = tx.Exec(`DELETE FROM state_meta WHERE key IN (?, ?)`, sqliteChatPollScheduleProjectionVersionKey, sqliteBackfillCursorKey(sqliteChatPollScheduleProjectionVersionKey))
		return err
	})

	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		return backfillSQLiteChatPollScheduleColumns(db)
	}); err != nil {
		t.Fatalf("backfill chat poll schedule: %v", err)
	}

	var next, blocked int64
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		return tx.QueryRow(`SELECT next_poll_at, blocked_until FROM chat_polls WHERE chat_id = ?`, "chat-backfill-omitted").Scan(&next, &blocked)
	})
	if next != sqliteTime(now.Add(11*time.Minute)) || blocked != sqliteTime(now.Add(13*time.Minute)) {
		t.Fatalf("backfill deadlines next=%d blocked=%d, want %d/%d", next, blocked, sqliteTime(now.Add(11*time.Minute)), sqliteTime(now.Add(13*time.Minute)))
	}
}

func TestSQLiteOpaqueChatPollFullStateWritePreservesLivenessSidecars(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 12, 13, 30, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.ChatPolls["chat-opaque-sidecars"] = ChatPollState{
			ChatID: "chat-opaque-sidecars", Seeded: true, PollState: "blocked",
			NextPollAt: now.Add(5 * time.Minute), BlockedUntil: now.Add(7 * time.Minute),
			LastActivityAt: now, ParkedAt: now.Add(-time.Hour), ParkNoticeSentAt: now.Add(-30 * time.Minute),
			LastSuccessfulPollAt: now.Add(-2 * time.Hour), LastError: "Graph 429", LastErrorAt: now,
			FailureCount: 7, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed opaque chat-poll fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	// Keep the operational scalar state valid while making the canonical JSON
	// semantically opaque. This is the exact forensic row a full-state rewrite
	// must preserve: it cannot be decoded into ChatPollState, but its retry,
	// frontier, and attempt fences are still durable evidence.
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE chat_polls SET
			json = ?, recovery_required = 1, next_poll_at = ?, blocked_until = ?,
			frontier_active = 1, admission_valid = 0, poll_failure_count = 7,
			pending_page_active = 1, attempt_active = 1, canonical_revision = 41,
			projection_revision = 40, projection_trusted = 0, updated_at = ?
			WHERE chat_id = ?`,
			[]byte(`{"chat_id":"chat-opaque-sidecars","pending_page":{}}`),
			sqliteTime(now.Add(5*time.Minute)), sqliteTime(now.Add(7*time.Minute)), sqliteTime(now), "chat-opaque-sidecars")
		return err
	})

	if err := store.Update(ctx, func(state *State) error {
		state.ChatRateLimits["unrelated"] = ChatRateLimitState{ChatID: "unrelated", BlockedUntil: now.Add(time.Hour)}
		return nil
	}); err != nil {
		t.Fatalf("unrelated full-state write: %v", err)
	}

	var raw []byte
	var recovery, next, blocked, frontier, admission, failure, pending, attempt, canonical, projection, trusted, updated int64
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT json, recovery_required, next_poll_at, blocked_until,
			frontier_active, admission_valid, poll_failure_count, pending_page_active,
			attempt_active, canonical_revision, projection_revision, projection_trusted, updated_at
			FROM chat_polls WHERE chat_id = ?`, "chat-opaque-sidecars").Scan(
			&raw, &recovery, &next, &blocked, &frontier, &admission, &failure, &pending,
			&attempt, &canonical, &projection, &trusted, &updated)
	})
	if string(raw) != `{"chat_id":"chat-opaque-sidecars","pending_page":{}}` {
		t.Fatalf("opaque chat-poll JSON changed across unrelated full-state write: %s", raw)
	}
	if recovery != 1 || next != sqliteTime(now.Add(5*time.Minute)) || blocked != sqliteTime(now.Add(7*time.Minute)) ||
		frontier != 1 || admission != 0 || failure != 7 || pending != 1 || attempt != 1 ||
		canonical != 41 || projection != 40 || trusted != 0 || updated != sqliteTime(now) {
		t.Fatalf("opaque chat-poll sidecars changed across unrelated full-state write: recovery=%d next=%d blocked=%d frontier=%d admission=%d failure=%d pending=%d attempt=%d canonical=%d projection=%d trusted=%d updated=%d",
			recovery, next, blocked, frontier, admission, failure, pending, attempt, canonical, projection, trusted, updated)
	}
}

func TestSQLiteOpaqueChatPollScheduleBackfillPreservesLivenessSidecars(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 12, 13, 45, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.ChatPolls["chat-opaque-backfill"] = ChatPollState{
			ChatID: "chat-opaque-backfill", Seeded: true, PollState: "blocked", UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed opaque backfill fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE chat_polls SET
			json = ?, recovery_required = 1, next_poll_at = ?, blocked_until = ?,
			frontier_active = 1, admission_valid = 0, poll_failure_count = 9,
			pending_page_active = 1, attempt_active = 1, canonical_revision = 12,
			projection_revision = 11, projection_trusted = 0, updated_at = ?
			WHERE chat_id = ?`,
			[]byte(`{"chat_id":"chat-opaque-backfill","pending_page":{}}`),
			sqliteTime(now.Add(2*time.Minute)), sqliteTime(now.Add(4*time.Minute)), sqliteTime(now), "chat-opaque-backfill"); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM state_meta WHERE key = ?`, sqliteChatPollScheduleProjectionVersionKey)
		return err
	})

	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		return backfillSQLiteChatPollScheduleColumns(db)
	}); err != nil {
		t.Fatalf("opaque chat-poll schedule backfill: %v", err)
	}

	var recovery, next, blocked, frontier, admission, failure, pending, attempt int64
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT recovery_required, next_poll_at, blocked_until,
			frontier_active, admission_valid, poll_failure_count, pending_page_active, attempt_active
			FROM chat_polls WHERE chat_id = ?`, "chat-opaque-backfill").Scan(
			&recovery, &next, &blocked, &frontier, &admission, &failure, &pending, &attempt)
	})
	if recovery != 1 || next != sqliteTime(now.Add(2*time.Minute)) || blocked != sqliteTime(now.Add(4*time.Minute)) ||
		frontier != 1 || admission != 0 || failure != 9 || pending != 1 || attempt != 1 {
		t.Fatalf("opaque chat-poll sidecars changed during schedule backfill: recovery=%d next=%d blocked=%d frontier=%d admission=%d failure=%d pending=%d attempt=%d",
			recovery, next, blocked, frontier, admission, failure, pending, attempt)
	}
}

// The compatibility admission lane is invoked while the listener is trying
// to discover work. It must remain a pure read: repeatedly rewriting
// admission_valid for a malformed row would acquire SQLite's writer lock and
// can block inbound/outbox durable progress on every poll cycle.
func TestSQLiteHotPollLegacyRecoveryIsPureRead(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t)
	const chatID = "chat-hot-poll-pure-read"
	if err := store.Update(ctx, func(state *State) error {
		state.ChatPolls[chatID] = ChatPollState{
			ChatID: chatID, Seeded: true, PollState: chatPollStateWarm,
			NextPollAt: now.Add(-time.Minute), UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed pure-read fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		// The first update models an opaque writer. Restore admission_valid in a
		// separate statement so the row is eligible for the bounded recovery
		// lane while still retaining invalid JSON.
		if _, err := tx.ExecContext(ctx, `UPDATE chat_polls SET json = ? WHERE chat_id = ?`, []byte(`{"chat_id":`), chatID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE chat_polls SET admission_valid = 1 WHERE chat_id = ?`, chatID)
		return err
	})

	var beforeRaw []byte
	var beforeAdmission int64
	var beforeVersion int64
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		if err := db.QueryRowContext(ctx, `SELECT json, admission_valid FROM chat_polls WHERE chat_id = ?`, chatID).Scan(&beforeRaw, &beforeAdmission); err != nil {
			return err
		}
		return db.QueryRowContext(ctx, `PRAGMA data_version`).Scan(&beforeVersion)
	}); err != nil {
		t.Fatalf("read pure-read baseline: %v", err)
	}

	var ids []string
	var malformed int
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		ids, malformed, err = loadSQLiteHotPollReadyChatIDsLegacyWithBudget(ctx, db, "", now, 8, 1, "1=1")
		return err
	}); err != nil {
		t.Fatalf("run pure-read recovery: %v", err)
	}
	if len(ids) != 1 || ids[0] != chatID || malformed != 1 {
		t.Fatalf("pure-read recovery ids=%v malformed=%d, want [%q]/1", ids, malformed, chatID)
	}

	var afterRaw []byte
	var afterAdmission int64
	var afterVersion int64
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err := store.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		if err := db.QueryRowContext(ctx, `SELECT json, admission_valid FROM chat_polls WHERE chat_id = ?`, chatID).Scan(&afterRaw, &afterAdmission); err != nil {
			return err
		}
		return db.QueryRowContext(ctx, `PRAGMA data_version`).Scan(&afterVersion)
	}); err != nil {
		t.Fatalf("read pure-read result: %v", err)
	}
	if string(afterRaw) != string(beforeRaw) || afterAdmission != beforeAdmission || afterVersion != beforeVersion {
		t.Fatalf("legacy recovery mutated SQLite: raw=%q/%q admission=%d/%d data_version=%d/%d", afterRaw, beforeRaw, afterAdmission, beforeAdmission, afterVersion, beforeVersion)
	}
}
