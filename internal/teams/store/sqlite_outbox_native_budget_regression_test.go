//go:build !race

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// seedSQLiteNativeAdmissionBudgetFixture creates a prepared SQLite store and
// then adds raw, projection-valid rows.  The rows intentionally exercise the
// two cases where a scalar candidate lane cannot prove the canonical schedule:
// a canonical future retry hidden behind a due scalar, and a canonical due
// retry hidden behind a future scalar.  The latter is the exceptional schedule
// lane that used to scan every matching row without a total bound.
func seedSQLiteNativeAdmissionBudgetFixture(t *testing.T, count int, deliverAfter, nextAttemptAt time.Time) (*Store, string, time.Time) {
	t.Helper()
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	chatID := "chat:native-admission-budget"
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages = map[string]OutboxMessage{}
		return nil
	}); err != nil {
		t.Fatalf("seed native admission budget state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	if err := store.PrepareOutboxProjection(ctx); err != nil {
		t.Fatalf("prepare native admission budget projection: %v", err)
	}

	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		for i := 0; i < count; i++ {
			created := now.Add(time.Duration(i+1) * time.Nanosecond)
			message := OutboxMessage{
				ID:            fmt.Sprintf("outbox:native-admission-budget:%05d", i),
				TeamsChatID:   chatID,
				Kind:          "helper-status",
				Body:          "native admission budget fixture",
				Status:        OutboxStatusQueued,
				Sequence:      int64(i + 1),
				CreatedAt:     created,
				UpdatedAt:     now,
				NextAttemptAt: nextAttemptAt,
			}
			raw, err := json.Marshal(message)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO outbox_messages(
id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence,
created_at, deliver_after, post_send_effects_pending, json)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				message.ID, "", "", chatID, "", string(message.Status), message.Sequence,
				sqliteTime(created), sqliteTime(deliverAfter), 0, raw); err != nil {
				return err
			}
		}
		return nil
	})
	return store, chatID, now
}

func nativeAdmissionFastDB(t *testing.T, store *Store) *sql.DB {
	t.Helper()
	ctx := context.Background()
	var db *sql.DB
	if err := store.withStateLock(ctx, func() error {
		pointer, ok, err := store.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("store is not backed by sqlite")
		}
		db, err = store.sqliteDBUnlocked(pointer)
		return err
	}); err != nil {
		t.Fatalf("open native admission database: %v", err)
	}
	return db
}

func TestSQLiteNativeOutboxAdmissionBoundsScalarScanBeforeCanonicalFallback(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	store, _, now := seedSQLiteNativeAdmissionBudgetFixture(t, int(sqliteOutboxNativeAdmissionMaxRows)+1, time.Time{}, base.Add(time.Hour))
	db := nativeAdmissionFastDB(t, store)
	var headRows int
	previousHeadHook := sqliteOutboxPendingChatHeadRowTestHook
	sqliteOutboxPendingChatHeadRowTestHook = func() { headRows++ }
	t.Cleanup(func() { sqliteOutboxPendingChatHeadRowTestHook = previousHeadHook })

	var chatIDs []string
	var chatErr error
	if err := store.withStateLock(ctx, func() error {
		chatIDs, chatErr = pendingOutboxChatIDsAtSQLiteFast(ctx, db, PendingOutboxQuery{Now: now, Limit: 1}, 1)
		return nil
	}); err != nil {
		t.Fatalf("run scalar chat admission: %v", err)
	}
	if chatErr != nil || len(chatIDs) != 0 {
		t.Fatalf("scalar chat admission = %#v err=%v, want one blocked chat with no admitted IDs", chatIDs, chatErr)
	}
	if headRows != 1 {
		t.Fatalf("scalar chat admission decoded %d head rows, want exactly one", headRows)
	}

	var page PendingOutboxPage
	var pageErr error
	if err := store.withStateLock(ctx, func() error {
		page, pageErr = pendingOutboxPageAtSQLiteFast(ctx, db, PendingOutboxQuery{Now: now, Limit: 1})
		return nil
	}); err != nil {
		t.Fatalf("run scalar page admission: %v", err)
	}
	if !errors.Is(pageErr, errSQLiteOutboxProjectionFallback) || len(page.Messages) != 0 {
		t.Fatalf("scalar page admission = %#v err=%v, want bounded fallback", page, pageErr)
	}
}

func TestSQLiteAmbiguousOutboxAdmissionUsesBoundedNativeProjection(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages = map[string]OutboxMessage{}
		return nil
	}); err != nil {
		t.Fatalf("clear outbox state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		for i := 0; i < 38; i++ {
			created := now.Add(time.Duration(i) * time.Nanosecond)
			message := OutboxMessage{
				ID:              fmt.Sprintf("outbox:ambiguous-native:%03d", i),
				TeamsChatID:     "chat:ambiguous-native",
				Kind:            "helper",
				Body:            "bounded ambiguous recovery",
				Status:          OutboxStatusSending,
				Sequence:        int64(i + 1),
				CreatedAt:       created,
				UpdatedAt:       created,
				LastSendAttempt: now.Add(-time.Hour),
				LastSendError:   "ambiguous Graph send; previous owner stopped before durable Graph identity",
			}
			raw, err := json.Marshal(message)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO outbox_messages(
id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence,
created_at, deliver_after, post_send_effects_pending, json)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				message.ID, message.SessionID, message.TurnID, message.TeamsChatID, message.TeamsMessageID,
				string(message.Status), message.Sequence, sqliteTime(message.CreatedAt), sqliteTime(message.NextAttemptAt), 0, raw); err != nil {
				return err
			}
		}
		return nil
	})
	if err := store.PrepareOutboxProjection(ctx); err != nil {
		t.Fatalf("prepare ambiguous outbox projection: %v", err)
	}

	fallbacks := 0
	previousHook := sqliteOutboxPendingPageCanonicalFallbackTestHook
	sqliteOutboxPendingPageCanonicalFallbackTestHook = func() { fallbacks++ }
	t.Cleanup(func() { sqliteOutboxPendingPageCanonicalFallbackTestHook = previousHook })

	page, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{
		Now:                  now,
		Limit:                2,
		IncludeActiveSending: true,
		IncludeAmbiguous:     true,
		AmbiguousOnly:        true,
		IgnoreRateLimit:      true,
	})
	if err != nil {
		t.Fatalf("ambiguous native page: %v", err)
	}
	if len(page.Messages) != 2 || !page.More {
		t.Fatalf("ambiguous native page = messages=%d more=%t, want 2 and more", len(page.Messages), page.More)
	}
	if page.Messages[0].ID != "outbox:ambiguous-native:000" || page.Messages[1].ID != "outbox:ambiguous-native:001" {
		t.Fatalf("ambiguous native page IDs=%q,%q, want oldest two", page.Messages[0].ID, page.Messages[1].ID)
	}
	if fallbacks != 0 {
		t.Fatalf("ambiguous native page used canonical fallback %d times, want bounded scalar path", fallbacks)
	}
}

func TestSQLiteNativeOutboxAdmissionBoundsExceptionalScheduleScan(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	store, chatID, now := seedSQLiteNativeAdmissionBudgetFixture(t, int(sqliteOutboxNativeAdmissionMaxRows)+1, base.Add(time.Hour), base.Add(-time.Hour))
	db := nativeAdmissionFastDB(t, store)
	var headRows int
	previousHeadHook := sqliteOutboxPendingChatHeadRowTestHook
	sqliteOutboxPendingChatHeadRowTestHook = func() { headRows++ }
	t.Cleanup(func() { sqliteOutboxPendingChatHeadRowTestHook = previousHeadHook })

	var chatIDs []string
	var chatErr error
	if err := store.withStateLock(ctx, func() error {
		chatIDs, chatErr = pendingOutboxChatIDsAtSQLiteFast(ctx, db, PendingOutboxQuery{Now: now, Limit: 1}, 1)
		return nil
	}); err != nil {
		t.Fatalf("run exceptional chat admission: %v", err)
	}
	if chatErr != nil || len(chatIDs) != 1 || chatIDs[0] != chatID {
		t.Fatalf("exceptional chat admission = %#v err=%v, want the canonical-due chat", chatIDs, chatErr)
	}
	if headRows != 1 {
		t.Fatalf("exceptional chat admission decoded %d head rows, want exactly one", headRows)
	}

	var page PendingOutboxPage
	var pageErr error
	if err := store.withStateLock(ctx, func() error {
		page, pageErr = pendingOutboxPageAtSQLiteFast(ctx, db, PendingOutboxQuery{
			Now: now, TeamsChatID: chatID, Limit: 1,
		})
		return nil
	}); err != nil {
		t.Fatalf("run exceptional targeted page admission: %v", err)
	}
	if !errors.Is(pageErr, errSQLiteOutboxProjectionFallback) || len(page.Messages) != 0 {
		t.Fatalf("exceptional targeted page admission = %#v err=%v, want bounded fallback", page, pageErr)
	}

	// The public caller must treat the sentinel as a compatibility fallback,
	// never as an empty successful page.  Verify that the canonical lane still
	// exposes the due row after the bounded native probe gives up.
	fallbackCalled := make(chan struct{}, 1)
	previousHook := sqliteOutboxPendingPageCanonicalFallbackTestHook
	sqliteOutboxPendingPageCanonicalFallbackTestHook = func() {
		select {
		case fallbackCalled <- struct{}{}:
		default:
		}
	}
	t.Cleanup(func() { sqliteOutboxPendingPageCanonicalFallbackTestHook = previousHook })
	publicPage, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now, TeamsChatID: chatID, Limit: 1})
	if err != nil {
		t.Fatalf("public exceptional page: %v", err)
	}
	if len(publicPage.Messages) != 1 || publicPage.Messages[0].TeamsChatID != chatID {
		t.Fatalf("public exceptional page = %#v, want one due message for %q", publicPage, chatID)
	}
	select {
	case <-fallbackCalled:
	default:
		t.Fatal("public exceptional page did not use canonical fallback after native scan bound")
	}
}

func TestSQLitePendingOutboxTargetedPageUsesChatOrderIndex(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	store, chatID, now := seedSQLiteNativeAdmissionBudgetFixture(t, 128, time.Time{}, base.Add(-time.Hour))
	db := nativeAdmissionFastDB(t, store)

	if err := store.withStateLock(ctx, func() error {
		rows, err := db.QueryContext(ctx, `EXPLAIN QUERY PLAN SELECT `+sqliteOutboxProjectionSelect("o")+`
FROM outbox_messages o
WHERE o.status IN ('queued', 'sending', 'accepted')
  AND o.teams_chat_id = ?
  AND o.id IS NOT NULL AND trim(o.id) <> ''
ORDER BY o.created_at, o.id LIMIT ?`, chatID, 65)
		if err != nil {
			return err
		}
		defer rows.Close()
		var details []string
		for rows.Next() {
			var selectID, parentID, unused int
			var detail string
			if err := rows.Scan(&selectID, &parentID, &unused, &detail); err != nil {
				return err
			}
			details = append(details, detail)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		plan := strings.Join(details, " | ")
		if !strings.Contains(plan, "outbox_chat_pending_order_idx") {
			return fmt.Errorf("targeted pending page plan=%q, want outbox_chat_pending_order_idx", plan)
		}
		if strings.Contains(plan, "USE TEMP B-TREE FOR ORDER BY") {
			return fmt.Errorf("targeted pending page plan=%q, want no temporary sort", plan)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	page, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now, TeamsChatID: chatID, Limit: 64})
	if err != nil {
		t.Fatalf("targeted pending page: %v", err)
	}
	if len(page.Messages) != 64 || !page.More {
		t.Fatalf("targeted pending page messages=%d more=%t, want 64 and more", len(page.Messages), page.More)
	}
}

func TestSQLitePendingOutboxChatAdmissionUsesChatOrderIndex(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	store, chatID, now := seedSQLiteNativeAdmissionBudgetFixture(t, 128, time.Time{}, base.Add(-time.Hour))
	db := nativeAdmissionFastDB(t, store)

	if err := store.withStateLock(ctx, func() error {
		rows, err := db.QueryContext(ctx, `EXPLAIN QUERY PLAN SELECT o.teams_chat_id
FROM outbox_messages o INDEXED BY outbox_chat_pending_order_idx
WHERE o.status IN ('queued', 'sending', 'accepted')
  AND o.teams_chat_id <> ''
  AND o.id IS NOT NULL AND o.id <> ''
  AND typeof(o.created_at) IN ('integer', 'real')
  AND o.created_at = CAST(o.created_at AS INTEGER)
GROUP BY o.teams_chat_id
ORDER BY o.teams_chat_id LIMIT ?`, 65)
		if err != nil {
			return err
		}
		defer rows.Close()
		var details []string
		for rows.Next() {
			var selectID, parentID, unused int
			var detail string
			if err := rows.Scan(&selectID, &parentID, &unused, &detail); err != nil {
				return err
			}
			details = append(details, detail)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		plan := strings.Join(details, " | ")
		if !strings.Contains(plan, "outbox_chat_pending_order_idx") {
			return fmt.Errorf("chat admission plan=%q, want outbox_chat_pending_order_idx", plan)
		}
		if strings.Contains(plan, "USE TEMP B-TREE FOR GROUP BY") || strings.Contains(plan, "USE TEMP B-TREE FOR ORDER BY") {
			return fmt.Errorf("chat admission plan=%q, want no temporary grouping/order sort", plan)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	ids, err := store.PendingOutboxChatIDsAt(ctx, PendingOutboxQuery{Now: now}, 1)
	if err != nil {
		t.Fatalf("chat admission: %v", err)
	}
	if len(ids) != 1 || ids[0] != chatID {
		t.Fatalf("chat admission IDs=%v, want [%q]", ids, chatID)
	}
}

func TestSQLiteNativeOutboxChatAdmissionAppliesRateLimitAtHead(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	for _, message := range []OutboxMessage{
		{ID: "outbox:native-rate-blocked", TeamsChatID: "chat-a-blocked", Kind: "helper", Body: "blocked"},
		{ID: "outbox:native-rate-open", TeamsChatID: "chat-b-open", Kind: "helper", Body: "open"},
	} {
		if _, _, err := store.QueueOutbox(ctx, message); err != nil {
			t.Fatalf("QueueOutbox %s: %v", message.ID, err)
		}
	}
	migrateStoreToSQLiteForTest(t, store)
	if _, err := store.SetChatRateLimit(ctx, "chat-a-blocked", now.Add(time.Hour), "chat 429"); err != nil {
		t.Fatalf("SetChatRateLimit: %v", err)
	}
	ids, err := store.PendingOutboxChatIDsAt(ctx, PendingOutboxQuery{Now: now}, 8)
	if err != nil {
		t.Fatalf("chat-local rate-limited admission: %v", err)
	}
	if len(ids) != 1 || ids[0] != "chat-b-open" {
		t.Fatalf("chat-local rate-limited IDs=%v, want [chat-b-open]", ids)
	}
	if _, err := store.SetChatRateLimit(ctx, GraphWriteAccountRateLimitKey, now.Add(time.Hour), "account 429"); err != nil {
		t.Fatalf("SetChatRateLimit account: %v", err)
	}
	ids, err = store.PendingOutboxChatIDsAt(ctx, PendingOutboxQuery{Now: now}, 8)
	if err != nil {
		t.Fatalf("account rate-limited admission: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("account rate-limited IDs=%v, want none", ids)
	}
}

func TestSQLiteSchemaPreparationVersionFencesPendingChatOrderIndex(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if _, _, err := store.QueueOutbox(ctx, OutboxMessage{
		ID: "outbox:schema-index-fence", TeamsChatID: "chat-schema-index", Kind: "helper", Body: "index",
	}); err != nil {
		t.Fatalf("QueueOutbox: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DROP INDEX outbox_chat_pending_order_idx`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = '2' WHERE key = ?`, sqliteSchemaPreparationVersionKey)
		return err
	})
	if err := store.PrepareSQLiteSchemaBeforeOwner(ctx); err != nil {
		t.Fatalf("reprepare schema after index removal: %v", err)
	}
	db := nativeAdmissionFastDB(t, store)
	var indexName string
	if err := db.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?`, "outbox_chat_pending_order_idx").Scan(&indexName); err != nil {
		t.Fatalf("read recreated pending chat index: %v", err)
	}
	if indexName != "outbox_chat_pending_order_idx" {
		t.Fatalf("recreated index=%q", indexName)
	}
	if got := sqliteMetaValueForTest(t, store, sqliteSchemaPreparationVersionKey); got != sqliteSchemaPreparationVersion {
		t.Fatalf("schema marker=%q, want %q", got, sqliteSchemaPreparationVersion)
	}
}
