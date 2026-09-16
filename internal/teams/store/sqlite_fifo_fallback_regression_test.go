package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestJSONOpaqueOutboxPredecessorCannotBeSkipped(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 13, 2, 0, 0, 0, time.UTC)
	target := OutboxMessage{
		ID: "outbox:opaque-target", TeamsChatID: "chat:opaque-fifo", Status: OutboxStatusQueued,
		Sequence: 2, CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second), Body: "target",
	}
	targetRaw, err := json.Marshal(target)
	if err != nil {
		t.Fatalf("marshal target: %v", err)
	}
	root := map[string]json.RawMessage{}
	root["schema_version"], _ = json.Marshal(SchemaVersion)
	root["outbox_messages"], err = json.Marshal(map[string]json.RawMessage{
		"outbox:opaque-predecessor": json.RawMessage(`{"id":"outbox:opaque-predecessor","teams_chat_id":"chat:opaque-fifo","status":"queued","sequence":"not-an-integer","created_at":"2026-09-13T02:00:00Z"}`),
		target.ID:                   targetRaw,
	})
	if err != nil {
		t.Fatalf("marshal opaque outbox map: %v", err)
	}
	data, err := json.Marshal(root)
	if err != nil {
		t.Fatalf("marshal raw state: %v", err)
	}
	writeRawStoreStateForTest(t, store, data)

	loaded, err := store.OutboxMessageByID(ctx, target.ID)
	if err != nil {
		t.Fatalf("load target: %v", err)
	}
	_, found, proof, err := store.EarlierUnsentOutboxWithProof(ctx, loaded)
	if err != nil || found || proof == nil {
		t.Fatalf("opaque predecessor lookup found=%v proof=%v err=%v, want no typed predecessor and proof", found, proof != nil, err)
	}
	if !proof.jsonOpaqueOutbox {
		t.Fatal("JSON FIFO proof did not retain opaque outbox disposition")
	}
	if _, err := store.MarkOutboxSendAttemptWithFIFOSnapshotProof(ctx, loaded.ID, proof); !errors.Is(err, ErrOutboxPredecessorIndeterminate) {
		t.Fatalf("proof-aware claim error = %v, want ErrOutboxPredecessorIndeterminate", err)
	}
	if _, err := store.MarkOutboxSendAttempt(ctx, loaded.ID); !errors.Is(err, ErrOutboxPredecessorIndeterminate) {
		t.Fatalf("plain claim error = %v, want ErrOutboxPredecessorIndeterminate", err)
	}
	persisted, err := store.OutboxMessageByID(ctx, loaded.ID)
	if err != nil {
		t.Fatalf("reload target: %v", err)
	}
	if persisted.Status != OutboxStatusQueued {
		t.Fatalf("opaque predecessor claim changed target status to %q, want queued", persisted.Status)
	}
}

func sqliteOutboxGenerationForTest(t *testing.T, store *Store) int64 {
	t.Helper()
	var generation int64
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		var err error
		generation, err = sqliteReadOutboxGenerationContext(context.Background(), tx)
		return err
	})
	return generation
}

func insertSQLiteFIFOOutboxRowForTest(t *testing.T, tx *sql.Tx, id string, chatID string, sequence int64, at time.Time) error {
	t.Helper()
	canonicalTime := at.UTC().Format(time.RFC3339Nano)
	raw := fmt.Sprintf(`{"id":%q,"teams_chat_id":%q,"status":%q,"sequence":%d,"created_at":%q,"body":"fifo fallback test"}`,
		id, chatID, string(OutboxStatusQueued), sequence, canonicalTime)
	_, err := tx.ExecContext(context.Background(), `INSERT INTO outbox_messages
(id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence, created_at, deliver_after, post_send_effects_pending, json)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, "", "", chatID, "", string(OutboxStatusQueued), sequence, sqliteTime(at), 0, 0, []byte(raw))
	return err
}

func insertSQLiteFIFOOutboxRowsForTest(t *testing.T, tx *sql.Tx, chatID string, firstSequence, count int64, at time.Time) error {
	t.Helper()
	if count <= 0 {
		return nil
	}
	return withSQLiteOutboxProjectionTriggersDisabledForTest(t, tx, func() error {
		const batchSize int64 = 512
		for offset := int64(0); offset < count; {
			batchCount := count - offset
			if batchCount > batchSize {
				batchCount = batchSize
			}
			var query strings.Builder
			query.WriteString(`INSERT INTO outbox_messages
(id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence, created_at, deliver_after, post_send_effects_pending, json)
VALUES `)
			args := make([]any, 0, batchCount*11)
			for index := int64(0); index < batchCount; index++ {
				sequence := firstSequence + offset + index
				id := fmt.Sprintf("outbox:fallback-budget:%05d", sequence)
				rowAt := at.Add(time.Duration(sequence) * time.Nanosecond)
				canonicalTime := rowAt.UTC().Format(time.RFC3339Nano)
				raw := fmt.Sprintf(`{"id":%q,"teams_chat_id":%q,"status":%q,"sequence":%d,"created_at":%q,"body":"fifo fallback test"}`,
					id, chatID, string(OutboxStatusQueued), sequence, canonicalTime)
				if index != 0 {
					query.WriteString(",")
				}
				query.WriteString("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
				args = append(args, id, "", "", chatID, "", string(OutboxStatusQueued), sequence, sqliteTime(rowAt), 0, 0, []byte(raw))
			}
			if _, err := tx.ExecContext(context.Background(), query.String(), args...); err != nil {
				return err
			}
			offset += batchCount
		}
		return nil
	})
}

func withSQLiteOutboxProjectionTriggersDisabledForTest(t *testing.T, tx *sql.Tx, fn func() error) error {
	t.Helper()
	names := []string{
		"outbox_projection_guard_insert", "outbox_projection_guard_update",
		"outbox_session_projection_guard_insert", "outbox_session_projection_guard_update",
		"outbox_turn_projection_guard_insert", "outbox_turn_projection_guard_update",
		"outbox_generation_bump_insert", "outbox_generation_bump_update", "outbox_generation_bump_delete",
	}
	type definition struct {
		name string
		sql  string
	}
	definitions := make([]definition, 0, len(names))
	for _, name := range names {
		var ddl string
		if err := tx.QueryRowContext(context.Background(), `SELECT COALESCE(sql, '') FROM sqlite_master WHERE type = 'trigger' AND name = ?`, name).Scan(&ddl); err != nil {
			return fmt.Errorf("read outbox trigger %q: %w", name, err)
		}
		if strings.TrimSpace(ddl) == "" {
			return fmt.Errorf("outbox trigger %q has empty DDL", name)
		}
		definitions = append(definitions, definition{name: name, sql: ddl})
	}
	for _, trigger := range definitions {
		if _, err := tx.ExecContext(context.Background(), `DROP TRIGGER `+trigger.name); err != nil {
			return fmt.Errorf("drop outbox trigger %q: %w", trigger.name, err)
		}
	}
	operationErr := fn()
	var restoreErr error
	for _, trigger := range definitions {
		if _, err := tx.ExecContext(context.Background(), trigger.sql); err != nil {
			restoreErr = fmt.Errorf("restore outbox trigger %q: %w", trigger.name, err)
			break
		}
	}
	if operationErr != nil {
		if restoreErr != nil {
			return fmt.Errorf("seed outbox rows: %v; %w", operationErr, restoreErr)
		}
		return operationErr
	}
	return restoreErr
}

func TestSQLitePendingOutboxKeysetAcceptsZeroTimestampCursor(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	const chatID = "chat:zero-timestamp-cursor"
	seedLegacyStateFileForSQLiteMigrationTest(t, store)
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate zero-timestamp fixture: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		for i := 0; i < 64; i++ {
			id := fmt.Sprintf("outbox:zero-page-%03d", i)
			if err := insertSQLiteFIFOOutboxRowForTest(t, tx, id, chatID, int64(i+1), time.Time{}); err != nil {
				return err
			}
		}
		return insertSQLiteFIFOOutboxRowForTest(t, tx, "outbox:zero-page-999", chatID, 65, now)
	})
	if err := store.PrepareOutboxProjection(ctx); err != nil {
		t.Fatalf("prepare zero-timestamp projection: %v", err)
	}

	page, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now.Add(time.Hour), Limit: 1})
	if err != nil {
		t.Fatalf("pending page after zero-timestamp prefix: %v", err)
	}
	if len(page.Messages) != 1 || page.Messages[0].ID != "outbox:zero-page-000" {
		t.Fatalf("zero-timestamp page = %#v, want first zero-time row", page)
	}
	if !page.More || page.NextCursor.CreatedAt.IsZero() == false || page.NextCursor.ID == "" {
		// The cursor's zero CreatedAt is intentional; ID is the progress witness.
		t.Fatalf("zero-timestamp cursor = %#v, want zero time with non-empty ID and more=true", page.NextCursor)
	}

	page, err = store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now.Add(time.Hour), Limit: 1, After: page.NextCursor})
	if err != nil {
		t.Fatalf("pending page after zero-timestamp cursor: %v", err)
	}
	if len(page.Messages) != 1 || page.Messages[0].ID != "outbox:zero-page-001" {
		t.Fatalf("zero-timestamp second page = %#v, want next zero-time row", page)
	}
}

func TestPendingOutboxAdmissionExcludesUnboundAndPaddedDestinations(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 14, 13, 0, 0, 0, time.UTC)
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			store := newTestStore(t)
			if err := store.Update(ctx, func(state *State) error {
				state.OutboxMessages = map[string]OutboxMessage{
					"outbox:unbound": {
						ID: "outbox:unbound", Kind: "helper", Body: "not Graph work",
						Status: OutboxStatusQueued, CreatedAt: now, UpdatedAt: now,
					},
					"outbox:padded": {
						ID: "outbox:padded", TeamsChatID: " chat:padded ", Kind: "helper", Body: "repair first",
						Status: OutboxStatusQueued, CreatedAt: now.Add(time.Second), UpdatedAt: now,
					},
					"outbox:valid": {
						ID: "outbox:valid", TeamsChatID: "chat:valid", Kind: "helper", Body: "sendable",
						Status: OutboxStatusQueued, CreatedAt: now.Add(2 * time.Second), UpdatedAt: now,
					},
				}
				return nil
			}); err != nil {
				t.Fatalf("seed pending destination fixture: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}
			page, err := store.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now.Add(time.Minute), Limit: 10})
			if err != nil {
				t.Fatalf("PendingOutboxPageAt: %v", err)
			}
			if len(page.Messages) != 1 || page.Messages[0].ID != "outbox:valid" {
				t.Fatalf("pending destination page = %#v, want only outbox:valid", page.Messages)
			}
		})
	}
}

func TestSQLiteLegacyFIFOFallbackMatchesJSONChatBinding(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 14, 13, 30, 0, 0, time.UTC)
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite-legacy-fallback"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			if err := store.Update(ctx, func(state *State) error {
				state.OutboxMessages = map[string]OutboxMessage{
					"outbox:fifo-padded": {
						ID: "outbox:fifo-padded", TeamsChatID: " fifo:parity ", Kind: "helper", Body: "padded predecessor",
						Status: OutboxStatusQueued, Sequence: 1, CreatedAt: now, UpdatedAt: now,
					},
					"outbox:fifo-target": {
						ID: "outbox:fifo-target", TeamsChatID: "fifo:parity", Kind: "helper", Body: "target",
						Status: OutboxStatusQueued, Sequence: 2, CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
					},
				}
				return nil
			}); err != nil {
				t.Fatalf("seed FIFO parity fixture: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
				if err := store.revokeSQLiteOutboxProjectionTrust(ctx); err != nil {
					t.Fatalf("revoke native FIFO trust: %v", err)
				}
			}
			target, err := store.OutboxMessageByID(ctx, "outbox:fifo-target")
			if err != nil {
				t.Fatalf("load FIFO target: %v", err)
			}
			earliest, found, _, err := store.EarlierUnsentOutboxWithProof(ctx, target)
			if err != nil {
				t.Fatalf("FIFO predecessor lookup: %v", err)
			}
			if found || earliest.ID != "" {
				t.Fatalf("FIFO predecessor=%#v found=%v, want no predecessor for padded sibling chat", earliest, found)
			}
		})
	}
}

func TestSQLiteOutboxGenerationTriggersAreTransactional(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages["outbox:generation-seed"] = OutboxMessage{
			ID: "outbox:generation-seed", TeamsChatID: "chat:generation", Status: OutboxStatusSent,
			Sequence: 1, CreatedAt: time.Unix(1, 0).UTC(), UpdatedAt: time.Unix(1, 0).UTC(),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed generation store: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	base := sqliteOutboxGenerationForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		return insertSQLiteFIFOOutboxRowForTest(t, tx, "outbox:generation-insert", "chat:generation", 2, time.Unix(2, 0).UTC())
	})
	afterInsert := sqliteOutboxGenerationForTest(t, store)
	if afterInsert != base+1 {
		t.Fatalf("generation after committed insert = %d, want %d", afterInsert, base+1)
	}

	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET status = status WHERE id = ?`, "outbox:generation-insert")
		return err
	})
	afterUpdate := sqliteOutboxGenerationForTest(t, store)
	if afterUpdate != afterInsert+1 {
		t.Fatalf("generation after committed update = %d, want %d", afterUpdate, afterInsert+1)
	}

	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `SAVEPOINT generation_rollback`); err != nil {
			return err
		}
		if err := insertSQLiteFIFOOutboxRowForTest(t, tx, "outbox:generation-rollback", "chat:generation", 3, time.Unix(3, 0).UTC()); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `ROLLBACK TO generation_rollback`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `RELEASE generation_rollback`)
		return err
	})
	afterRollback := sqliteOutboxGenerationForTest(t, store)
	if afterRollback != afterUpdate {
		t.Fatalf("generation after rolled-back insert = %d, want unchanged %d", afterRollback, afterUpdate)
	}

	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM outbox_messages WHERE id = ?`, "outbox:generation-insert")
		return err
	})
	afterDelete := sqliteOutboxGenerationForTest(t, store)
	if afterDelete != afterRollback+1 {
		t.Fatalf("generation after committed delete = %d, want %d", afterDelete, afterRollback+1)
	}
}

func TestSQLiteOutboxGenerationSurvivesFullStateRewrite(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages["outbox:generation-rewrite"] = OutboxMessage{
			ID: "outbox:generation-rewrite", TeamsChatID: "chat:generation-rewrite", Status: OutboxStatusQueued,
			Sequence: 1, CreatedAt: time.Unix(10, 0).UTC(), UpdatedAt: time.Unix(10, 0).UTC(),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed generation rewrite store: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = ?`, "1000000", sqliteOutboxGenerationKey)
		return err
	})
	before := sqliteOutboxGenerationForTest(t, store)
	if before != 1000000 {
		t.Fatalf("seed generation = %d, want 1000000", before)
	}
	if err := store.Update(ctx, func(state *State) error {
		state.Scope.Profile = "generation-rewrite-proof"
		return nil
	}); err != nil {
		t.Fatalf("full state rewrite: %v", err)
	}
	after := sqliteOutboxGenerationForTest(t, store)
	if after <= before {
		t.Fatalf("generation after full rewrite = %d, want greater than %d", after, before)
	}
}

func TestSQLiteOutboxAuditUsesStrictNativeKeyContract(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		set  string
		arg  string
	}{
		{name: "blank-chat", set: "teams_chat_id", arg: ""},
		{name: "space-padded-id", set: "id", arg: " outbox:strict-audit "},
		{name: "blank-status", set: "status", arg: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestStore(t)
			row := OutboxMessage{
				ID: "outbox:strict-audit", TeamsChatID: "chat:strict-audit", Status: OutboxStatusQueued,
				Sequence: 1, CreatedAt: time.Unix(20, 0).UTC(), UpdatedAt: time.Unix(20, 0).UTC(),
			}
			if _, _, err := store.QueueOutbox(ctx, row); err != nil {
				t.Fatalf("seed strict audit row: %v", err)
			}
			migrateStoreToSQLiteForTest(t, store)
			withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
				if _, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET `+tc.set+` = ? WHERE id = ?`, tc.arg, row.ID); err != nil {
					return err
				}
				_, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = ?`, sqliteOutboxProjectionTrustUnknown, sqliteOutboxProjectionTrustKey)
				return err
			})
			if err := store.PrepareOutboxProjection(ctx); err != nil {
				t.Fatalf("strict audit preparation: %v", err)
			}
			var marker string
			withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
				return tx.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, sqliteOutboxProjectionTrustKey).Scan(&marker)
			})
			if marker != sqliteOutboxProjectionTrustUntrusted {
				t.Fatalf("strict audit marker = %q, want %q", marker, sqliteOutboxProjectionTrustUntrusted)
			}
		})
	}
}

func TestSQLiteOutboxGuardRevokesTrustedNativeLaneForBlankStatus(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	row := OutboxMessage{
		ID: "outbox:strict-guard-status", TeamsChatID: "chat:strict-guard-status",
		Status: OutboxStatusQueued, Sequence: 1, CreatedAt: time.Unix(30, 0).UTC(),
	}
	if _, _, err := store.QueueOutbox(ctx, row); err != nil {
		t.Fatalf("seed strict guard row: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	if err := store.PrepareOutboxProjection(ctx); err != nil {
		t.Fatalf("prepare strict guard projection: %v", err)
	}
	if marker := readOutboxProjectionTrustMarkersForTest(t, store)[sqliteOutboxProjectionTrustKey]; marker != sqliteOutboxProjectionTrustTrusted {
		t.Fatalf("initial projection marker = %q, want trusted", marker)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET status = '' WHERE id = ?`, row.ID)
		return err
	})
	if marker := readOutboxProjectionTrustMarkersForTest(t, store)[sqliteOutboxProjectionTrustKey]; marker != sqliteOutboxProjectionTrustUntrusted {
		t.Fatalf("marker after blank-status write = %q, want untrusted", marker)
	}
}

func TestSQLiteUntrustedOutboxFIFOFallbackIsBoundedAndFailClosed(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 12, 16, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages["outbox:fallback-later"] = OutboxMessage{
			ID: "outbox:fallback-later", TeamsChatID: "chat:fallback-budget", Status: OutboxStatusQueued,
			Sequence: sqliteOutboxFIFOLegacyMaxRows + 1, CreatedAt: now.Add(time.Duration(sqliteOutboxFIFOLegacyMaxRows+1) * time.Nanosecond), UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed fallback budget store: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = ?`, sqliteOutboxProjectionTrustDeferred, sqliteOutboxProjectionTrustKey); err != nil {
			return err
		}
		return insertSQLiteFIFOOutboxRowsForTest(t, tx, "chat:fallback-budget", 1, sqliteOutboxFIFOLegacyMaxRows+1, now)
	})
	later, err := store.OutboxMessageByID(ctx, "outbox:fallback-later")
	if err != nil {
		t.Fatalf("load fallback target: %v", err)
	}

	started := time.Now()
	predecessor, found, err := store.EarlierUnsentOutbox(ctx, later)
	elapsed := time.Since(started)
	if !errors.Is(err, ErrOutboxPredecessorIndeterminate) || found || predecessor.ID != "" {
		t.Fatalf("oversized fallback result = %#v found=%v err=%v, want fail-closed indeterminate", predecessor, found, err)
	}
	if elapsed > sqliteOutboxFIFOLegacyMaxDuration+2*time.Second {
		t.Fatalf("oversized fallback took %s, exceeded hard budget %s plus tolerance", elapsed, sqliteOutboxFIFOLegacyMaxDuration)
	}
	t.Logf("bounded untrusted FIFO fallback: %s for %d+ rows", elapsed, sqliteOutboxFIFOLegacyMaxRows)
}

func TestSQLiteUntrustedOutboxFIFOFallbackDoesNotHoldStateLock(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 12, 16, 5, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages["outbox:fallback-lock-earlier"] = OutboxMessage{
			ID: "outbox:fallback-lock-earlier", TeamsChatID: "chat:fallback-lock", Status: OutboxStatusQueued,
			Sequence: 1, CreatedAt: now, UpdatedAt: now,
		}
		state.OutboxMessages["outbox:fallback-lock-later"] = OutboxMessage{
			ID: "outbox:fallback-lock-later", TeamsChatID: "chat:fallback-lock", Status: OutboxStatusQueued,
			Sequence: 2, CreatedAt: now.Add(time.Second), UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed fallback lock store: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = ?`, sqliteOutboxProjectionTrustDeferred, sqliteOutboxProjectionTrustKey)
		return err
	})
	later, err := store.OutboxMessageByID(ctx, "outbox:fallback-lock-later")
	if err != nil {
		t.Fatalf("load fallback lock target: %v", err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	previousHook := sqliteOutboxFIFOFallbackTestHook
	sqliteOutboxFIFOFallbackTestHook = func(stage string) {
		if stage != "snapshot-open" {
			return
		}
		once.Do(func() { close(entered) })
		<-release
	}
	t.Cleanup(func() {
		sqliteOutboxFIFOFallbackTestHook = previousHook
		select {
		case <-release:
		default:
			close(release)
		}
	})

	lookupDone := make(chan error, 1)
	go func() {
		_, _, lookupErr := store.EarlierUnsentOutbox(ctx, later)
		lookupDone <- lookupErr
	}()
	select {
	case <-entered:
	case err := <-lookupDone:
		t.Fatalf("fallback lookup ended before independent snapshot hook: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("fallback lookup did not reach independent snapshot hook")
	}

	writeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- store.Update(writeCtx, func(state *State) error {
			state.ControlChat.TeamsChatID = "fallback-lock-control-update"
			return nil
		})
	}()
	select {
	case err := <-writeDone:
		cancel()
		if err != nil {
			t.Fatalf("durable writer blocked during fallback snapshot: %v", err)
		}
	case <-time.After(1500 * time.Millisecond):
		cancel()
		t.Fatal("durable writer did not progress while fallback snapshot was held")
	}
	close(release)
	select {
	case err := <-lookupDone:
		if err != nil {
			t.Fatalf("fallback lookup after concurrent writer: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fallback lookup did not finish after release")
	}
}

func TestSQLiteNativeFIFOProductionPathMatchesCanonicalFallbackAcrossPages(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	const chatID = "chat:fifo-page-parity"
	now := time.Date(2026, 9, 12, 16, 10, 0, 0, time.UTC)
	const rowCount = 260
	if err := store.Update(ctx, func(state *State) error {
		for i := 0; i < rowCount; i++ {
			id := fmt.Sprintf("outbox:fifo-page:%03d", i)
			at := now.Add(time.Duration(i/2) * time.Second)
			state.OutboxMessages[id] = OutboxMessage{
				ID: id, TeamsChatID: chatID, Status: OutboxStatusQueued,
				Sequence: int64(i/2 + 1), CreatedAt: at, UpdatedAt: at,
			}
		}
		state.OutboxMessages["outbox:fifo-page-target"] = OutboxMessage{
			ID: "outbox:fifo-page-target", TeamsChatID: chatID, Status: OutboxStatusQueued,
			Sequence: rowCount + 1, CreatedAt: now.Add(time.Hour), UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed FIFO page parity store: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	if err := store.PrepareOutboxProjection(ctx); err != nil {
		t.Fatalf("prepare FIFO page parity projection: %v", err)
	}
	target, err := store.OutboxMessageByID(ctx, "outbox:fifo-page-target")
	if err != nil {
		t.Fatalf("load FIFO page parity target: %v", err)
	}

	native, err := store.EarlierUnsentOutboxes(ctx, target)
	if err != nil || len(native) != rowCount {
		t.Fatalf("native FIFO page result len=%d err=%v, want %d", len(native), err, rowCount)
	}
	for i, got := range native {
		want := fmt.Sprintf("outbox:fifo-page:%03d", i)
		if got.ID != want {
			t.Fatalf("native FIFO page result[%d] = %q, want %q", i, got.ID, want)
		}
	}

	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = ?`, sqliteOutboxProjectionTrustDeferred, sqliteOutboxProjectionTrustKey)
		return err
	})
	legacy, err := store.EarlierUnsentOutboxes(ctx, target)
	if err != nil || len(legacy) != rowCount {
		t.Fatalf("canonical FIFO page result len=%d err=%v, want %d", len(legacy), err, rowCount)
	}
	for i, got := range legacy {
		want := fmt.Sprintf("outbox:fifo-page:%03d", i)
		if got.ID != want {
			t.Fatalf("canonical FIFO page result[%d] = %q, want %q", i, got.ID, want)
		}
	}
}

func TestSQLiteOutboxFIFOSnapshotProofIsOneShotAndTargetBound(t *testing.T) {
	target := OutboxMessage{ID: "outbox:proof", TeamsChatID: "chat:proof", Sequence: 2, CreatedAt: time.Unix(2, 0).UTC()}
	proof := newOutboxFIFOSnapshotProof(sqliteOutboxFIFOSnapshot{
		dbPath: "first", databaseIdentity: "db", marker: sqliteOutboxProjectionTrustTrusted, generation: 1,
	}, target)
	if proof.targetMatches(target) == false {
		t.Fatal("proof did not bind to its target row")
	}
	if proof.consumeForTarget("outbox:other") {
		t.Fatal("proof authorized a different target")
	}
	if proof.consumeForTarget(target.ID) {
		t.Fatal("mismatched proof became reusable for its original target")
	}

	proof = newOutboxFIFOSnapshotProof(sqliteOutboxFIFOSnapshot{
		dbPath: "second", databaseIdentity: "db", marker: sqliteOutboxProjectionTrustTrusted, generation: 2,
	}, target)
	if !proof.consumeForTarget(target.ID) {
		t.Fatal("fresh proof was not consumed by its target")
	}
	if proof.consumeForTarget(target.ID) {
		t.Fatal("one-shot proof was consumed twice")
	}
}

func TestSQLiteOutboxFIFOSnapshotProofCannotAuthorizeStaleResult(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 12, 16, 25, 0, 0, time.UTC)
	target := OutboxMessage{
		ID: "outbox:proof-target", TeamsChatID: "chat:proof-target", Status: OutboxStatusQueued,
		Sequence: 2, CreatedAt: now.Add(time.Second), UpdatedAt: now,
	}
	if _, _, err := store.QueueOutbox(ctx, target); err != nil {
		t.Fatalf("seed proof target: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	if err := store.PrepareOutboxProjection(ctx); err != nil {
		t.Fatalf("prepare proof target projection: %v", err)
	}
	target, err := store.OutboxMessageByID(ctx, target.ID)
	if err != nil {
		t.Fatalf("load proof target: %v", err)
	}
	_, found, oldProof, err := store.EarlierUnsentOutboxWithProof(ctx, target)
	if err != nil || found || oldProof == nil {
		t.Fatalf("initial proof lookup found=%v proof=%v err=%v, want empty result with proof", found, oldProof != nil, err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		return insertSQLiteFIFOOutboxRowForTest(t, tx, "outbox:proof-new-predecessor", target.TeamsChatID, 1, now)
	})
	if _, err := store.MarkOutboxSendAttemptWithFIFOSnapshotProof(ctx, target.ID, oldProof); !errors.Is(err, ErrOutboxPredecessorIndeterminate) {
		t.Fatalf("stale proof claim error = %v, want ErrOutboxPredecessorIndeterminate", err)
	}
	persisted, err := store.OutboxMessageByID(ctx, target.ID)
	if err != nil {
		t.Fatalf("reload proof target after stale claim: %v", err)
	}
	if persisted.Status != OutboxStatusQueued {
		t.Fatalf("stale proof changed target status to %q, want queued", persisted.Status)
	}
}

func TestSQLiteOutboxFIFOSnapshotClaimBarrierRejectsIndependentPredecessor(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 13, 2, 30, 0, 0, time.UTC)
	target := OutboxMessage{
		ID: "outbox:barrier-target", TeamsChatID: "chat:barrier", Status: OutboxStatusQueued,
		Sequence: 2, CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second), Body: "target",
	}
	if _, _, err := store.QueueOutbox(ctx, target); err != nil {
		t.Fatalf("seed barrier target: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	if err := store.PrepareOutboxProjection(ctx); err != nil {
		t.Fatalf("prepare barrier projection: %v", err)
	}
	target, err := store.OutboxMessageByID(ctx, target.ID)
	if err != nil {
		t.Fatalf("load barrier target: %v", err)
	}
	_, found, proof, err := store.EarlierUnsentOutboxWithProof(ctx, target)
	if err != nil || found || proof == nil || proof.jsonBackend {
		t.Fatalf("initial barrier proof found=%v proof=%v json=%v err=%v, want empty native proof", found, proof != nil, proof != nil && proof.jsonBackend, err)
	}

	peer, err := Open(store.Path())
	if err != nil {
		t.Fatalf("open independent store: %v", err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	previousHook := sqliteOutboxFIFOSnapshotClaimTestHook
	sqliteOutboxFIFOSnapshotClaimTestHook = func() {
		withSQLiteTxForTest(t, peer, func(tx *sql.Tx) error {
			return insertSQLiteFIFOOutboxRowForTest(t, tx, "outbox:barrier-predecessor", target.TeamsChatID, 1, now)
		})
	}
	t.Cleanup(func() { sqliteOutboxFIFOSnapshotClaimTestHook = previousHook })

	if _, err := store.MarkOutboxSendAttemptWithFIFOSnapshotProof(ctx, target.ID, proof); !errors.Is(err, ErrOutboxPredecessorIndeterminate) {
		t.Fatalf("claim after independent predecessor insert error = %v, want ErrOutboxPredecessorIndeterminate", err)
	}
	persisted, err := store.OutboxMessageByID(ctx, target.ID)
	if err != nil {
		t.Fatalf("reload barrier target: %v", err)
	}
	if persisted.Status != OutboxStatusQueued {
		t.Fatalf("claim barrier changed target status to %q, want queued", persisted.Status)
	}
}

func TestSQLiteOutboxProjectionAuditDoesNotPublishAcrossGenerationChange(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages["outbox:audit-generation"] = OutboxMessage{
			ID: "outbox:audit-generation", TeamsChatID: "chat:audit-generation", Status: OutboxStatusQueued,
			Sequence: 1, CreatedAt: time.Date(2026, 9, 12, 16, 20, 0, 0, time.UTC),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed audit generation store: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	previousHook := sqliteOutboxAuditTestHook
	sqliteOutboxAuditTestHook = func(stage string) {
		if stage != "opened" {
			return
		}
		withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET status = status WHERE id = ?`, "outbox:audit-generation")
			return err
		})
	}
	t.Cleanup(func() { sqliteOutboxAuditTestHook = previousHook })
	if err := store.PrepareOutboxProjection(ctx); err != nil {
		t.Fatalf("prepare generation-changing audit: %v", err)
	}
	markers := readOutboxProjectionTrustMarkersForTest(t, store)
	if markers[sqliteOutboxProjectionTrustKey] != sqliteOutboxProjectionTrustDeferred {
		t.Fatalf("outbox marker after generation-changing audit = %q, want deferred; all=%#v", markers[sqliteOutboxProjectionTrustKey], markers)
	}
}

func TestSQLiteDeferredOutboxProjectionAuditRetriesAfterGenerationRace(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 13, 16, 30, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages["outbox:audit-retry"] = OutboxMessage{
			ID: "outbox:audit-retry", TeamsChatID: "chat:audit-retry", Status: OutboxStatusQueued,
			Sequence: 1, CreatedAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed audit retry store: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		for _, key := range []string{
			sqliteOutboxProjectionTrustKey,
			sqliteOutboxSessionProjectionTrustKey,
			sqliteOutboxTurnProjectionTrustKey,
		} {
			if _, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = ?`, sqliteOutboxProjectionTrustDeferred, key); err != nil {
				return err
			}
		}
		return nil
	})

	var mutateOnce sync.Once
	previousHook := sqliteOutboxAuditTestHook
	sqliteOutboxAuditTestHook = func(stage string) {
		if stage != "opened" {
			return
		}
		mutateOnce.Do(func() {
			withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET status = status WHERE id = ?`, "outbox:audit-retry")
				return err
			})
		})
	}
	t.Cleanup(func() { sqliteOutboxAuditTestHook = previousHook })
	if err := store.RetryDeferredOutboxProjectionAudit(ctx); !errors.Is(err, ErrSQLiteOutboxProjectionAuditDeferred) {
		t.Fatalf("generation-raced deferred audit error = %v, want ErrSQLiteOutboxProjectionAuditDeferred", err)
	}
	markers := readOutboxProjectionTrustMarkersForTest(t, store)
	for key, marker := range markers {
		if marker != sqliteOutboxProjectionTrustDeferred {
			t.Fatalf("generation-raced marker %s = %q, want deferred; all=%#v", key, marker, markers)
		}
	}

	sqliteOutboxAuditTestHook = nil
	if err := store.RetryDeferredOutboxProjectionAudit(ctx); err != nil {
		t.Fatalf("retry deferred audit after writer settled: %v", err)
	}
	markers = readOutboxProjectionTrustMarkersForTest(t, store)
	for key, marker := range markers {
		if marker != sqliteOutboxProjectionTrustTrusted {
			t.Fatalf("retried marker %s = %q, want trusted; all=%#v", key, marker, markers)
		}
	}
}

func TestSQLiteOutboxFIFOFallbackDoesNotFilterDuplicateChatPredecessor(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, 9, 13, 17, 0, 0, 0, time.UTC)
	if err := store.Update(ctx, func(state *State) error {
		state.OutboxMessages["outbox:duplicate-chat-predecessor"] = OutboxMessage{
			ID: "outbox:duplicate-chat-predecessor", TeamsChatID: "chat:duplicate-chat-fifo",
			Status: OutboxStatusQueued, Sequence: 1, CreatedAt: now, UpdatedAt: now,
		}
		state.OutboxMessages["outbox:duplicate-chat-target"] = OutboxMessage{
			ID: "outbox:duplicate-chat-target", TeamsChatID: "chat:duplicate-chat-fifo",
			Status: OutboxStatusQueued, Sequence: 2, CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed duplicate-chat FIFO fixture: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	duplicate := `{"id":"outbox:duplicate-chat-predecessor","teams_chat_id":"other-chat","teams_chat_id":"chat:duplicate-chat-fifo","status":"queued","sequence":1,"created_at":"2026-09-13T17:00:00Z","body":"ambiguous predecessor"}`
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = ?, teams_chat_id = ? WHERE id = ?`, duplicate, "other-chat", "outbox:duplicate-chat-predecessor")
		return err
	})
	target, err := store.OutboxMessageByID(ctx, "outbox:duplicate-chat-target")
	if err != nil {
		t.Fatalf("load duplicate-chat FIFO target: %v", err)
	}
	_, found, proof, err := store.EarlierUnsentOutboxWithProof(ctx, target)
	if !errors.Is(err, ErrOutboxPredecessorIndeterminate) {
		t.Fatalf("duplicate-chat FIFO fallback error = %v, want ErrOutboxPredecessorIndeterminate", err)
	}
	if found || proof != nil {
		t.Fatalf("duplicate-chat FIFO fallback found=%v proof=%v, want fail-closed without proof", found, proof != nil)
	}
}
