package delegation

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

type WorkerStorePruneLimits struct {
	TerminalExecutionLimit int
	TerminalOutboxLimit    int
}

func StorePathUsesSQLite(path string) bool {
	return storePathUsesSQLite(path)
}

func TryStartWorkerSQLite(path string, request Record, claim Record, now time.Time, limits WorkerStorePruneLimits) (bool, error) {
	if !storePathUsesSQLite(path) {
		return false, fmt.Errorf("delegation store %q is not sqlite", path)
	}
	if err := materializeSQLiteStoreFromLegacy(path); err != nil {
		return false, err
	}
	db, err := openDelegationSQLiteStore(path, true)
	if err != nil {
		return false, err
	}
	defer db.Close()
	ctx := context.Background()
	if err := ensureDelegationSQLiteSchema(ctx, db); err != nil {
		return false, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if existing, ok, err := sqliteExecutionForIDTx(ctx, tx, request.DelegationID); err != nil {
		return false, err
	} else if ok {
		if existing.ClaimID == claim.ClaimID && existing.ClaimEpoch == claim.ClaimEpoch && existing.WorkerInstanceID == claim.WorkerInstanceID {
			return false, nil
		}
		if workerTerminalOrRunningStatus(existing.Status) {
			return false, nil
		}
	}
	if threadID := strings.TrimSpace(request.RemoteThreadID); threadID != "" {
		if thread, ok, err := sqliteRemoteThreadForIDTx(ctx, tx, threadID); err != nil {
			return false, err
		} else if ok {
			activeID := strings.TrimSpace(thread.ActiveDelegationID)
			if thread.State == RemoteThreadStateActive && activeID != "" && activeID != strings.TrimSpace(request.DelegationID) {
				return false, nil
			}
		}
	}
	now = now.UTC()
	nowText := now.Format(time.RFC3339Nano)
	if err := sqliteUpsertJSONTx(ctx, tx, "executions", strings.TrimSpace(request.DelegationID), ExecutionFence{
		DelegationID:     request.DelegationID,
		ClaimID:          claim.ClaimID,
		ClaimEpoch:       claim.ClaimEpoch,
		WorkerInstanceID: claim.WorkerInstanceID,
		MachineID:        claim.MachineID,
		Status:           StateRunning,
		StartedAt:        nowText,
		UpdatedAt:        nowText,
	}); err != nil {
		return false, err
	}
	if thread := workerActiveRemoteThread(request, now); strings.TrimSpace(thread.ThreadID) != "" {
		if existing, ok, err := sqliteRemoteThreadForIDTx(ctx, tx, thread.ThreadID); err != nil {
			return false, err
		} else if ok {
			thread.SourceSessionID = strings.TrimSpace(firstNonEmptyString(existing.SourceSessionID, thread.SourceSessionID))
			thread.Generation = strings.TrimSpace(firstNonEmptyString(existing.Generation, thread.Generation))
			thread.Title = firstNonEmptyString(existing.Title, thread.Title)
			if strings.TrimSpace(thread.CreatedAt) == "" {
				thread.CreatedAt = existing.CreatedAt
			}
			if strings.TrimSpace(thread.ExpiresAt) == "" {
				thread.ExpiresAt = existing.ExpiresAt
			}
		}
		if err := sqliteUpsertJSONTx(ctx, tx, "remote_threads", thread.ThreadID, thread); err != nil {
			return false, err
		}
	}
	if err := pruneWorkerSQLiteStateTx(ctx, tx, limits); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	chmodDelegationSQLiteFiles(path)
	return true, nil
}

func FinishWorkerSQLite(path string, request Record, claim Record, status string, now time.Time, limits WorkerStorePruneLimits) error {
	if !storePathUsesSQLite(path) {
		return fmt.Errorf("delegation store %q is not sqlite", path)
	}
	if err := materializeSQLiteStoreFromLegacy(path); err != nil {
		return err
	}
	db, err := openDelegationSQLiteStore(path, true)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx := context.Background()
	if err := ensureDelegationSQLiteSchema(ctx, db); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now = now.UTC()
	nowText := now.Format(time.RFC3339Nano)
	execution := ExecutionFence{
		DelegationID:     request.DelegationID,
		ClaimID:          claim.ClaimID,
		ClaimEpoch:       claim.ClaimEpoch,
		WorkerInstanceID: claim.WorkerInstanceID,
		MachineID:        claim.MachineID,
		Status:           status,
		UpdatedAt:        nowText,
	}
	if existing, ok, err := sqliteExecutionForIDTx(ctx, tx, request.DelegationID); err != nil {
		return err
	} else if ok {
		execution.StartedAt = existing.StartedAt
	}
	if err := sqliteUpsertJSONTx(ctx, tx, "executions", strings.TrimSpace(request.DelegationID), execution); err != nil {
		return err
	}
	if threadID := strings.TrimSpace(request.RemoteThreadID); threadID != "" {
		routesExist, err := sqliteRoutesExistTx(ctx, tx)
		if err != nil {
			return err
		}
		if routesExist {
			thread := workerIdleRemoteThread(request, now)
			if existing, ok, err := sqliteRemoteThreadForIDTx(ctx, tx, threadID); err != nil {
				return err
			} else if ok {
				thread.SourceSessionID = strings.TrimSpace(firstNonEmptyString(existing.SourceSessionID, thread.SourceSessionID))
				thread.Generation = strings.TrimSpace(firstNonEmptyString(existing.Generation, thread.Generation))
				thread.Title = firstNonEmptyString(existing.Title, thread.Title)
				thread.CreatedAt = firstNonEmptyString(existing.CreatedAt, thread.CreatedAt)
				thread.ExpiresAt = firstNonEmptyString(existing.ExpiresAt, thread.ExpiresAt)
			}
			if err := sqliteUpsertJSONTx(ctx, tx, "remote_threads", thread.ThreadID, thread); err != nil {
				return err
			}
		} else if err := sqliteDeleteTx(ctx, tx, "remote_threads", threadID); err != nil {
			return err
		}
	}
	if err := pruneWorkerSQLiteStateTx(ctx, tx, limits); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	chmodDelegationSQLiteFiles(path)
	return nil
}

func UpsertWorkerOutboxSQLite(path string, record Record, status string, chatID string, messageID string, errText string, now time.Time, limits WorkerStorePruneLimits) error {
	if !storePathUsesSQLite(path) {
		return fmt.Errorf("delegation store %q is not sqlite", path)
	}
	if err := materializeSQLiteStoreFromLegacy(path); err != nil {
		return err
	}
	db, err := openDelegationSQLiteStore(path, true)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx := context.Background()
	if err := ensureDelegationSQLiteSchema(ctx, db); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := upsertOutboxSQLiteTx(ctx, tx, record, status, chatID, record.InboxRef, messageID, errText, now); err != nil {
		return err
	}
	if err := pruneWorkerSQLiteStateTx(ctx, tx, limits); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	chmodDelegationSQLiteFiles(path)
	return nil
}

func UpsertOutboxSQLite(path string, record Record, status string, chatID string, inboxRef string, messageID string, errText string, now time.Time, retention time.Duration) error {
	if !storePathUsesSQLite(path) {
		return fmt.Errorf("delegation store %q is not sqlite", path)
	}
	if err := materializeSQLiteStoreFromLegacy(path); err != nil {
		return err
	}
	db, err := openDelegationSQLiteStore(path, true)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx := context.Background()
	if err := ensureDelegationSQLiteSchema(ctx, db); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := upsertOutboxSQLiteTx(ctx, tx, record, status, chatID, inboxRef, messageID, errText, now); err != nil {
		return err
	}
	if err := pruneOutboxSQLiteByRetentionTx(ctx, tx, now.UTC(), retention); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	chmodDelegationSQLiteFiles(path)
	return nil
}

func InboxCursorSQLite(path string, chatID string) (InboxCursor, bool, error) {
	return sqliteLoadByID[InboxCursor](path, "inbox_cursors", chatID)
}

func UpsertInboxCursorSQLite(path string, cursor InboxCursor) error {
	return sqliteUpsertSingle(path, "inbox_cursors", cursor.ChatID, cursor)
}

func InboxBackoffSQLite(path string, chatID string) (InboxBackoff, bool, error) {
	return sqliteLoadByID[InboxBackoff](path, "inbox_backoffs", chatID)
}

func UpsertInboxBackoffSQLite(path string, backoff InboxBackoff) error {
	return sqliteUpsertSingle(path, "inbox_backoffs", backoff.ChatID, backoff)
}

func RouteSQLite(path string, delegationID string) (Route, bool, error) {
	return sqliteLoadByID[Route](path, "routes", delegationID)
}

func UpsertRouteSQLite(path string, route Route) error {
	if strings.TrimSpace(route.DelegationID) == "" {
		return nil
	}
	if existing, ok, err := RouteSQLite(path, route.DelegationID); err != nil {
		return err
	} else if ok && strings.TrimSpace(route.CreatedAt) == "" {
		route.CreatedAt = existing.CreatedAt
	}
	tmp := Store{}
	tmp.UpsertRoute(route)
	route, _ = tmp.RouteForID(route.DelegationID)
	return sqliteUpsertSingle(path, "routes", route.DelegationID, route)
}

func RemoteThreadSQLite(path string, threadID string) (RemoteThread, bool, error) {
	return sqliteLoadByID[RemoteThread](path, "remote_threads", threadID)
}

func UpsertRemoteThreadSQLite(path string, thread RemoteThread) error {
	if strings.TrimSpace(thread.ThreadID) == "" {
		return nil
	}
	tmp := Store{}
	if existing, ok, err := RemoteThreadSQLite(path, thread.ThreadID); err != nil {
		return err
	} else if ok {
		tmp.UpsertRemoteThread(existing)
	}
	tmp.UpsertRemoteThread(thread)
	thread, _ = tmp.RemoteThreadForID(thread.ThreadID)
	return sqliteUpsertSingle(path, "remote_threads", thread.ThreadID, thread)
}

func UpsertRecordSQLite(path string, record Record) error {
	if strings.TrimSpace(record.RecordID) == "" {
		return nil
	}
	return sqliteUpsertSingle(path, "records", record.RecordID, record)
}

func sqliteLoadByID[T any](path string, table string, id string) (T, bool, error) {
	var zero T
	if !storePathUsesSQLite(path) {
		return zero, false, fmt.Errorf("delegation store %q is not sqlite", path)
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return zero, false, nil
	}
	if err := materializeSQLiteStoreFromLegacy(path); err != nil {
		return zero, false, err
	}
	if exists, err := osStatSQLite(path); err != nil {
		return zero, false, err
	} else if !exists {
		return zero, false, nil
	}
	db, err := openDelegationSQLiteStore(path, true)
	if err != nil {
		return zero, false, err
	}
	defer db.Close()
	ctx := context.Background()
	if err := ensureDelegationSQLiteSchema(ctx, db); err != nil {
		return zero, false, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return zero, false, err
	}
	defer tx.Rollback()
	value, ok, err := sqliteJSONForIDTx[T](ctx, tx, table, id)
	if err != nil {
		return zero, false, err
	}
	if err := tx.Commit(); err != nil {
		return zero, false, err
	}
	return value, ok, nil
}

func sqliteUpsertSingle[T any](path string, table string, id string, value T) error {
	if !storePathUsesSQLite(path) {
		return fmt.Errorf("delegation store %q is not sqlite", path)
	}
	if err := materializeSQLiteStoreFromLegacy(path); err != nil {
		return err
	}
	db, err := openDelegationSQLiteStore(path, true)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx := context.Background()
	if err := ensureDelegationSQLiteSchema(ctx, db); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := sqliteUpsertJSONTx(ctx, tx, table, id, value); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	chmodDelegationSQLiteFiles(path)
	return nil
}

func workerActiveRemoteThread(request Record, now time.Time) RemoteThread {
	thread := workerIdleRemoteThread(request, now)
	thread.State = RemoteThreadStateActive
	thread.ActiveDelegationID = strings.TrimSpace(request.DelegationID)
	thread.LastTerminalRecordID = ""
	return thread
}

func workerIdleRemoteThread(request Record, now time.Time) RemoteThread {
	if strings.TrimSpace(request.RemoteThreadID) == "" {
		return RemoteThread{}
	}
	now = now.UTC()
	nowText := now.Format(time.RFC3339Nano)
	return RemoteThread{
		ThreadID:           strings.TrimSpace(request.RemoteThreadID),
		MachineID:          strings.TrimSpace(request.MachineID),
		SourceSessionID:    strings.TrimSpace(request.SourceSessionID),
		State:              RemoteThreadStateIdle,
		Generation:         strings.TrimSpace(firstNonEmptyString(request.ThreadGeneration, NewThreadGeneration(request.RemoteThreadID, now))),
		Title:              firstNonEmptyString(request.Spec.Title, request.Spec.Objective),
		UpdatedAt:          nowText,
		LastUsedAt:         nowText,
		CreatedAt:          strings.TrimSpace(firstNonEmptyString(request.CreatedAt, nowText)),
		ExpiresAt:          now.Add(DefaultStoreRetention).Format(time.RFC3339Nano),
		ActiveDelegationID: "",
	}
}

func sqliteExecutionForIDTx(ctx context.Context, tx *sql.Tx, id string) (ExecutionFence, bool, error) {
	return sqliteJSONForIDTx[ExecutionFence](ctx, tx, "executions", id)
}

func sqliteRemoteThreadForIDTx(ctx context.Context, tx *sql.Tx, id string) (RemoteThread, bool, error) {
	return sqliteJSONForIDTx[RemoteThread](ctx, tx, "remote_threads", id)
}

func sqliteOutboxForIDTx(ctx context.Context, tx *sql.Tx, id string) (OutboxRecord, bool, error) {
	return sqliteJSONForIDTx[OutboxRecord](ctx, tx, "outbox", id)
}

func upsertOutboxSQLiteTx(ctx context.Context, tx *sql.Tx, record Record, status string, chatID string, inboxRef string, messageID string, errText string, now time.Time) error {
	nowText := now.UTC().Format(time.RFC3339Nano)
	existing, _, err := sqliteOutboxForIDTx(ctx, tx, record.RecordID)
	if err != nil {
		return err
	}
	attempts := existing.Attempts
	if status == OutboxPending {
		attempts++
	}
	createdAt := existing.CreatedAt
	if strings.TrimSpace(createdAt) == "" {
		createdAt = nowText
	}
	if strings.TrimSpace(messageID) == "" {
		messageID = existing.MessageID
	}
	next := OutboxRecord{
		RecordID:     record.RecordID,
		DelegationID: record.DelegationID,
		ChatID:       strings.TrimSpace(chatID),
		InboxRef:     strings.TrimSpace(inboxRef),
		Status:       strings.TrimSpace(status),
		MessageID:    strings.TrimSpace(messageID),
		Attempts:     attempts,
		Error:        strings.TrimSpace(errText),
		CreatedAt:    createdAt,
		UpdatedAt:    nowText,
	}
	return sqliteUpsertJSONTx(ctx, tx, "outbox", next.RecordID, next)
}

func sqliteJSONForIDTx[T any](ctx context.Context, tx *sql.Tx, table string, id string) (T, bool, error) {
	var zero T
	id = strings.TrimSpace(id)
	if id == "" {
		return zero, false, nil
	}
	var raw []byte
	err := tx.QueryRowContext(ctx, "SELECT json FROM "+table+" WHERE id = ?", id).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, err
	}
	var value T
	if err := json.Unmarshal(raw, &value); err != nil {
		return zero, false, err
	}
	return value, true, nil
}

func sqliteUpsertJSONTx[T any](ctx context.Context, tx *sql.Tx, table string, id string, value T) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	var existing []byte
	err = tx.QueryRowContext(ctx, "SELECT json FROM "+table+" WHERE id = ?", id).Scan(&existing)
	if err == nil && string(existing) == string(raw) {
		return nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO "+table+"(id, json) VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET json = excluded.json", id, raw)
	return err
}

func sqliteDeleteTx(ctx context.Context, tx *sql.Tx, table string, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil
	}
	_, err := tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE id = ?", id)
	return err
}

func sqliteRoutesExistTx(ctx context.Context, tx *sql.Tx) (bool, error) {
	var id string
	err := tx.QueryRowContext(ctx, `SELECT id FROM routes LIMIT 1`).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func pruneWorkerSQLiteStateTx(ctx context.Context, tx *sql.Tx, limits WorkerStorePruneLimits) error {
	routesExist, err := sqliteRoutesExistTx(ctx, tx)
	if err != nil || routesExist {
		return err
	}
	if err := pruneWorkerSQLiteIdleRemoteThreadsTx(ctx, tx); err != nil {
		return err
	}
	if err := pruneWorkerSQLiteExecutionsTx(ctx, tx, limits.TerminalExecutionLimit); err != nil {
		return err
	}
	return pruneWorkerSQLiteOutboxTx(ctx, tx, limits.TerminalOutboxLimit)
}

func pruneWorkerSQLiteIdleRemoteThreadsTx(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT id, json FROM remote_threads`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var deleteIDs []string
	for rows.Next() {
		var id string
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			return err
		}
		var thread RemoteThread
		if err := json.Unmarshal(raw, &thread); err != nil {
			return err
		}
		if strings.TrimSpace(thread.State) != RemoteThreadStateActive {
			deleteIDs = append(deleteIDs, id)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range deleteIDs {
		if err := sqliteDeleteTx(ctx, tx, "remote_threads", id); err != nil {
			return err
		}
	}
	return nil
}

func pruneWorkerSQLiteExecutionsTx(ctx context.Context, tx *sql.Tx, limit int) error {
	if limit <= 0 {
		return nil
	}
	entries, err := workerSQLitePruneEntries(ctx, tx, "executions", func(raw []byte) (time.Time, bool, error) {
		var execution ExecutionFence
		if err := json.Unmarshal(raw, &execution); err != nil {
			return time.Time{}, false, err
		}
		if !terminalExecutionStatus(execution.Status) {
			return time.Time{}, false, nil
		}
		return parseRecordTime(execution.UpdatedAt), true, nil
	})
	if err != nil {
		return err
	}
	return pruneWorkerSQLiteEntries(ctx, tx, "executions", entries, limit)
}

func pruneWorkerSQLiteOutboxTx(ctx context.Context, tx *sql.Tx, limit int) error {
	if limit <= 0 {
		return nil
	}
	entries, err := workerSQLitePruneEntries(ctx, tx, "outbox", func(raw []byte) (time.Time, bool, error) {
		var outbox OutboxRecord
		if err := json.Unmarshal(raw, &outbox); err != nil {
			return time.Time{}, false, err
		}
		if outbox.Status != OutboxVisible && outbox.Status != OutboxFailed {
			return time.Time{}, false, nil
		}
		return parseRecordTime(outbox.UpdatedAt), true, nil
	})
	if err != nil {
		return err
	}
	return pruneWorkerSQLiteEntries(ctx, tx, "outbox", entries, limit)
}

func pruneOutboxSQLiteByRetentionTx(ctx context.Context, tx *sql.Tx, now time.Time, retention time.Duration) error {
	if retention <= 0 {
		retention = DefaultStoreRetention
	}
	cutoff := now.UTC().Add(-retention)
	rows, err := tx.QueryContext(ctx, `SELECT id, json FROM outbox`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var deleteIDs []string
	for rows.Next() {
		var id string
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			return err
		}
		var outbox OutboxRecord
		if err := json.Unmarshal(raw, &outbox); err != nil {
			return err
		}
		if pruneOutboxRecord(outbox, cutoff) {
			deleteIDs = append(deleteIDs, id)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range deleteIDs {
		if err := sqliteDeleteTx(ctx, tx, "outbox", id); err != nil {
			return err
		}
	}
	return nil
}

type workerSQLitePruneEntry struct {
	id string
	at time.Time
}

func workerSQLitePruneEntries(ctx context.Context, tx *sql.Tx, table string, include func([]byte) (time.Time, bool, error)) ([]workerSQLitePruneEntry, error) {
	rows, err := tx.QueryContext(ctx, "SELECT id, json FROM "+table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []workerSQLitePruneEntry
	for rows.Next() {
		var id string
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, err
		}
		at, ok, err := include(raw)
		if err != nil {
			return nil, err
		}
		if ok {
			entries = append(entries, workerSQLitePruneEntry{id: id, at: at})
		}
	}
	return entries, rows.Err()
}

func pruneWorkerSQLiteEntries(ctx context.Context, tx *sql.Tx, table string, entries []workerSQLitePruneEntry, limit int) error {
	if len(entries) <= limit {
		return nil
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].at.After(entries[j].at)
	})
	for _, entry := range entries[limit:] {
		if err := sqliteDeleteTx(ctx, tx, table, entry.id); err != nil {
			return err
		}
	}
	return nil
}

func workerTerminalOrRunningStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case StateRunning, StateComplete, StateBlocked, StateCanceled, StateReuseRejected:
		return true
	default:
		return false
	}
}

func osStatSQLite(path string) (bool, error) {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
