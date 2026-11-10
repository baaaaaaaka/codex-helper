package delegation

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type WorkerStorePruneLimits struct {
	TerminalExecutionLimit int
	TerminalOutboxLimit    int
}

// Fixed-width UTC ordering preserves the original timestamp string and its
// nanosecond precision while allowing indexed retention and stable ordering.
func delegationTimeOrder(raw string) string {
	t := parseRecordTime(raw)
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02T15:04:05.000000000Z")
}

func (s *WorkerDB) TryStart(ctx context.Context, request, claim Record, now time.Time, limits WorkerStorePruneLimits) (bool, error) {
	started := false
	err := s.transaction(ctx, true, func(tx *sql.Tx) error {
		existing, ok, err := readExecutionFenceTx(ctx, tx, request.DelegationID)
		if err != nil {
			return err
		}
		if ok && ((existing.ClaimID == claim.ClaimID && existing.ClaimEpoch == claim.ClaimEpoch && existing.WorkerInstanceID == claim.WorkerInstanceID) || workerTerminalOrRunningStatus(existing.Status)) {
			return nil
		}
		thread, threadExists, err := readRemoteThreadTx(ctx, tx, request.RemoteThreadID)
		if err != nil {
			return err
		}
		if threadExists && thread.State == RemoteThreadStateActive && thread.ActiveDelegationID != "" && thread.ActiveDelegationID != strings.TrimSpace(request.DelegationID) {
			return nil
		}
		nowText := now.UTC().Format(time.RFC3339Nano)
		if _, err = putExecutionFenceTx(ctx, tx, ExecutionFence{DelegationID: request.DelegationID, ClaimID: claim.ClaimID, ClaimEpoch: claim.ClaimEpoch, WorkerInstanceID: claim.WorkerInstanceID, MachineID: claim.MachineID, Status: StateRunning, StartedAt: nowText, UpdatedAt: nowText}); err != nil {
			return err
		}
		next := workerActiveRemoteThread(request, now)
		if next.ThreadID != "" {
			if threadExists {
				next.SourceSessionID = strings.TrimSpace(firstNonEmptyString(thread.SourceSessionID, next.SourceSessionID))
				next.Generation = strings.TrimSpace(firstNonEmptyString(thread.Generation, next.Generation))
				next.Title = firstNonEmptyString(thread.Title, next.Title)
				next.CreatedAt = firstNonEmptyString(next.CreatedAt, thread.CreatedAt)
				next.ExpiresAt = firstNonEmptyString(next.ExpiresAt, thread.ExpiresAt)
			}
			if _, err = putRemoteThreadTx(ctx, tx, next); err != nil {
				return err
			}
		}
		if err = pruneWorkerSQLiteStateTx(ctx, tx, limits); err != nil {
			return err
		}
		started = true
		return nil
	})
	return started, err
}

// The writer lock precedes the read and the Graph POST follows the commit.
// SQLite serializes this decision across processes; no blob comparison or
// second application lock is needed to prevent an ABA replacement.
func (s *WorkerDB) ReserveOutbox(ctx context.Context, record Record, chatID string, now time.Time) (OutboxRecord, bool, error) {
	var out OutboxRecord
	reserved := false
	if strings.TrimSpace(record.RecordID) == "" {
		return out, false, fmt.Errorf("delegation record id is required")
	}
	err := s.transaction(ctx, true, func(tx *sql.Tx) error {
		existing, ok, err := readOutboxRecordTx(ctx, tx, record.RecordID)
		if err != nil {
			return err
		}
		out = existing
		if ok && outboxHasDeliveryWitness(existing) {
			return nil
		}
		out = nextWorkerOutbox(existing, record, OutboxPending, chatID, "", "", now)
		if _, err = putOutboxRecordTx(ctx, tx, out); err != nil {
			return err
		}
		reserved = true
		return nil
	})
	return out, reserved, err
}

func (s *WorkerDB) UpsertOutbox(ctx context.Context, record Record, status, chatID, inboxRef, messageID, errText string, now time.Time, retention time.Duration) error {
	return s.transaction(ctx, true, func(tx *sql.Tx) error {
		existing, _, err := readOutboxRecordTx(ctx, tx, record.RecordID)
		if err != nil {
			return err
		}
		record.InboxRef = strings.TrimSpace(inboxRef)
		next := nextWorkerOutbox(existing, record, status, chatID, messageID, errText, now)
		if _, err = putOutboxRecordTx(ctx, tx, next); err != nil {
			return err
		}
		return pruneOutboxSQLiteByRetentionTx(ctx, tx, now, retention)
	})
}

func pruneWorkerSQLiteStateTx(ctx context.Context, tx *sql.Tx, limits WorkerStorePruneLimits) error {
	var routesExist bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM routes)`).Scan(&routesExist); err != nil {
		return err
	}
	if routesExist {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM remote_threads WHERE state<>'active'`); err != nil {
		return err
	}
	if limits.TerminalExecutionLimit > 0 {
		if _, err := tx.ExecContext(ctx, `DELETE FROM executions WHERE id IN (SELECT id FROM executions WHERE status IN ('complete','blocked','canceled','expired','conflict','reuse_rejected') ORDER BY updated_order DESC,id LIMIT -1 OFFSET ?)`, limits.TerminalExecutionLimit); err != nil {
			return err
		}
	}
	if limits.TerminalOutboxLimit > 0 {
		if _, err := tx.ExecContext(ctx, `DELETE FROM outbox WHERE id IN (SELECT id FROM outbox WHERE status IN ('visible','failed') ORDER BY updated_order DESC,id LIMIT -1 OFFSET ?)`, limits.TerminalOutboxLimit); err != nil {
			return err
		}
	}
	return nil
}

func pruneOutboxSQLiteByRetentionTx(ctx context.Context, tx *sql.Tx, now time.Time, retention time.Duration) error {
	if retention <= 0 {
		retention = DefaultStoreRetention
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM outbox WHERE status IN ('visible','failed') AND updated_order>'' AND updated_order<?`, delegationTimeOrder(now.UTC().Add(-retention).Format(time.RFC3339Nano)))
	return err
}

func workerTerminalOrRunningStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case StateRunning, StateComplete, StateBlocked, StateCanceled, StateReuseRejected:
		return true
	default:
		return false
	}
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

func (s *WorkerDB) Route(ctx context.Context, id string) (Route, bool, error) {
	var v Route
	var ok bool
	err := s.transaction(ctx, false, func(tx *sql.Tx) error { var err error; v, ok, err = readRouteTx(ctx, tx, id); return err })
	return v, ok, err
}

func (s *WorkerDB) UpsertRoute(ctx context.Context, v Route) error {
	return s.transaction(ctx, true, func(tx *sql.Tx) error {
		existing, ok, err := readRouteTx(ctx, tx, v.DelegationID)
		if err != nil {
			return err
		}
		if ok && strings.TrimSpace(v.CreatedAt) == "" {
			v.CreatedAt = existing.CreatedAt
		}
		if strings.TrimSpace(v.CreatedAt) == "" {
			v.CreatedAt = v.UpdatedAt
		}
		_, err = putRouteTx(ctx, tx, v)
		return err
	})
}

func (s *WorkerDB) RemoteThread(ctx context.Context, id string) (RemoteThread, bool, error) {
	var v RemoteThread
	var ok bool
	err := s.transaction(ctx, false, func(tx *sql.Tx) error { var err error; v, ok, err = readRemoteThreadTx(ctx, tx, id); return err })
	return v, ok, err
}

func (s *WorkerDB) UpsertRemoteThread(ctx context.Context, v RemoteThread) error {
	return s.transaction(ctx, true, func(tx *sql.Tx) error {
		existing, _, err := readRemoteThreadTx(ctx, tx, v.ThreadID)
		if err != nil {
			return err
		}
		v = normalizeRemoteThread(v, existing)
		_, err = putRemoteThreadTx(ctx, tx, v)
		return err
	})
}

func (s *WorkerDB) Execution(ctx context.Context, id string) (ExecutionFence, bool, error) {
	var v ExecutionFence
	var ok bool
	err := s.transaction(ctx, false, func(tx *sql.Tx) error { var err error; v, ok, err = readExecutionFenceTx(ctx, tx, id); return err })
	return v, ok, err
}

func (s *WorkerDB) Outbox(ctx context.Context, id string) (OutboxRecord, bool, error) {
	var v OutboxRecord
	var ok bool
	err := s.transaction(ctx, false, func(tx *sql.Tx) error { var err error; v, ok, err = readOutboxRecordTx(ctx, tx, id); return err })
	return v, ok, err
}

func (s *WorkerDB) InboxCursor(ctx context.Context, id string) (InboxCursor, bool, error) {
	var v InboxCursor
	var ok bool
	err := s.transaction(ctx, false, func(tx *sql.Tx) error { var err error; v, ok, err = readInboxCursorTx(ctx, tx, id); return err })
	return v, ok, err
}

func (s *WorkerDB) UpsertInboxCursor(ctx context.Context, v InboxCursor) error {
	return s.transaction(ctx, true, func(tx *sql.Tx) error {
		_, err := putInboxCursorTx(ctx, tx, v)
		return err
	})
}

func (s *WorkerDB) InboxBackoff(ctx context.Context, id string) (InboxBackoff, bool, error) {
	var v InboxBackoff
	var ok bool
	err := s.transaction(ctx, false, func(tx *sql.Tx) error { var err error; v, ok, err = readInboxBackoffTx(ctx, tx, id); return err })
	return v, ok, err
}

func (s *WorkerDB) UpsertInboxBackoff(ctx context.Context, v InboxBackoff) error {
	return s.transaction(ctx, true, func(tx *sql.Tx) error {
		_, err := putInboxBackoffTx(ctx, tx, v)
		return err
	})
}

func TryStartWorkerSQLite(path string, request, claim Record, now time.Time, limits WorkerStorePruneLimits) (bool, error) {
	s, err := OpenWorkerDB(path)
	if err != nil {
		return false, err
	}
	defer s.Close()
	return s.TryStart(context.Background(), request, claim, now, limits)
}
func reserveOutboxSQLite(ctx context.Context, path string, record Record, chatID string, now time.Time) (OutboxRecord, bool, error) {
	s, err := OpenWorkerDB(path)
	if err != nil {
		return OutboxRecord{}, false, err
	}
	defer s.Close()
	return s.ReserveOutbox(ctx, record, chatID, now)
}
func UpsertOutboxSQLite(path string, record Record, status, chatID, inboxRef, messageID, errText string, now time.Time, retention time.Duration) error {
	s, err := OpenWorkerDB(path)
	if err != nil {
		return err
	}
	defer s.Close()
	return s.UpsertOutbox(context.Background(), record, status, chatID, inboxRef, messageID, errText, now, retention)
}

func RouteSQLite(path, id string) (Route, bool, error) {
	s, err := OpenWorkerDB(path)
	if err != nil {
		return Route{}, false, err
	}
	defer s.Close()
	return s.Route(context.Background(), id)
}
func UpsertRouteSQLite(path string, v Route) error {
	s, err := OpenWorkerDB(path)
	if err != nil {
		return err
	}
	defer s.Close()
	return s.UpsertRoute(context.Background(), v)
}

func RemoteThreadSQLite(path, id string) (RemoteThread, bool, error) {
	s, err := OpenWorkerDB(path)
	if err != nil {
		return RemoteThread{}, false, err
	}
	defer s.Close()
	return s.RemoteThread(context.Background(), id)
}
func UpsertRemoteThreadSQLite(path string, v RemoteThread) error {
	s, err := OpenWorkerDB(path)
	if err != nil {
		return err
	}
	defer s.Close()
	return s.UpsertRemoteThread(context.Background(), v)
}
