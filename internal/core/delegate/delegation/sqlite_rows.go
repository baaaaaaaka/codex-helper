package delegation

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
)

type delegationScanner interface{ Scan(...any) error }

const recordsColumns = `id,kind,delegation_id,source_key,source_session_id,source_turn_id,parent_id,hop_budget,machine_id,inbox_ref,inbox_generation,remote_thread_id,thread_policy,thread_generation,thread_summary_hash,claim_id,claim_epoch,worker_instance_id,payload_hash,result_sequence,status,reason,body,spec_hash,created_at,expires_at,spec_title,spec_objective,spec_timeout_seconds,has_thread_update,update_title,update_summary,update_summary_delta,update_last_result_summary,update_next_step_hint`

func scanRecord(row delegationScanner) (Record, error) {
	var v Record
	var hasThreadUpdate bool
	var update ThreadUpdate
	err := row.Scan(&v.RecordID, &v.Kind, &v.DelegationID, &v.SourceKey, &v.SourceSessionID, &v.SourceTurnID, &v.ParentID, &v.HopBudget, &v.MachineID, &v.InboxRef, &v.InboxGeneration, &v.RemoteThreadID, &v.ThreadPolicy, &v.ThreadGeneration, &v.ThreadSummaryHash, &v.ClaimID, &v.ClaimEpoch, &v.WorkerInstanceID, &v.PayloadHash, &v.ResultSequence, &v.Status, &v.Reason, &v.Body, &v.SpecHash, &v.CreatedAt, &v.ExpiresAt, &v.Spec.Title, &v.Spec.Objective, &v.Spec.TimeoutSeconds, &hasThreadUpdate, &update.Title, &update.Summary, &update.SummaryDelta, &update.LastResultSummary, &update.NextStepHint)
	if hasThreadUpdate {
		v.ThreadUpdate = &update
	}
	return v, err
}

func readRecordTx(ctx context.Context, tx *sql.Tx, id string) (Record, bool, error) {
	v, err := scanRecord(tx.QueryRowContext(ctx, `SELECT `+recordsColumns+` FROM records WHERE id=?`, strings.TrimSpace(id)))
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, false, nil
	}

	if err == nil {
		err = readRecordChildrenTx(ctx, tx, &v)
	}

	return v, err == nil, err
}

const recordsInsertSQL = `INSERT INTO records(id,kind,delegation_id,source_key,source_session_id,source_turn_id,parent_id,hop_budget,machine_id,inbox_ref,inbox_generation,remote_thread_id,thread_policy,thread_generation,thread_summary_hash,claim_id,claim_epoch,worker_instance_id,payload_hash,result_sequence,status,reason,body,spec_hash,created_at,expires_at,spec_title,spec_objective,spec_timeout_seconds,has_thread_update,update_title,update_summary,update_summary_delta,update_last_result_summary,update_next_step_hint,created_order) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
const recordsUpsertSQL = recordsInsertSQL + ` ON CONFLICT(id) DO UPDATE SET kind=excluded.kind,delegation_id=excluded.delegation_id,source_key=excluded.source_key,source_session_id=excluded.source_session_id,source_turn_id=excluded.source_turn_id,parent_id=excluded.parent_id,hop_budget=excluded.hop_budget,machine_id=excluded.machine_id,inbox_ref=excluded.inbox_ref,inbox_generation=excluded.inbox_generation,remote_thread_id=excluded.remote_thread_id,thread_policy=excluded.thread_policy,thread_generation=excluded.thread_generation,thread_summary_hash=excluded.thread_summary_hash,claim_id=excluded.claim_id,claim_epoch=excluded.claim_epoch,worker_instance_id=excluded.worker_instance_id,payload_hash=excluded.payload_hash,result_sequence=excluded.result_sequence,status=excluded.status,reason=excluded.reason,body=excluded.body,spec_hash=excluded.spec_hash,created_at=excluded.created_at,expires_at=excluded.expires_at,spec_title=excluded.spec_title,spec_objective=excluded.spec_objective,spec_timeout_seconds=excluded.spec_timeout_seconds,has_thread_update=excluded.has_thread_update,update_title=excluded.update_title,update_summary=excluded.update_summary,update_summary_delta=excluded.update_summary_delta,update_last_result_summary=excluded.update_last_result_summary,update_next_step_hint=excluded.update_next_step_hint,created_order=excluded.created_order
 WHERE (records.kind,records.delegation_id,records.source_key,records.source_session_id,records.source_turn_id,records.parent_id,records.hop_budget,records.machine_id,records.inbox_ref,records.inbox_generation,records.remote_thread_id,records.thread_policy,records.thread_generation,records.thread_summary_hash,records.claim_id,records.claim_epoch,records.worker_instance_id,records.payload_hash,records.result_sequence,records.status,records.reason,records.body,records.spec_hash,records.created_at,records.expires_at,records.spec_title,records.spec_objective,records.spec_timeout_seconds,records.has_thread_update,records.update_title,records.update_summary,records.update_summary_delta,records.update_last_result_summary,records.update_next_step_hint,records.created_order) IS NOT (excluded.kind,excluded.delegation_id,excluded.source_key,excluded.source_session_id,excluded.source_turn_id,excluded.parent_id,excluded.hop_budget,excluded.machine_id,excluded.inbox_ref,excluded.inbox_generation,excluded.remote_thread_id,excluded.thread_policy,excluded.thread_generation,excluded.thread_summary_hash,excluded.claim_id,excluded.claim_epoch,excluded.worker_instance_id,excluded.payload_hash,excluded.result_sequence,excluded.status,excluded.reason,excluded.body,excluded.spec_hash,excluded.created_at,excluded.expires_at,excluded.spec_title,excluded.spec_objective,excluded.spec_timeout_seconds,excluded.has_thread_update,excluded.update_title,excluded.update_summary,excluded.update_summary_delta,excluded.update_last_result_summary,excluded.update_next_step_hint,excluded.created_order)`

func recordsArgs(v Record) []any {
	hasThreadUpdate := v.ThreadUpdate != nil
	var update ThreadUpdate
	if hasThreadUpdate {
		update = *v.ThreadUpdate
	}
	return []any{v.RecordID, v.Kind, v.DelegationID, v.SourceKey, v.SourceSessionID, v.SourceTurnID, v.ParentID, v.HopBudget, v.MachineID, v.InboxRef, v.InboxGeneration, v.RemoteThreadID, v.ThreadPolicy, v.ThreadGeneration, v.ThreadSummaryHash, v.ClaimID, v.ClaimEpoch, v.WorkerInstanceID, v.PayloadHash, v.ResultSequence, v.Status, v.Reason, v.Body, v.SpecHash, v.CreatedAt, v.ExpiresAt, v.Spec.Title, v.Spec.Objective, v.Spec.TimeoutSeconds, hasThreadUpdate, update.Title, update.Summary, update.SummaryDelta, update.LastResultSummary, update.NextStepHint, delegationTimeOrder(v.CreatedAt)}
}

func putRecordTx(ctx context.Context, tx *sql.Tx, v Record) (int64, error) {
	v.RecordID = strings.TrimSpace(v.RecordID)
	if v.RecordID == "" {
		return 0, nil
	}
	result, err := tx.ExecContext(ctx, recordsUpsertSQL, recordsArgs(v)...)
	if err != nil {
		return 0, err
	}
	n, err := result.RowsAffected()

	if err == nil {
		var added int64
		added, err = writeRecordChildrenTx(ctx, tx, v)
		n += added
	}

	return n, err
}

const routesColumns = `id,source_key,machine_id,inbox_ref,inbox_generation,remote_thread_id,thread_policy,thread_generation,created_at,updated_at`

func scanRoute(row delegationScanner) (Route, error) {
	var v Route
	err := row.Scan(&v.DelegationID, &v.SourceKey, &v.MachineID, &v.InboxRef, &v.InboxGeneration, &v.RemoteThreadID, &v.ThreadPolicy, &v.ThreadGeneration, &v.CreatedAt, &v.UpdatedAt)
	return v, err
}

func readRouteTx(ctx context.Context, tx *sql.Tx, id string) (Route, bool, error) {
	v, err := scanRoute(tx.QueryRowContext(ctx, `SELECT `+routesColumns+` FROM routes WHERE id=?`, strings.TrimSpace(id)))
	if errors.Is(err, sql.ErrNoRows) {
		return Route{}, false, nil
	}

	return v, err == nil, err
}

const routesInsertSQL = `INSERT INTO routes(id,source_key,machine_id,inbox_ref,inbox_generation,remote_thread_id,thread_policy,thread_generation,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)`
const routesUpsertSQL = routesInsertSQL + ` ON CONFLICT(id) DO UPDATE SET source_key=excluded.source_key,machine_id=excluded.machine_id,inbox_ref=excluded.inbox_ref,inbox_generation=excluded.inbox_generation,remote_thread_id=excluded.remote_thread_id,thread_policy=excluded.thread_policy,thread_generation=excluded.thread_generation,created_at=excluded.created_at,updated_at=excluded.updated_at
 WHERE (routes.source_key,routes.machine_id,routes.inbox_ref,routes.inbox_generation,routes.remote_thread_id,routes.thread_policy,routes.thread_generation,routes.created_at,routes.updated_at) IS NOT (excluded.source_key,excluded.machine_id,excluded.inbox_ref,excluded.inbox_generation,excluded.remote_thread_id,excluded.thread_policy,excluded.thread_generation,excluded.created_at,excluded.updated_at)`

func routesArgs(v Route) []any {
	return []any{v.DelegationID, v.SourceKey, v.MachineID, v.InboxRef, v.InboxGeneration, v.RemoteThreadID, v.ThreadPolicy, v.ThreadGeneration, v.CreatedAt, v.UpdatedAt}
}

func putRouteTx(ctx context.Context, tx *sql.Tx, v Route) (int64, error) {
	v.DelegationID = strings.TrimSpace(v.DelegationID)
	if v.DelegationID == "" {
		return 0, nil
	}
	result, err := tx.ExecContext(ctx, routesUpsertSQL, routesArgs(v)...)
	if err != nil {
		return 0, err
	}
	n, err := result.RowsAffected()

	return n, err
}

const remote_threadsColumns = `id,machine_id,source_session_id,workspace_fingerprint,model_profile,protocol_version,title,summary,last_result_summary,state,active_delegation_id,generation,summary_hash,last_terminal_record_id,created_at,updated_at,last_used_at,expires_at`

func scanRemoteThread(row delegationScanner) (RemoteThread, error) {
	var v RemoteThread
	err := row.Scan(&v.ThreadID, &v.MachineID, &v.SourceSessionID, &v.WorkspaceFingerprint, &v.ModelProfile, &v.ProtocolVersion, &v.Title, &v.Summary, &v.LastResultSummary, &v.State, &v.ActiveDelegationID, &v.Generation, &v.SummaryHash, &v.LastTerminalRecordID, &v.CreatedAt, &v.UpdatedAt, &v.LastUsedAt, &v.ExpiresAt)
	return v, err
}

func readRemoteThreadTx(ctx context.Context, tx *sql.Tx, id string) (RemoteThread, bool, error) {
	v, err := scanRemoteThread(tx.QueryRowContext(ctx, `SELECT `+remote_threadsColumns+` FROM remote_threads WHERE id=?`, strings.TrimSpace(id)))
	if errors.Is(err, sql.ErrNoRows) {
		return RemoteThread{}, false, nil
	}

	return v, err == nil, err
}

const remote_threadsInsertSQL = `INSERT INTO remote_threads(id,machine_id,source_session_id,workspace_fingerprint,model_profile,protocol_version,title,summary,last_result_summary,state,active_delegation_id,generation,summary_hash,last_terminal_record_id,created_at,updated_at,last_used_at,expires_at,last_used_order,expires_order) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
const remote_threadsUpsertSQL = remote_threadsInsertSQL + ` ON CONFLICT(id) DO UPDATE SET machine_id=excluded.machine_id,source_session_id=excluded.source_session_id,workspace_fingerprint=excluded.workspace_fingerprint,model_profile=excluded.model_profile,protocol_version=excluded.protocol_version,title=excluded.title,summary=excluded.summary,last_result_summary=excluded.last_result_summary,state=excluded.state,active_delegation_id=excluded.active_delegation_id,generation=excluded.generation,summary_hash=excluded.summary_hash,last_terminal_record_id=excluded.last_terminal_record_id,created_at=excluded.created_at,updated_at=excluded.updated_at,last_used_at=excluded.last_used_at,expires_at=excluded.expires_at,last_used_order=excluded.last_used_order,expires_order=excluded.expires_order
 WHERE (remote_threads.machine_id,remote_threads.source_session_id,remote_threads.workspace_fingerprint,remote_threads.model_profile,remote_threads.protocol_version,remote_threads.title,remote_threads.summary,remote_threads.last_result_summary,remote_threads.state,remote_threads.active_delegation_id,remote_threads.generation,remote_threads.summary_hash,remote_threads.last_terminal_record_id,remote_threads.created_at,remote_threads.updated_at,remote_threads.last_used_at,remote_threads.expires_at,remote_threads.last_used_order,remote_threads.expires_order) IS NOT (excluded.machine_id,excluded.source_session_id,excluded.workspace_fingerprint,excluded.model_profile,excluded.protocol_version,excluded.title,excluded.summary,excluded.last_result_summary,excluded.state,excluded.active_delegation_id,excluded.generation,excluded.summary_hash,excluded.last_terminal_record_id,excluded.created_at,excluded.updated_at,excluded.last_used_at,excluded.expires_at,excluded.last_used_order,excluded.expires_order)`

func remote_threadsArgs(v RemoteThread) []any {
	return []any{v.ThreadID, v.MachineID, v.SourceSessionID, v.WorkspaceFingerprint, v.ModelProfile, v.ProtocolVersion, v.Title, v.Summary, v.LastResultSummary, v.State, v.ActiveDelegationID, v.Generation, v.SummaryHash, v.LastTerminalRecordID, v.CreatedAt, v.UpdatedAt, v.LastUsedAt, v.ExpiresAt, delegationTimeOrder(firstNonEmptyString(v.LastUsedAt, v.UpdatedAt, v.CreatedAt)), delegationTimeOrder(v.ExpiresAt)}
}

func putRemoteThreadTx(ctx context.Context, tx *sql.Tx, v RemoteThread) (int64, error) {
	v.ThreadID = strings.TrimSpace(v.ThreadID)
	if v.ThreadID == "" {
		return 0, nil
	}
	result, err := tx.ExecContext(ctx, remote_threadsUpsertSQL, remote_threadsArgs(v)...)
	if err != nil {
		return 0, err
	}
	n, err := result.RowsAffected()

	return n, err
}

const executionsColumns = `id,claim_id,claim_epoch,worker_instance_id,machine_id,status,started_at,updated_at`

func scanExecutionFence(row delegationScanner) (ExecutionFence, error) {
	var v ExecutionFence
	err := row.Scan(&v.DelegationID, &v.ClaimID, &v.ClaimEpoch, &v.WorkerInstanceID, &v.MachineID, &v.Status, &v.StartedAt, &v.UpdatedAt)
	return v, err
}

func readExecutionFenceTx(ctx context.Context, tx *sql.Tx, id string) (ExecutionFence, bool, error) {
	v, err := scanExecutionFence(tx.QueryRowContext(ctx, `SELECT `+executionsColumns+` FROM executions WHERE id=?`, strings.TrimSpace(id)))
	if errors.Is(err, sql.ErrNoRows) {
		return ExecutionFence{}, false, nil
	}

	return v, err == nil, err
}

const executionsInsertSQL = `INSERT INTO executions(id,claim_id,claim_epoch,worker_instance_id,machine_id,status,started_at,updated_at,updated_order) VALUES(?,?,?,?,?,?,?,?,?)`
const executionsUpsertSQL = executionsInsertSQL + ` ON CONFLICT(id) DO UPDATE SET claim_id=excluded.claim_id,claim_epoch=excluded.claim_epoch,worker_instance_id=excluded.worker_instance_id,machine_id=excluded.machine_id,status=excluded.status,started_at=excluded.started_at,updated_at=excluded.updated_at,updated_order=excluded.updated_order
 WHERE (executions.claim_id,executions.claim_epoch,executions.worker_instance_id,executions.machine_id,executions.status,executions.started_at,executions.updated_at,executions.updated_order) IS NOT (excluded.claim_id,excluded.claim_epoch,excluded.worker_instance_id,excluded.machine_id,excluded.status,excluded.started_at,excluded.updated_at,excluded.updated_order)`

func executionsArgs(v ExecutionFence) []any {
	return []any{v.DelegationID, v.ClaimID, v.ClaimEpoch, v.WorkerInstanceID, v.MachineID, v.Status, v.StartedAt, v.UpdatedAt, delegationTimeOrder(v.UpdatedAt)}
}

func putExecutionFenceTx(ctx context.Context, tx *sql.Tx, v ExecutionFence) (int64, error) {
	v.DelegationID = strings.TrimSpace(v.DelegationID)
	if v.DelegationID == "" {
		return 0, nil
	}
	result, err := tx.ExecContext(ctx, executionsUpsertSQL, executionsArgs(v)...)
	if err != nil {
		return 0, err
	}
	n, err := result.RowsAffected()

	return n, err
}

const outboxColumns = `id,delegation_id,chat_id,inbox_ref,status,message_id,attempts,error,created_at,updated_at`

func scanOutboxRecord(row delegationScanner) (OutboxRecord, error) {
	var v OutboxRecord
	err := row.Scan(&v.RecordID, &v.DelegationID, &v.ChatID, &v.InboxRef, &v.Status, &v.MessageID, &v.Attempts, &v.Error, &v.CreatedAt, &v.UpdatedAt)
	return v, err
}

func readOutboxRecordTx(ctx context.Context, tx *sql.Tx, id string) (OutboxRecord, bool, error) {
	v, err := scanOutboxRecord(tx.QueryRowContext(ctx, `SELECT `+outboxColumns+` FROM outbox WHERE id=?`, strings.TrimSpace(id)))
	if errors.Is(err, sql.ErrNoRows) {
		return OutboxRecord{}, false, nil
	}

	return v, err == nil, err
}

const outboxInsertSQL = `INSERT INTO outbox(id,delegation_id,chat_id,inbox_ref,status,message_id,attempts,error,created_at,updated_at,updated_order) VALUES(?,?,?,?,?,?,?,?,?,?,?)`
const outboxUpsertSQL = outboxInsertSQL + ` ON CONFLICT(id) DO UPDATE SET delegation_id=excluded.delegation_id,chat_id=excluded.chat_id,inbox_ref=excluded.inbox_ref,status=excluded.status,message_id=excluded.message_id,attempts=excluded.attempts,error=excluded.error,created_at=excluded.created_at,updated_at=excluded.updated_at,updated_order=excluded.updated_order
 WHERE (outbox.delegation_id,outbox.chat_id,outbox.inbox_ref,outbox.status,outbox.message_id,outbox.attempts,outbox.error,outbox.created_at,outbox.updated_at,outbox.updated_order) IS NOT (excluded.delegation_id,excluded.chat_id,excluded.inbox_ref,excluded.status,excluded.message_id,excluded.attempts,excluded.error,excluded.created_at,excluded.updated_at,excluded.updated_order)`

func outboxArgs(v OutboxRecord) []any {
	return []any{v.RecordID, v.DelegationID, v.ChatID, v.InboxRef, v.Status, v.MessageID, v.Attempts, v.Error, v.CreatedAt, v.UpdatedAt, delegationTimeOrder(v.UpdatedAt)}
}

func putOutboxRecordTx(ctx context.Context, tx *sql.Tx, v OutboxRecord) (int64, error) {
	v.RecordID = strings.TrimSpace(v.RecordID)
	if v.RecordID == "" {
		return 0, nil
	}
	result, err := tx.ExecContext(ctx, outboxUpsertSQL, outboxArgs(v)...)
	if err != nil {
		return 0, err
	}
	n, err := result.RowsAffected()

	return n, err
}

const inbox_cursorsColumns = `id,last_head_message_id,continuation_path,updated_at`

func scanInboxCursor(row delegationScanner) (InboxCursor, error) {
	var v InboxCursor
	err := row.Scan(&v.ChatID, &v.LastHeadMessageID, &v.ContinuationPath, &v.UpdatedAt)
	return v, err
}

func readInboxCursorTx(ctx context.Context, tx *sql.Tx, id string) (InboxCursor, bool, error) {
	v, err := scanInboxCursor(tx.QueryRowContext(ctx, `SELECT `+inbox_cursorsColumns+` FROM inbox_cursors WHERE id=?`, strings.TrimSpace(id)))
	if errors.Is(err, sql.ErrNoRows) {
		return InboxCursor{}, false, nil
	}

	return v, err == nil, err
}

const inbox_cursorsInsertSQL = `INSERT INTO inbox_cursors(id,last_head_message_id,continuation_path,updated_at,updated_order) VALUES(?,?,?,?,?)`
const inbox_cursorsUpsertSQL = inbox_cursorsInsertSQL + ` ON CONFLICT(id) DO UPDATE SET last_head_message_id=excluded.last_head_message_id,continuation_path=excluded.continuation_path,updated_at=excluded.updated_at,updated_order=excluded.updated_order
 WHERE (inbox_cursors.last_head_message_id,inbox_cursors.continuation_path,inbox_cursors.updated_at,inbox_cursors.updated_order) IS NOT (excluded.last_head_message_id,excluded.continuation_path,excluded.updated_at,excluded.updated_order)`

func inbox_cursorsArgs(v InboxCursor) []any {
	return []any{v.ChatID, v.LastHeadMessageID, v.ContinuationPath, v.UpdatedAt, delegationTimeOrder(v.UpdatedAt)}
}

func putInboxCursorTx(ctx context.Context, tx *sql.Tx, v InboxCursor) (int64, error) {
	v.ChatID = strings.TrimSpace(v.ChatID)
	if v.ChatID == "" {
		return 0, nil
	}
	result, err := tx.ExecContext(ctx, inbox_cursorsUpsertSQL, inbox_cursorsArgs(v)...)
	if err != nil {
		return 0, err
	}
	n, err := result.RowsAffected()

	return n, err
}

const inbox_backoffsColumns = `id,blocked_until,reason,updated_at`

func scanInboxBackoff(row delegationScanner) (InboxBackoff, error) {
	var v InboxBackoff
	err := row.Scan(&v.ChatID, &v.BlockedUntil, &v.Reason, &v.UpdatedAt)
	return v, err
}

func readInboxBackoffTx(ctx context.Context, tx *sql.Tx, id string) (InboxBackoff, bool, error) {
	v, err := scanInboxBackoff(tx.QueryRowContext(ctx, `SELECT `+inbox_backoffsColumns+` FROM inbox_backoffs WHERE id=?`, strings.TrimSpace(id)))
	if errors.Is(err, sql.ErrNoRows) {
		return InboxBackoff{}, false, nil
	}

	return v, err == nil, err
}

const inbox_backoffsInsertSQL = `INSERT INTO inbox_backoffs(id,blocked_until,reason,updated_at,updated_order,blocked_order) VALUES(?,?,?,?,?,?)`
const inbox_backoffsUpsertSQL = inbox_backoffsInsertSQL + ` ON CONFLICT(id) DO UPDATE SET blocked_until=excluded.blocked_until,reason=excluded.reason,updated_at=excluded.updated_at,updated_order=excluded.updated_order,blocked_order=excluded.blocked_order
 WHERE (inbox_backoffs.blocked_until,inbox_backoffs.reason,inbox_backoffs.updated_at,inbox_backoffs.updated_order,inbox_backoffs.blocked_order) IS NOT (excluded.blocked_until,excluded.reason,excluded.updated_at,excluded.updated_order,excluded.blocked_order)`

func inbox_backoffsArgs(v InboxBackoff) []any {
	return []any{v.ChatID, v.BlockedUntil, v.Reason, v.UpdatedAt, delegationTimeOrder(v.UpdatedAt), delegationTimeOrder(v.BlockedUntil)}
}

func putInboxBackoffTx(ctx context.Context, tx *sql.Tx, v InboxBackoff) (int64, error) {
	v.ChatID = strings.TrimSpace(v.ChatID)
	if v.ChatID == "" {
		return 0, nil
	}
	result, err := tx.ExecContext(ctx, inbox_backoffsUpsertSQL, inbox_backoffsArgs(v)...)
	if err != nil {
		return 0, err
	}
	n, err := result.RowsAffected()

	return n, err
}

var delegationNativeSchema = []string{
	`CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)`,
	`CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY CHECK(id<>''),kind TEXT NOT NULL,delegation_id TEXT NOT NULL,source_key TEXT NOT NULL,source_session_id TEXT NOT NULL,source_turn_id TEXT NOT NULL,parent_id TEXT NOT NULL,hop_budget INTEGER NOT NULL,machine_id TEXT NOT NULL,inbox_ref TEXT NOT NULL,inbox_generation TEXT NOT NULL,remote_thread_id TEXT NOT NULL,thread_policy TEXT NOT NULL,thread_generation TEXT NOT NULL,thread_summary_hash TEXT NOT NULL,claim_id TEXT NOT NULL,claim_epoch INTEGER NOT NULL,worker_instance_id TEXT NOT NULL,payload_hash TEXT NOT NULL,result_sequence INTEGER NOT NULL,status TEXT NOT NULL,reason TEXT NOT NULL,body TEXT NOT NULL,spec_hash TEXT NOT NULL,created_at TEXT NOT NULL,expires_at TEXT NOT NULL,spec_title TEXT NOT NULL,spec_objective TEXT NOT NULL,spec_timeout_seconds INTEGER NOT NULL,has_thread_update INTEGER NOT NULL CHECK(has_thread_update IN (0,1)),update_title TEXT NOT NULL,update_summary TEXT NOT NULL,update_summary_delta TEXT NOT NULL,update_last_result_summary TEXT NOT NULL,update_next_step_hint TEXT NOT NULL,created_order TEXT NOT NULL)`,
	`CREATE TABLE routes(id TEXT NOT NULL PRIMARY KEY CHECK(id<>''),source_key TEXT NOT NULL,machine_id TEXT NOT NULL,inbox_ref TEXT NOT NULL,inbox_generation TEXT NOT NULL,remote_thread_id TEXT NOT NULL,thread_policy TEXT NOT NULL,thread_generation TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL)`,
	`CREATE TABLE remote_threads(id TEXT NOT NULL PRIMARY KEY CHECK(id<>''),machine_id TEXT NOT NULL,source_session_id TEXT NOT NULL,workspace_fingerprint TEXT NOT NULL,model_profile TEXT NOT NULL,protocol_version TEXT NOT NULL,title TEXT NOT NULL,summary TEXT NOT NULL,last_result_summary TEXT NOT NULL,state TEXT NOT NULL,active_delegation_id TEXT NOT NULL,generation TEXT NOT NULL,summary_hash TEXT NOT NULL,last_terminal_record_id TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,last_used_at TEXT NOT NULL,expires_at TEXT NOT NULL,last_used_order TEXT NOT NULL,expires_order TEXT NOT NULL)`,
	`CREATE TABLE executions(id TEXT NOT NULL PRIMARY KEY CHECK(id<>''),claim_id TEXT NOT NULL,claim_epoch INTEGER NOT NULL,worker_instance_id TEXT NOT NULL,machine_id TEXT NOT NULL,status TEXT NOT NULL,started_at TEXT NOT NULL,updated_at TEXT NOT NULL,updated_order TEXT NOT NULL)`,
	`CREATE TABLE outbox(id TEXT NOT NULL PRIMARY KEY CHECK(id<>''),delegation_id TEXT NOT NULL,chat_id TEXT NOT NULL,inbox_ref TEXT NOT NULL,status TEXT NOT NULL,message_id TEXT NOT NULL,attempts INTEGER NOT NULL,error TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,updated_order TEXT NOT NULL)`,
	`CREATE TABLE inbox_cursors(id TEXT NOT NULL PRIMARY KEY CHECK(id<>''),last_head_message_id TEXT NOT NULL,continuation_path TEXT NOT NULL,updated_at TEXT NOT NULL,updated_order TEXT NOT NULL)`,
	`CREATE TABLE inbox_backoffs(id TEXT NOT NULL PRIMARY KEY CHECK(id<>''),blocked_until TEXT NOT NULL,reason TEXT NOT NULL,updated_at TEXT NOT NULL,updated_order TEXT NOT NULL,blocked_order TEXT NOT NULL)`,
	`CREATE TABLE record_path(record_id TEXT NOT NULL REFERENCES records(id) ON DELETE CASCADE,position INTEGER NOT NULL,value TEXT NOT NULL,PRIMARY KEY(record_id,position))`,
	`CREATE TABLE record_constraints(record_id TEXT NOT NULL REFERENCES records(id) ON DELETE CASCADE,position INTEGER NOT NULL,value TEXT NOT NULL,PRIMARY KEY(record_id,position))`,
	`CREATE TABLE record_actions(record_id TEXT NOT NULL REFERENCES records(id) ON DELETE CASCADE,position INTEGER NOT NULL,value TEXT NOT NULL,PRIMARY KEY(record_id,position))`,
	`CREATE TABLE record_context(record_id TEXT NOT NULL REFERENCES records(id) ON DELETE CASCADE,position INTEGER NOT NULL,value TEXT NOT NULL,PRIMARY KEY(record_id,position))`,
	`CREATE TABLE record_artifacts(record_id TEXT NOT NULL REFERENCES records(id) ON DELETE CASCADE,position INTEGER NOT NULL,kind TEXT NOT NULL,name TEXT NOT NULL,path TEXT NOT NULL,hash TEXT NOT NULL,PRIMARY KEY(record_id,position))`,
	`CREATE INDEX records_delegation ON records(delegation_id,created_at,id)`,
	`CREATE INDEX records_source ON records(source_key,kind)`,
	`CREATE INDEX records_retention ON records(created_order,id) WHERE kind IN ('cxp.delegation.request.v1','cxp.delegation.result.v1','cxp.delegation.tombstone.v1')`,
	`CREATE INDEX remote_threads_machine ON remote_threads(machine_id,last_used_order DESC,id) WHERE state NOT IN ('active','closed','stale') AND active_delegation_id=''`,
	`CREATE INDEX remote_threads_used ON remote_threads(last_used_order) WHERE state<>'active'`,
	`CREATE INDEX remote_threads_expiry ON remote_threads(expires_order) WHERE state<>'active'`,
	`CREATE INDEX executions_terminal ON executions(updated_order DESC,id) WHERE status IN ('complete','blocked','canceled','expired','conflict','reuse_rejected')`,
	`CREATE INDEX outbox_terminal ON outbox(updated_order DESC,id) WHERE status IN ('visible','failed')`,
	`CREATE INDEX outbox_delegation ON outbox(delegation_id)`,
	`CREATE INDEX inbox_cursor_retention ON inbox_cursors(updated_order)`,
	`CREATE INDEX inbox_backoff_retention ON inbox_backoffs(updated_order)`,
	`CREATE INDEX inbox_backoff_expiry ON inbox_backoffs(blocked_order)`,
}

func readRecordChildrenTx(ctx context.Context, tx *sql.Tx, v *Record) error {

	rowsrecord_path, err := tx.QueryContext(ctx, `SELECT value FROM record_path WHERE record_id=? ORDER BY position`, v.RecordID)
	if err != nil {
		return err
	}
	for rowsrecord_path.Next() {
		var s string
		if err = rowsrecord_path.Scan(&s); err != nil {
			rowsrecord_path.Close()
			return err
		}
		v.Path = append(v.Path, s)
	}
	err = rowsrecord_path.Err()
	rowsrecord_path.Close()
	if err != nil {
		return err
	}

	rowsrecord_constraints, err := tx.QueryContext(ctx, `SELECT value FROM record_constraints WHERE record_id=? ORDER BY position`, v.RecordID)
	if err != nil {
		return err
	}
	for rowsrecord_constraints.Next() {
		var s string
		if err = rowsrecord_constraints.Scan(&s); err != nil {
			rowsrecord_constraints.Close()
			return err
		}
		v.Spec.Constraints = append(v.Spec.Constraints, s)
	}
	err = rowsrecord_constraints.Err()
	rowsrecord_constraints.Close()
	if err != nil {
		return err
	}

	rowsrecord_actions, err := tx.QueryContext(ctx, `SELECT value FROM record_actions WHERE record_id=? ORDER BY position`, v.RecordID)
	if err != nil {
		return err
	}
	for rowsrecord_actions.Next() {
		var s string
		if err = rowsrecord_actions.Scan(&s); err != nil {
			rowsrecord_actions.Close()
			return err
		}
		v.Spec.AllowedActions = append(v.Spec.AllowedActions, s)
	}
	err = rowsrecord_actions.Err()
	rowsrecord_actions.Close()
	if err != nil {
		return err
	}

	rowsrecord_context, err := tx.QueryContext(ctx, `SELECT value FROM record_context WHERE record_id=? ORDER BY position`, v.RecordID)
	if err != nil {
		return err
	}
	for rowsrecord_context.Next() {
		var s string
		if err = rowsrecord_context.Scan(&s); err != nil {
			rowsrecord_context.Close()
			return err
		}
		v.Spec.Context = append(v.Spec.Context, s)
	}
	err = rowsrecord_context.Err()
	rowsrecord_context.Close()
	if err != nil {
		return err
	}

	rows, err := tx.QueryContext(ctx, `SELECT kind,name,path,hash FROM record_artifacts WHERE record_id=? ORDER BY position`, v.RecordID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var a ArtifactRef
		if err = rows.Scan(&a.Kind, &a.Name, &a.Path, &a.Hash); err != nil {
			return err
		}
		v.Spec.ArtifactRefs = append(v.Spec.ArtifactRefs, a)
	}
	return rows.Err()
}

func writeRecordChildrenTx(ctx context.Context, tx *sql.Tx, v Record) (int64, error) {
	old := Record{RecordID: v.RecordID}
	if err := readRecordChildrenTx(ctx, tx, &old); err != nil {
		return 0, err
	}
	var n int64

	if !slices.Equal(v.Path, old.Path) {
		result, err := tx.ExecContext(ctx, `DELETE FROM record_path WHERE record_id=?`, v.RecordID)
		if err != nil {
			return 0, err
		}
		deleted, _ := result.RowsAffected()
		n += deleted
		stmt, err := tx.PrepareContext(ctx, `INSERT INTO record_path(record_id,position,value) VALUES(?,?,?)`)
		if err != nil {
			return 0, err
		}
		for i, s := range v.Path {
			if _, err = stmt.ExecContext(ctx, v.RecordID, i, s); err != nil {
				stmt.Close()
				return 0, err
			}
			n++
		}
		if err = stmt.Close(); err != nil {
			return 0, err
		}
	}

	if !slices.Equal(v.Spec.Constraints, old.Spec.Constraints) {
		result, err := tx.ExecContext(ctx, `DELETE FROM record_constraints WHERE record_id=?`, v.RecordID)
		if err != nil {
			return 0, err
		}
		deleted, _ := result.RowsAffected()
		n += deleted
		stmt, err := tx.PrepareContext(ctx, `INSERT INTO record_constraints(record_id,position,value) VALUES(?,?,?)`)
		if err != nil {
			return 0, err
		}
		for i, s := range v.Spec.Constraints {
			if _, err = stmt.ExecContext(ctx, v.RecordID, i, s); err != nil {
				stmt.Close()
				return 0, err
			}
			n++
		}
		if err = stmt.Close(); err != nil {
			return 0, err
		}
	}

	if !slices.Equal(v.Spec.AllowedActions, old.Spec.AllowedActions) {
		result, err := tx.ExecContext(ctx, `DELETE FROM record_actions WHERE record_id=?`, v.RecordID)
		if err != nil {
			return 0, err
		}
		deleted, _ := result.RowsAffected()
		n += deleted
		stmt, err := tx.PrepareContext(ctx, `INSERT INTO record_actions(record_id,position,value) VALUES(?,?,?)`)
		if err != nil {
			return 0, err
		}
		for i, s := range v.Spec.AllowedActions {
			if _, err = stmt.ExecContext(ctx, v.RecordID, i, s); err != nil {
				stmt.Close()
				return 0, err
			}
			n++
		}
		if err = stmt.Close(); err != nil {
			return 0, err
		}
	}

	if !slices.Equal(v.Spec.Context, old.Spec.Context) {
		result, err := tx.ExecContext(ctx, `DELETE FROM record_context WHERE record_id=?`, v.RecordID)
		if err != nil {
			return 0, err
		}
		deleted, _ := result.RowsAffected()
		n += deleted
		stmt, err := tx.PrepareContext(ctx, `INSERT INTO record_context(record_id,position,value) VALUES(?,?,?)`)
		if err != nil {
			return 0, err
		}
		for i, s := range v.Spec.Context {
			if _, err = stmt.ExecContext(ctx, v.RecordID, i, s); err != nil {
				stmt.Close()
				return 0, err
			}
			n++
		}
		if err = stmt.Close(); err != nil {
			return 0, err
		}
	}

	if !slices.Equal(v.Spec.ArtifactRefs, old.Spec.ArtifactRefs) {
		result, err := tx.ExecContext(ctx, `DELETE FROM record_artifacts WHERE record_id=?`, v.RecordID)
		if err != nil {
			return 0, err
		}
		deleted, _ := result.RowsAffected()
		n += deleted
		stmt, err := tx.PrepareContext(ctx, `INSERT INTO record_artifacts(record_id,position,kind,name,path,hash) VALUES(?,?,?,?,?,?)`)
		if err != nil {
			return 0, err
		}
		for i, a := range v.Spec.ArtifactRefs {
			if _, err = stmt.ExecContext(ctx, v.RecordID, i, a.Kind, a.Name, a.Path, a.Hash); err != nil {
				stmt.Close()
				return 0, err
			}
			n++
		}
		if err = stmt.Close(); err != nil {
			return 0, err
		}
	}
	return n, nil
}
