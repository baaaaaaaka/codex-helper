package delegation

import (
	"context"
	"database/sql"
	"time"
)

// A strict lower bound seeks past legacy records with no usable timestamp.
// A not-equal predicate would visit that entire index prefix per operation.
const delegationRetentionPageSQL = `SELECT created_order,id,delegation_id FROM records
WHERE kind IN ('cxp.delegation.request.v1','cxp.delegation.result.v1','cxp.delegation.tombstone.v1')
AND created_order>'' AND created_order<? AND (created_order,id)>(?,?)
ORDER BY created_order,id LIMIT 128`

const delegationBackoffExpirySQL = `DELETE FROM inbox_backoffs WHERE blocked_order>'' AND blocked_order<?`

// Pruning retains the reducer's existing decision about terminal/expired
// delegations. SQL selects old metadata in keyset pages; bodies and task lists
// are not read by cleanup.
func pruneDelegationSQLiteTx(ctx context.Context, tx *sql.Tx, now time.Time, retention time.Duration) error {
	if retention <= 0 {
		retention = DefaultStoreRetention
	}
	cutoff := now.UTC().Add(-retention)
	before := delegationTimeOrder(cutoff.Format(time.RFC3339Nano))
	var afterTime, afterID string
	for {
		rows, err := tx.QueryContext(ctx, delegationRetentionPageSQL, before, afterTime, afterID)
		if err != nil {
			return err
		}
		var ids []string
		seen := map[string]bool{}
		for rows.Next() {
			var id string
			if err = rows.Scan(&afterTime, &afterID, &id); err != nil {
				rows.Close()
				return err
			}
			if !seen[id] {
				ids = append(ids, id)
				seen[id] = true
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			break
		}
		for _, id := range ids {
			if id == "" {
				continue
			}
			state, err := retentionStateTx(ctx, tx, id, now)
			if err != nil {
				return err
			}
			at := stateTime(state)
			if !terminalOrExpiredState(state) || at.IsZero() || !at.Before(cutoff) {
				continue
			}
			// Parent/children and all associated safety evidence leave together,
			// only after the existing retention policy permits forgetting them.
			for _, q := range []string{`DELETE FROM records WHERE delegation_id=?`, `DELETE FROM outbox WHERE delegation_id=?`, `DELETE FROM executions WHERE id=?`, `DELETE FROM routes WHERE id=?`} {
				if _, err = tx.ExecContext(ctx, q, id); err != nil {
					return err
				}
			}
		}
	}
	for _, q := range []string{
		`DELETE FROM remote_threads WHERE state<>'active' AND last_used_order>'' AND last_used_order<?`,
		`DELETE FROM remote_threads WHERE state<>'active' AND expires_order>'' AND expires_order<?`,
		`DELETE FROM executions WHERE status IN ('complete','blocked','canceled','expired','conflict','reuse_rejected') AND updated_order<?`,
		`DELETE FROM inbox_cursors WHERE updated_order>'' AND updated_order<?`,
		`DELETE FROM inbox_backoffs WHERE updated_order>'' AND updated_order<?`,
		delegationBackoffExpirySQL,
	} {
		if _, err := tx.ExecContext(ctx, q, before); err != nil {
			return err
		}
	}
	return pruneOutboxSQLiteByRetentionTx(ctx, tx, now, retention)
}

// The reducer's retention decision needs claim identity, status and time, not
// answer bodies, task context or artifact lists. Reuse its rules with that
// projection so the SQL cleanup cannot diverge from cancel/claim semantics.
func retentionStateTx(ctx context.Context, tx *sql.Tx, id string, now time.Time) (State, error) {
	rows, err := tx.QueryContext(ctx, `SELECT kind,id,delegation_id,source_key,spec_hash,machine_id,claim_id,claim_epoch,worker_instance_id,payload_hash,result_sequence,status,created_at,expires_at FROM records WHERE delegation_id=?`, id)
	if err != nil {
		return State{}, err
	}
	defer rows.Close()
	var records []Record
	for rows.Next() {
		var v Record
		if err = rows.Scan(&v.Kind, &v.RecordID, &v.DelegationID, &v.SourceKey, &v.SpecHash, &v.MachineID, &v.ClaimID, &v.ClaimEpoch, &v.WorkerInstanceID, &v.PayloadHash, &v.ResultSequence, &v.Status, &v.CreatedAt, &v.ExpiresAt); err != nil {
			return State{}, err
		}
		records = append(records, v)
	}
	if err = rows.Err(); err != nil {
		return State{}, err
	}
	return Reduce(records, now), nil
}
