package delegation

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
	"os"
	"sort"
	"strings"
	"time"
)

// WorkerDB owns the connection for a command or worker lifetime. Records and
// their ordered task lists share one transaction; no JSON snapshot is loaded
// or saved by a worker operation.
type WorkerDB struct{ db *sql.DB }

func OpenWorkerDB(path string) (*WorkerDB, error) {
	if !storePathUsesSQLite(path) {
		return nil, fmt.Errorf("delegation state requires a .sqlite path: %q", path)
	}
	if _, err := sqlitestate.Inspect(path); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	db, err := openDelegationSQLiteStore(path, true)
	if err != nil {
		return nil, err
	}
	if err = sqlitestate.CheckPublishedSchema(db); err != nil {
		db.Close()
		return nil, err
	}
	if err = migrateDelegationSQLite(context.Background(), db, legacyJSONStorePathForSQLite(path)); err != nil {
		db.Close()
		return nil, err
	}
	return &WorkerDB{db: db}, nil
}

func (s *WorkerDB) Close() error { return s.db.Close() }

func (s *WorkerDB) transaction(ctx context.Context, write bool, fn func(*sql.Tx) error) error {
	if ctx == nil {
		return fmt.Errorf("delegation operation requires a context")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: !write})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// _txlock=immediate acquires the writer before the callback. The driver
	// uses a deferred transaction for ReadOnly, so reads never take that lock.
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func recordsForDelegationTx(ctx context.Context, tx *sql.Tx, id string) ([]Record, error) {
	rows, err := tx.QueryContext(ctx, `SELECT `+recordsColumns+` FROM records WHERE delegation_id=?`, strings.TrimSpace(id))
	if err != nil {
		return nil, err
	}
	var records []Record
	for rows.Next() {
		record, err := scanRecord(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		records = append(records, record)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	for i := range records {
		if err = readRecordChildrenTx(ctx, tx, &records[i]); err != nil {
			return nil, err
		}
	}
	// Wire timestamps retain their exact spelling. The reducer's ordering
	// uses parsed times, including nanoseconds and offsets.
	sort.SliceStable(records, func(i, j int) bool {
		a, b := parseRecordTime(records[i].CreatedAt), parseRecordTime(records[j].CreatedAt)
		if a.Equal(b) {
			return records[i].RecordID < records[j].RecordID
		}
		return a.Before(b)
	})
	return records, nil
}

func (s *WorkerDB) Records(ctx context.Context, id string) ([]Record, error) {
	var records []Record
	err := s.transaction(ctx, false, func(tx *sql.Tx) error {
		var err error
		records, err = recordsForDelegationTx(ctx, tx, id)
		return err
	})
	return records, err
}

func (s *WorkerDB) Append(ctx context.Context, record Record, now time.Time) ([]Record, error) {
	var records []Record
	err := s.transaction(ctx, true, func(tx *sql.Tx) error {
		if _, err := putRecordTx(ctx, tx, record); err != nil {
			return err
		}
		if err := pruneDelegationSQLiteTx(ctx, tx, now, DefaultStoreRetention); err != nil {
			return err
		}
		var err error
		records, err = recordsForDelegationTx(ctx, tx, record.DelegationID)
		return err
	})
	return records, err
}

// Start deduplicates the source key and creates the associated thread in the
// same write transaction. Another CLI cannot race a second local request in.
func (s *WorkerDB) Start(ctx context.Context, record Record, thread *RemoteThread, now time.Time) (Record, []Record, bool, error) {
	if strings.TrimSpace(record.RecordID) == "" || strings.TrimSpace(record.SourceKey) == "" {
		return Record{}, nil, false, fmt.Errorf("delegation request and source keys are required")
	}
	idempotent := false
	var records []Record
	err := s.transaction(ctx, true, func(tx *sql.Tx) error {
		var existingID string
		err := tx.QueryRowContext(ctx, `SELECT id FROM records WHERE source_key=? AND kind=? LIMIT 1`, record.SourceKey, RequestKind).Scan(&existingID)
		if err == nil {
			var ok bool
			record, ok, err = readRecordTx(ctx, tx, existingID)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("delegation request disappeared")
			}
			idempotent = true
		} else if err != sql.ErrNoRows {
			return err
		} else {
			if _, err = putRecordTx(ctx, tx, record); err != nil {
				return err
			}
			if thread != nil {
				next := *thread
				current, ok, err := readRemoteThreadTx(ctx, tx, next.ThreadID)
				if err != nil {
					return err
				}
				if ok {
					if current.State == RemoteThreadStateActive && current.ActiveDelegationID != "" && current.ActiveDelegationID != record.DelegationID {
						return fmt.Errorf("remote thread %s is active", next.ThreadID)
					}
					if record.ThreadPolicy == ThreadPolicyReuse && record.ThreadGeneration != "" && current.Generation != record.ThreadGeneration {
						return fmt.Errorf("remote thread %s generation changed", next.ThreadID)
					}
					// Keep summaries changed since the CLI validated its token.
					next.Summary, next.LastResultSummary, next.SummaryHash = current.Summary, current.LastResultSummary, current.SummaryHash
					next.Title = firstNonEmptyString(current.Title, next.Title)
					next.CreatedAt = firstNonEmptyString(current.CreatedAt, next.CreatedAt)
					next.ExpiresAt = firstNonEmptyString(current.ExpiresAt, next.ExpiresAt)
					next.ModelProfile, next.ProtocolVersion = current.ModelProfile, current.ProtocolVersion
				}
				next = normalizeRemoteThread(next, current)
				if _, err = putRemoteThreadTx(ctx, tx, next); err != nil {
					return err
				}
			}
			if err := pruneDelegationSQLiteTx(ctx, tx, now, DefaultStoreRetention); err != nil {
				return err
			}
		}
		records, err = recordsForDelegationTx(ctx, tx, record.DelegationID)
		return err
	})
	return record, records, idempotent, err
}

// ChangeThread holds the writer across a point read and its business update.
// Callbacks must not perform network or process operations.
func (s *WorkerDB) ChangeThread(ctx context.Context, id string, change func(*RemoteThread) error) error {
	return s.transaction(ctx, true, func(tx *sql.Tx) error {
		thread, _, err := readRemoteThreadTx(ctx, tx, id)
		if err != nil {
			return err
		}
		thread.ThreadID = strings.TrimSpace(id)
		existing := thread
		if err = change(&thread); err != nil {
			return err
		}
		thread = normalizeRemoteThread(thread, existing)
		_, err = putRemoteThreadTx(ctx, tx, thread)
		return err
	})
}

// ReusableThreads reads only the selected machine's eligible candidates.
// Summaries for unrelated machines and already ineligible rows stay in SQLite.
func (s *WorkerDB) ReusableThreads(ctx context.Context, machine, session, workspace string, now time.Time, limit int) ([]RemoteThread, error) {
	if limit <= 0 || limit > 128 {
		limit = 128
	}
	var threads []RemoteThread
	err := s.transaction(ctx, false, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT `+remote_threadsColumns+` FROM remote_threads
WHERE machine_id=? AND state NOT IN ('active','closed','stale') AND active_delegation_id=''
AND (?='' OR source_session_id='' OR source_session_id=?)
AND (?='' OR workspace_fingerprint='' OR workspace_fingerprint=?)
AND (expires_order='' OR expires_order>=?)
ORDER BY last_used_order DESC,id LIMIT ?`, strings.TrimSpace(machine), strings.TrimSpace(session), strings.TrimSpace(session), strings.TrimSpace(workspace), strings.TrimSpace(workspace), delegationTimeOrder(now.UTC().Format(time.RFC3339Nano)), limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			v, err := scanRemoteThread(rows)
			if err != nil {
				return err
			}
			threads = append(threads, v)
		}
		return rows.Err()
	})
	return threads, err
}
