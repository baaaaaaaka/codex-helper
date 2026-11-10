package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

// This rollback-journal mailbox is independent of the core's leased WAL.
// Execution decisions remain in core CLI tables, not in this transport store.
type teamsExchangeStore struct{ db *sql.DB }

const teamsExchangeName = "cli-exchange.sqlite"

func teamsExchangePath(anchor string) string {
	return filepath.Join(filepath.Dir(anchor), teamsExchangeName)
}
func openTeamsExchange(anchor string, readonly bool) (*teamsExchangeStore, error) {
	path := teamsExchangePath(anchor)
	if !readonly {
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return nil, err
		}
	}
	db, err := sqlitestate.Open(path, readonly)
	if err != nil {
		return nil, err
	}
	if readonly {
		var v int
		err = db.QueryRow(`PRAGMA user_version`).Scan(&v)
		if err == nil && v != 1 {
			err = fmt.Errorf("unsupported CLI exchange schema %d", v)
		}
	} else {
		err = migrateTeamsExchange(db, anchor)
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	return &teamsExchangeStore{db: db}, nil
}
func (s *teamsExchangeStore) Close() error { return s.db.Close() }

const exchangeDDL = `CREATE TABLE requests(id TEXT PRIMARY KEY,op TEXT NOT NULL,selector TEXT NOT NULL,chat_id TEXT NOT NULL,history INTEGER NOT NULL,activate INTEGER NOT NULL,force INTEGER NOT NULL,file TEXT NOT NULL,message TEXT NOT NULL);
 CREATE TABLE results(id TEXT PRIMARY KEY,error TEXT NOT NULL,completed_at INTEGER NOT NULL,lines_present INTEGER NOT NULL);
 CREATE INDEX result_expiry ON results(completed_at,id);
 CREATE TABLE result_lines(request_id TEXT NOT NULL REFERENCES results(id) ON DELETE CASCADE,position INTEGER NOT NULL,text TEXT NOT NULL,PRIMARY KEY(request_id,position));
 CREATE TABLE workflow(id INTEGER PRIMARY KEY CHECK(id=1),revision TEXT NOT NULL,enabled INTEGER NOT NULL,file TEXT NOT NULL);
 CREATE TABLE exchange_imports(source_path TEXT PRIMARY KEY);`
const exchangeRequestInsert = `INSERT INTO requests VALUES(?,?,?,?,?,?,?,?,?)`

func exchangeRequestArgs(r engine.CLIRequest) []any {
	return []any{r.ID, r.Op, r.Selector, r.ChatID, r.History, r.Activate, r.Force, r.File, r.Message}
}
func (s *teamsExchangeStore) Submit(r engine.CLIRequest) error {
	_, err := s.db.Exec(exchangeRequestInsert, exchangeRequestArgs(r)...)
	return err
}
func (s *teamsExchangeStore) Request(id string) (r engine.CLIRequest, ok bool, err error) {
	err = s.db.QueryRow(`SELECT id,op,selector,chat_id,history,activate,force,file,message FROM requests WHERE id=?`, id).Scan(&r.ID, &r.Op, &r.Selector, &r.ChatID, &r.History, &r.Activate, &r.Force, &r.File, &r.Message)
	if errors.Is(err, sql.ErrNoRows) {
		return r, false, nil
	}
	return r, err == nil, err
}
func (s *teamsExchangeStore) IDs(after string) ([]string, error) {
	rows, err := s.db.Query(`SELECT id FROM requests WHERE id>? ORDER BY id LIMIT 128`, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
func exchangeResultInsert(tx *sql.Tx, id string, r engine.CLIResult, at int64) error {
	if _, err := tx.Exec(`INSERT INTO results VALUES(?,?,?,?)`, id, r.Error, at, r.Lines != nil); err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO result_lines VALUES(?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for i, line := range r.Lines {
		if _, err = stmt.Exec(id, i, line); err != nil {
			return err
		}
	}
	return nil
}
func (s *teamsExchangeStore) Complete(id string, r engine.CLIResult, at time.Time) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = exchangeResultInsert(tx, id, r, at.UnixMilli()); err != nil {
		return err
	}
	if _, err = tx.Exec(`DELETE FROM requests WHERE id=?`, id); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *teamsExchangeStore) Result(id string) (r engine.CLIResult, ok bool, err error) {
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return r, false, err
	}
	defer tx.Rollback()
	var present bool
	err = tx.QueryRow(`SELECT error,lines_present FROM results WHERE id=?`, id).Scan(&r.Error, &present)
	if errors.Is(err, sql.ErrNoRows) {
		return r, false, nil
	}
	if err != nil {
		return r, false, err
	}
	if present {
		r.Lines = []string{}
	}
	rows, err := tx.Query(`SELECT text FROM result_lines WHERE request_id=? ORDER BY position`, id)
	if err != nil {
		return r, false, err
	}
	for rows.Next() {
		var l string
		if err = rows.Scan(&l); err != nil {
			rows.Close()
			return r, false, err
		}
		r.Lines = append(r.Lines, l)
	}
	err = rows.Err()
	rows.Close()
	return r, err == nil, err
}
func (s *teamsExchangeStore) Ack(id string) error {
	_, err := s.db.Exec(`DELETE FROM results WHERE id=?`, id)
	return err
}
func (s *teamsExchangeStore) Pending() (bool, error) {
	var yes bool
	err := s.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM requests)`).Scan(&yes)
	return yes, err
}
func (s *teamsExchangeStore) Expire(before time.Time) ([]string, error) {
	// A quiet watcher performs no write transaction or journal work.
	var any bool
	if err := s.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM results WHERE completed_at<?)`, before.UnixMilli()).Scan(&any); err != nil {
		return nil, err
	}
	if !any {
		return nil, nil
	}
	rows, err := s.db.Query(`DELETE FROM results WHERE id IN(SELECT id FROM results WHERE completed_at<? ORDER BY completed_at,id LIMIT 128) RETURNING id`, before.UnixMilli())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
func (s *teamsExchangeStore) Workflow() (r teamsWorkflowRequest, rev string, ok bool, err error) {
	err = s.db.QueryRow(`SELECT revision,enabled,file FROM workflow WHERE id=1`).Scan(&rev, &r.Enabled, &r.WebhookURLFile)
	if errors.Is(err, sql.ErrNoRows) {
		return r, "", false, nil
	}
	return r, rev, err == nil, err
}
func (s *teamsExchangeStore) SetWorkflow(rev string, r teamsWorkflowRequest) error {
	_, err := s.db.Exec(`INSERT INTO workflow VALUES(1,?,?,?) ON CONFLICT(id) DO UPDATE SET revision=excluded.revision,enabled=excluded.enabled,file=excluded.file`, rev, r.Enabled, r.WebhookURLFile)
	return err
}
func (s *teamsExchangeStore) ClearWorkflow(rev string) error {
	_, err := s.db.Exec(`DELETE FROM workflow WHERE id=1 AND revision=?`, rev)
	return err
}
