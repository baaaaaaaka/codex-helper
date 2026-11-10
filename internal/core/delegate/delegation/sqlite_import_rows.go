package delegation

import (
	"context"
	"database/sql"
)

// These prepared statements exist only during a single atomic upgrade.
type delegationImportStatements struct {
	records            *sql.Stmt
	routes             *sql.Stmt
	remote_threads     *sql.Stmt
	executions         *sql.Stmt
	outbox             *sql.Stmt
	inbox_cursors      *sql.Stmt
	inbox_backoffs     *sql.Stmt
	record_path        *sql.Stmt
	record_constraints *sql.Stmt
	record_actions     *sql.Stmt
	record_context     *sql.Stmt
	record_artifacts   *sql.Stmt
}

func prepareDelegationImport(ctx context.Context, tx *sql.Tx) (*delegationImportStatements, error) {
	w := &delegationImportStatements{}
	for _, s := range []struct {
		target **sql.Stmt
		query  string
	}{

		{&w.records, recordsInsertSQL},
		{&w.routes, routesInsertSQL},
		{&w.remote_threads, remote_threadsInsertSQL},
		{&w.executions, executionsInsertSQL},
		{&w.outbox, outboxInsertSQL},
		{&w.inbox_cursors, inbox_cursorsInsertSQL},
		{&w.inbox_backoffs, inbox_backoffsInsertSQL},
		{&w.record_path, `INSERT INTO record_path(record_id,position,value) VALUES(?,?,?)`},
		{&w.record_constraints, `INSERT INTO record_constraints(record_id,position,value) VALUES(?,?,?)`},
		{&w.record_actions, `INSERT INTO record_actions(record_id,position,value) VALUES(?,?,?)`},
		{&w.record_context, `INSERT INTO record_context(record_id,position,value) VALUES(?,?,?)`},
		{&w.record_artifacts, `INSERT INTO record_artifacts(record_id,position,kind,name,path,hash) VALUES(?,?,?,?,?,?)`},
	} {
		var err error
		*s.target, err = tx.PrepareContext(ctx, s.query)
		if err != nil {
			w.Close()
			return nil, err
		}
	}
	return w, nil
}
func (w *delegationImportStatements) Close() {
	for _, s := range []*sql.Stmt{
		w.records, w.routes, w.remote_threads, w.executions, w.outbox, w.inbox_cursors, w.inbox_backoffs, w.record_path, w.record_constraints, w.record_actions, w.record_context, w.record_artifacts,
	} {
		if s != nil {
			_ = s.Close()
		}
	}
}
func (w *delegationImportStatements) Record(ctx context.Context, v Record) error {
	if _, err := w.records.ExecContext(ctx, recordsArgs(v)...); err != nil {
		return err
	}

	for i, value := range v.Path {
		if _, err := w.record_path.ExecContext(ctx, v.RecordID, i, value); err != nil {
			return err
		}
	}

	for i, value := range v.Spec.Constraints {
		if _, err := w.record_constraints.ExecContext(ctx, v.RecordID, i, value); err != nil {
			return err
		}
	}

	for i, value := range v.Spec.AllowedActions {
		if _, err := w.record_actions.ExecContext(ctx, v.RecordID, i, value); err != nil {
			return err
		}
	}

	for i, value := range v.Spec.Context {
		if _, err := w.record_context.ExecContext(ctx, v.RecordID, i, value); err != nil {
			return err
		}
	}

	for i, a := range v.Spec.ArtifactRefs {
		if _, err := w.record_artifacts.ExecContext(ctx, v.RecordID, i, a.Kind, a.Name, a.Path, a.Hash); err != nil {
			return err
		}
	}
	return nil
}
