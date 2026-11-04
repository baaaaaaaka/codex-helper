package store

import "context"

// Vacuum returns genuinely free pages (WAL churn, index splits) to the OS
// with PRAGMA incremental_vacuum. It never deletes or blanks a row: there is
// no retention (§6, owner decision 2026-11-04). Every key is kept forever, so
// an old Teams message whose lastModified is bumped by an edit or reaction is
// still recognised and never re-run.
func (s *Store) Vacuum() error {
	_, err := s.w.ExecContext(context.Background(), `PRAGMA incremental_vacuum`)
	return err
}
