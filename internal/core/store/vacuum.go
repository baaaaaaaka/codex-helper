package store

import (
	"context"
	"fmt"
)

// Vacuum returns genuinely free pages (WAL churn, index splits) to the OS
// with PRAGMA incremental_vacuum. It never deletes or blanks a row: there is
// no retention (§6, owner decision 2026-11-04). Every key is kept forever, so
// an old Teams message whose lastModified is bumped by an edit or reaction is
// still recognised and never re-run.
func (s *Store) Vacuum() error {
	if s.ls != nil {
		if err := s.ls.Check(); err != nil {
			return leaseErr(err)
		}
	}
	_, err := s.w.ExecContext(context.Background(), `PRAGMA incremental_vacuum`)
	return err
}

// VacuumStep is Vacuum bounded to at most pages free pages, so one call
// never costs more than a few milliseconds however large the free list is.
func (s *Store) VacuumStep(pages int) error {
	if pages <= 0 {
		pages = 1
	}
	if s.ls != nil {
		if err := s.ls.Check(); err != nil {
			return leaseErr(err)
		}
	}
	_, err := s.w.ExecContext(context.Background(), fmt.Sprintf(`PRAGMA incremental_vacuum(%d)`, pages))
	return err
}
