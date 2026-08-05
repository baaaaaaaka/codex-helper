package store

import (
	"context"
	"time"
)

func (s *Store) bindCodexThreadForRunningTurnSQLite(ctx context.Context, request CodexThreadStartBindingRequest) (CodexThreadStartBindingResult, bool, error) {
	var result CodexThreadStartBindingResult
	handled := false
	err := s.withSessionLock(ctx, request.SessionID, func() error {
		return s.withStateLock(ctx, func() error {
			pointer, ok, err := s.currentSQLitePointerUnlocked()
			if err != nil || !ok {
				return err
			}
			handled = true
			db, err := s.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()

			session, sessionOK, err := loadSQLiteJSONRow[SessionContext](ctx, tx, `SELECT json FROM sessions WHERE id = ?`, request.SessionID)
			if err != nil {
				return err
			}
			turn, turnOK, err := loadSQLiteJSONRow[Turn](ctx, tx, `SELECT json FROM turns WHERE id = ?`, request.TurnID)
			if err != nil {
				return err
			}
			lease, owner, err := loadCodexThreadBindingRuntimeTx(ctx, tx)
			if err != nil {
				return err
			}
			if err := validateCodexThreadStartBinding(session, sessionOK, turn, turnOK, lease, owner, request, nowForStore()); err != nil {
				return err
			}

			result.Session = session
			result.Turn = turn
			if session.CodexThreadID == request.ThreadID && turn.CodexThreadID == request.ThreadID {
				return tx.Commit()
			}
			now := nowForStore()
			session.CodexThreadID = request.ThreadID
			session.UpdatedAt = now
			turn.CodexThreadID = request.ThreadID
			turn.UpdatedAt = now
			if err := upsertSQLiteSessionTx(ctx, tx, session); err != nil {
				return err
			}
			if err := upsertSQLiteTurnTx(ctx, tx, turn); err != nil {
				return err
			}
			if err := tx.Commit(); err != nil {
				return &codexThreadStartBindingCommitError{Err: err}
			}
			result.Session = session
			result.Turn = turn
			result.Changed = true
			return nil
		})
	})
	return result, handled, err
}

func (s *Store) confirmCodexThreadForRunningTurnSQLite(ctx context.Context, request CodexThreadStartBindingRequest) (CodexThreadStartBindingResult, bool, error) {
	var result CodexThreadStartBindingResult
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		handled = true
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		session, sessionOK, err := loadSQLiteJSONRow[SessionContext](ctx, db, `SELECT json FROM sessions WHERE id = ?`, request.SessionID)
		if err != nil {
			return err
		}
		turn, turnOK, err := loadSQLiteJSONRow[Turn](ctx, db, `SELECT json FROM turns WHERE id = ?`, request.TurnID)
		if err != nil {
			return err
		}
		if sessionOK && turnOK && session.CodexThreadID == request.ThreadID && turn.CodexThreadID == request.ThreadID {
			result = CodexThreadStartBindingResult{Session: session, Turn: turn, Changed: true}
			return nil
		}
		return nil
	})
	if err != nil || !handled {
		return CodexThreadStartBindingResult{}, false, err
	}
	return result, result.Session.ID != "" && result.Turn.ID != "", nil
}

func nowForStore() time.Time { return time.Now() }
