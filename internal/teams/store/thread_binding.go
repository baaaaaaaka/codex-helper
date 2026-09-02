package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// CodexThreadStartBindingRequest is the fencing material captured immediately
// before a managed AppServer starts a first turn on a newly-created thread.
// The request is deliberately narrow: no Codex turn id is persisted here.
type CodexThreadStartBindingRequest struct {
	SessionID       string
	TurnID          string
	ThreadID        string
	ModelGeneration int
	MachineID       string
	LeaseGeneration int64
	Owner           OwnerMetadata
}

type CodexThreadStartBindingResult struct {
	Session SessionContext
	Turn    Turn
	Changed bool
}

// ErrCodexThreadStartBindingOwnerFence identifies an execution that lost its
// service-owner or control-lease fence while thread/start was in flight.
var ErrCodexThreadStartBindingOwnerFence = errors.New("Codex thread binding owner fence rejected")

// CodexThreadStartBindingFenceError is a fail-closed pre-dispatch rejection.
// Owner fencing errors additionally match ErrCodexThreadStartBindingOwnerFence;
// lifecycle and generation errors do not.
type CodexThreadStartBindingFenceError struct {
	SessionID string
	TurnID    string
	Reason    string
	Owner     bool
}

func (e *CodexThreadStartBindingFenceError) Error() string {
	if e == nil {
		return "Codex thread binding fence rejected"
	}
	detail := strings.TrimSpace(e.Reason)
	if detail == "" {
		detail = "durable session or turn fence changed"
	}
	return fmt.Sprintf("Codex thread binding fence rejected for session %s turn %s: %s", e.SessionID, e.TurnID, detail)
}

func (e *CodexThreadStartBindingFenceError) Is(target error) bool {
	return e != nil && e.Owner && (target == ErrCodexThreadStartBindingOwnerFence || target == ErrControlLeaseNotHeld)
}

type codexThreadStartBindingCommitError struct{ Err error }

func (e *codexThreadStartBindingCommitError) Error() string {
	if e == nil || e.Err == nil {
		return "Codex thread binding commit outcome is uncertain"
	}
	return "Codex thread binding commit outcome is uncertain: " + e.Err.Error()
}

func (e *codexThreadStartBindingCommitError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (r CodexThreadStartBindingRequest) normalized() (CodexThreadStartBindingRequest, error) {
	r.SessionID = strings.TrimSpace(r.SessionID)
	r.TurnID = strings.TrimSpace(r.TurnID)
	r.ThreadID = strings.TrimSpace(r.ThreadID)
	r.MachineID = strings.TrimSpace(r.MachineID)
	if r.SessionID == "" {
		return r, fmt.Errorf("session id is required")
	}
	if r.TurnID == "" {
		return r, fmt.Errorf("turn id is required")
	}
	if r.ThreadID == "" {
		return r, fmt.Errorf("codex thread id is required")
	}
	if r.LeaseGeneration < 0 {
		return r, fmt.Errorf("lease generation cannot be negative")
	}
	return r, nil
}

// BindCodexThreadForRunningTurn atomically binds a newly-created durable
// Codex thread to both its active Teams session and running turn. It never
// falls back to a session-only update: a missing, mismatched, terminal, or
// unfenced turn rejects the dispatch.
func (s *Store) BindCodexThreadForRunningTurn(ctx context.Context, request CodexThreadStartBindingRequest) (CodexThreadStartBindingResult, error) {
	request, err := request.normalized()
	if err != nil {
		return CodexThreadStartBindingResult{}, err
	}
	if result, handled, err := s.bindCodexThreadForRunningTurnSQLite(ctx, request); handled || err != nil {
		if err != nil {
			var commitErr *codexThreadStartBindingCommitError
			if errors.As(err, &commitErr) {
				if confirmed, ok, confirmErr := s.confirmCodexThreadForRunningTurnSQLite(ctx, request); confirmErr == nil && ok {
					confirmed.Changed = true
					return confirmed, nil
				}
			}
		}
		return result, err
	}

	result, err := s.bindCodexThreadForRunningTurnJSON(ctx, request)
	if err != nil {
		var commitErr *codexThreadStartBindingCommitError
		if errors.As(err, &commitErr) {
			if confirmed, ok, confirmErr := s.confirmCodexThreadForRunningTurnJSON(ctx, request); confirmErr == nil && ok {
				confirmed.Changed = true
				return confirmed, nil
			}
		}
	}
	return result, err
}

func (s *Store) bindCodexThreadForRunningTurnJSON(ctx context.Context, request CodexThreadStartBindingRequest) (CodexThreadStartBindingResult, error) {
	var result CodexThreadStartBindingResult
	err := s.withSessionLock(ctx, request.SessionID, func() error {
		return s.withStateLock(ctx, func() error {
			state, err := s.loadUnlocked(ctx)
			if err != nil {
				return err
			}
			session, sessionOK := state.Sessions[request.SessionID]
			turn, turnOK := state.Turns[request.TurnID]
			owner := state.ownerForThreadBinding()
			if err := validateCodexThreadStartBinding(session, sessionOK, turn, turnOK, state.ControlLease, owner, request, time.Now()); err != nil {
				return err
			}
			result.Session = session
			result.Turn = turn
			if strings.TrimSpace(session.CodexThreadID) == request.ThreadID && strings.TrimSpace(turn.CodexThreadID) == request.ThreadID {
				return errStoreNoChange
			}
			now := time.Now()
			session.CodexThreadID = request.ThreadID
			session.UpdatedAt = now
			turn.CodexThreadID = request.ThreadID
			turn.UpdatedAt = now
			// The callback bind is the first durable proof of the isolated live
			// branch. Record it in the same state transaction so a helper crash
			// after binding cannot make the next queued message create another
			// fresh branch while the old history anchor remains unresolved.
			recordLiveBranchThreadLocked(&state, turn, request.ThreadID, now)
			if state.Sessions == nil {
				state.Sessions = make(map[string]SessionContext)
			}
			if state.Turns == nil {
				state.Turns = make(map[string]Turn)
			}
			state.Sessions[request.SessionID] = session
			state.Turns[request.TurnID] = turn
			result.Session = session
			result.Turn = turn
			result.Changed = true
			state.ensure(now)
			if err := s.saveUnlocked(state); err != nil {
				s.invalidateMessageLookupCacheLocked()
				return &codexThreadStartBindingCommitError{Err: err}
			}
			if s.messageLookup.Valid {
				s.replaceMessageLookupCacheFromStateLocked(state)
			}
			return nil
		})
	})
	if errors.Is(err, errStoreNoChange) {
		err = nil
	}
	return result, err
}

func (state State) ownerForThreadBinding() *OwnerMetadata {
	if state.ServiceOwner != nil {
		owner := *state.ServiceOwner
		return &owner
	}
	if state.LockOwner != nil {
		owner := *state.LockOwner
		return &owner
	}
	return nil
}

func validateCodexThreadStartBinding(session SessionContext, sessionOK bool, turn Turn, turnOK bool, lease ControlLease, owner *OwnerMetadata, request CodexThreadStartBindingRequest, now time.Time) error {
	if err := validateKnownControlLeaseStatus(lease); err != nil {
		return err
	}
	if !sessionOK || strings.TrimSpace(session.ID) == "" {
		return &CodexThreadStartBindingFenceError{SessionID: request.SessionID, TurnID: request.TurnID, Reason: "session does not exist"}
	}
	if !sessionStatusIsActive(session.Status) {
		return &CodexThreadStartBindingFenceError{SessionID: request.SessionID, TurnID: request.TurnID, Reason: "session is no longer active"}
	}
	if !turnOK || strings.TrimSpace(turn.ID) == "" {
		return &CodexThreadStartBindingFenceError{SessionID: request.SessionID, TurnID: request.TurnID, Reason: "turn does not exist"}
	}
	if strings.TrimSpace(turn.SessionID) != request.SessionID {
		return &CodexThreadStartBindingFenceError{SessionID: request.SessionID, TurnID: request.TurnID, Reason: "turn belongs to another session"}
	}
	if turn.Status != TurnStatusRunning {
		return &CodexThreadStartBindingFenceError{SessionID: request.SessionID, TurnID: request.TurnID, Reason: "turn is not running"}
	}
	if session.ModelGeneration != turn.ModelGeneration || session.ModelGeneration != request.ModelGeneration {
		return &CodexThreadStartBindingFenceError{SessionID: request.SessionID, TurnID: request.TurnID, Reason: "model generation changed"}
	}
	if request.MachineID != "" && turn.MachineID != "" && strings.TrimSpace(turn.MachineID) != request.MachineID {
		return &CodexThreadStartBindingFenceError{SessionID: request.SessionID, TurnID: request.TurnID, Reason: "turn machine changed"}
	}
	if request.LeaseGeneration > 0 && turn.LeaseGeneration > 0 && turn.LeaseGeneration != request.LeaseGeneration {
		return &CodexThreadStartBindingFenceError{SessionID: request.SessionID, TurnID: request.TurnID, Reason: "turn lease generation changed", Owner: true}
	}
	if lease.Generation > 0 || strings.TrimSpace(lease.HolderMachineID) != "" || request.LeaseGeneration > 0 || request.MachineID != "" {
		if request.LeaseGeneration <= 0 || request.MachineID == "" {
			return &CodexThreadStartBindingFenceError{SessionID: request.SessionID, TurnID: request.TurnID, Reason: "incomplete control lease fence", Owner: true}
		}
		if lease.HolderMachineID != request.MachineID || lease.Generation != request.LeaseGeneration || !lease.LeaseUntil.After(now) {
			return &CodexThreadStartBindingFenceError{SessionID: request.SessionID, TurnID: request.TurnID, Reason: "control lease is no longer held", Owner: true}
		}
		if owner == nil || !sameOwnerInstance(*owner, request.Owner) || strings.TrimSpace(owner.MachineID) != request.MachineID || owner.LeaseGeneration != request.LeaseGeneration {
			return &CodexThreadStartBindingFenceError{SessionID: request.SessionID, TurnID: request.TurnID, Reason: "service owner instance changed", Owner: true}
		}
		if strings.TrimSpace(owner.ActiveSessionID) != request.SessionID || strings.TrimSpace(owner.ActiveTurnID) != request.TurnID {
			return &CodexThreadStartBindingFenceError{SessionID: request.SessionID, TurnID: request.TurnID, Reason: "service owner is serving another turn", Owner: true}
		}
	}
	if existing := strings.TrimSpace(session.CodexThreadID); existing != "" && existing != request.ThreadID && !turn.StartNewCodexThread {
		return CodexThreadBindingConflictError{SessionID: request.SessionID, Existing: existing, Observed: request.ThreadID}
	}
	if existing := strings.TrimSpace(turn.CodexThreadID); existing != "" && existing != request.ThreadID && !turn.StartNewCodexThread {
		return CodexThreadBindingConflictError{SessionID: request.SessionID, Existing: existing, Observed: request.ThreadID}
	}
	return nil
}

func (s *Store) confirmCodexThreadForRunningTurnJSON(ctx context.Context, request CodexThreadStartBindingRequest) (CodexThreadStartBindingResult, bool, error) {
	state, err := s.Load(ctx)
	if err != nil {
		return CodexThreadStartBindingResult{}, false, err
	}
	session, sessionOK := state.Sessions[request.SessionID]
	turn, turnOK := state.Turns[request.TurnID]
	if sessionOK && turnOK && session.CodexThreadID == request.ThreadID && turn.CodexThreadID == request.ThreadID {
		return CodexThreadStartBindingResult{Session: session, Turn: turn, Changed: true}, true, nil
	}
	return CodexThreadStartBindingResult{}, false, nil
}

func loadCodexThreadBindingRuntimeTx(ctx context.Context, tx *sql.Tx) (ControlLease, *OwnerMetadata, error) {
	lease, err := loadSQLiteControlLease(ctx, tx)
	if err != nil {
		return ControlLease{}, nil, err
	}
	serviceOwner, _, err := loadSQLiteJSONRow[*OwnerMetadata](ctx, tx, `SELECT json FROM runtime_state WHERE key = ?`, sqliteRuntimeKeyServiceOwner)
	if err != nil {
		return ControlLease{}, nil, err
	}
	if serviceOwner != nil {
		return lease, serviceOwner, nil
	}
	lockOwner, _, err := loadSQLiteJSONRow[*OwnerMetadata](ctx, tx, `SELECT json FROM runtime_state WHERE key = ?`, sqliteRuntimeKeyLockOwner)
	if err != nil {
		return ControlLease{}, nil, err
	}
	return lease, lockOwner, nil
}
