package store

// RecoveryEvidence is the latest recovery checkpoint for a request. Unknown
// acceptance is deliberately distinct from rejection. One indexed row per turn
// bounds storage and avoids rewriting the conversation's history on each probe.
type RecoveryEvidence struct {
	TurnID                       int64
	Attempt                      int
	ObservedAt                   int64
	Source, ErrorCode            string
	Observation, Acceptance      string
	RetryOwner, Decision, Reason string
	ThreadID, CodexTurnID        string
	RetryDeadline, NextAttemptAt int64

	CancelRequested bool
	ResumeCount     int
	ResumeDeadline  int64
	ResumePrompt    string
}

func (t *Tx) RecordRecovery(r RecoveryEvidence) error {
	_, err := t.exec(`INSERT INTO turn_recovery
 (turn_id,attempt,observed_at,source,error_code,observation,acceptance,retry_owner,decision,reason,thread_id,codex_turn_id,retry_deadline,next_attempt_at)
 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(turn_id) DO UPDATE SET
 attempt=excluded.attempt,observed_at=excluded.observed_at,source=excluded.source,error_code=excluded.error_code,
 observation=excluded.observation,acceptance=excluded.acceptance,retry_owner=excluded.retry_owner,
 decision=excluded.decision,reason=excluded.reason,thread_id=excluded.thread_id,
 codex_turn_id=excluded.codex_turn_id,retry_deadline=excluded.retry_deadline,next_attempt_at=excluded.next_attempt_at`,
		r.TurnID, r.Attempt, t.now, r.Source, r.ErrorCode, r.Observation, r.Acceptance, r.RetryOwner, r.Decision, r.Reason, r.ThreadID, r.CodexTurnID, r.RetryDeadline, r.NextAttemptAt)
	return err
}

func (t *Tx) Recovery(turnID int64) (RecoveryEvidence, bool, error) {
	return one(t.row(`SELECT turn_id,attempt,observed_at,source,error_code,observation,acceptance,retry_owner,decision,reason,thread_id,codex_turn_id,retry_deadline,next_attempt_at,resume_count,resume_deadline,resume_prompt,cancel_requested FROM turn_recovery WHERE turn_id=?`, turnID), func(s scanner) (RecoveryEvidence, error) {
		var r RecoveryEvidence
		err := s.Scan(&r.TurnID, &r.Attempt, &r.ObservedAt, &r.Source, &r.ErrorCode, &r.Observation, &r.Acceptance, &r.RetryOwner, &r.Decision, &r.Reason, &r.ThreadID, &r.CodexTurnID, &r.RetryDeadline, &r.NextAttemptAt, &r.ResumeCount, &r.ResumeDeadline, &r.ResumePrompt, &r.CancelRequested)
		return r, err
	})
}

// PlanRecoveryContinuation is committed with the requeue, so budgets and the
// exact recovery instruction survive both a crash and later probe snapshots.
func (t *Tx) PlanRecoveryContinuation(turnID int64, count int, deadline int64, prompt string) error {
	_, err := t.exec(`UPDATE turn_recovery SET resume_count=?,resume_deadline=?,resume_prompt=? WHERE turn_id=?`, count, deadline, prompt, turnID)
	return err
}

// RequestRecoveryCancel preserves the cancellation across a helper crash even
// when the native interrupt acknowledgement has not yet arrived.
func (t *Tx) RequestRecoveryCancel(turnID int64) error {
	tr, ok, err := t.Turn(turnID)
	if err != nil || !ok {
		return err
	}
	if terminalTurnState(tr.State) {
		return nil
	}
	if err := t.RecordRecovery(RecoveryEvidence{TurnID: tr.ID, Attempt: tr.Attempt, Source: "user_cancel", Decision: "cancel", Acceptance: "unknown", RetryOwner: "none", ThreadID: tr.ThreadID, CodexTurnID: tr.CodexTurnID}); err != nil {
		return err
	}
	_, err = t.exec(`UPDATE turn_recovery SET cancel_requested=1 WHERE turn_id=?`, turnID)
	return err
}
