package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// MetaTurnKeyPrefix is the meta key holding the turn key prefix
// ('cxp-<scope8>'). A random prefix is created on first use.
const MetaTurnKeyPrefix = "turn_key_prefix"

func (t *Tx) turnKeyPrefix() (string, error) {
	v, ok, err := t.Meta(MetaTurnKeyPrefix)
	if err != nil || ok {
		return v, err
	}
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	v = "cxp-" + hex.EncodeToString(b[:])
	return v, t.SetMeta(MetaTurnKeyPrefix, v)
}

// newTurn inserts a queued turn for an inbound prompt, capturing the model
// and effort that apply at enqueue time (R-31). The turn key is
// '<prefix>-<id>', at most ~32 characters, and is sent to Codex as
// clientUserMessageId.
func (t *Tx) newTurn(convID, inboundID int64) (int64, error) {
	prefix, err := t.turnKeyPrefix()
	if err != nil {
		return 0, err
	}
	var id int64
	if err := t.row(`SELECT COALESCE(MAX(id), 0) + 1 FROM turns`).Scan(&id); err != nil {
		return 0, err
	}
	_, err = t.exec(`INSERT INTO turns(id,conv_id,inbound_id,turn_key,state,model,effort,queued_at)
SELECT ?, ?, ?, ?, 'queued', th.m, th.e, ?
FROM (SELECT 1) LEFT JOIN (SELECT COALESCE(pending_model, model) AS m, COALESCE(pending_effort, effort) AS e
                           FROM threads WHERE conv_id = ?) th`,
		id, convID, nullInt(inboundID), fmt.Sprintf("%s-%d", prefix, id), t.now, convID)
	return id, err
}

// Turn reads one turn.
func (t *Tx) Turn(id int64) (Turn, bool, error) {
	return one(t.row(`SELECT `+turnCols+` FROM turns WHERE id = ?`, id), scanTurn)
}

// OpenTurnsOf lists the open turns of a conversation in order.
func (t *Tx) OpenTurnsOf(convID int64) ([]Turn, error) {
	return collect(scanTurn)(t.rows(`SELECT `+turnCols+` FROM turns
WHERE conv_id = ? AND state IN ('queued','starting','running') ORDER BY id`, convID))
}

// NextRunnable returns the head queued turn of an active, unpaused
// conversation when no turn of that conversation is starting or running, no
// outbox row that must precede the turn is pending, and the prompt's
// attachments are not pending.
//
// The rows that must precede it are an earlier turn's replies, final and
// interruption notice (keys turn:<id>:… with id below the head's) and rows
// of no turn. ACKs and rows of the head or later turns (their reminders
// and start notices) do not hold it back: under paced writes they would
// otherwise stall the queue for the time the ACK backlog takes to drain.
func (t *Tx) NextRunnable(convID int64) (Turn, bool, error) {
	c, ok, err := t.Conversation(convID)
	if err != nil || !ok || c.State != ConvActive || c.PausedAt != 0 {
		return Turn{}, false, err
	}
	open, err := t.OpenTurnsOf(convID)
	if err != nil || len(open) == 0 {
		return Turn{}, false, err
	}
	head := open[0]
	for _, tr := range open {
		if tr.State != TurnQueued {
			return Turn{}, false, nil
		}
	}
	var blocked int
	if err := t.row(`SELECT EXISTS(SELECT 1 FROM outbox WHERE conv_id = ? AND state IN ('queued','sending','unknown')
AND kind <> 'ack' AND NOT (delivery_key LIKE 'turn:%' AND CAST(substr(delivery_key, 6) AS INTEGER) >= ?))`, convID, head.ID).Scan(&blocked); err != nil {
		return Turn{}, false, err
	}
	if blocked != 0 {
		return Turn{}, false, nil
	}
	if head.InboundID != 0 {
		var pending int
		if err := t.row(`SELECT EXISTS(SELECT 1 FROM inbound_files WHERE inbound_id = ? AND state = 'pending')`, head.InboundID).Scan(&pending); err != nil {
			return Turn{}, false, err
		}
		if pending != 0 {
			return Turn{}, false, nil
		}
	}
	return head, true, nil
}

func (t *Tx) turnMove(id int64, from []string, set string, args ...any) error {
	q := `UPDATE turns SET ` + set + ` WHERE id = ? AND state IN (` + placeholders(len(from)) + `)`
	args = append(args, id)
	for _, s := range from {
		args = append(args, s)
	}
	n, err := affected(t.exec(q, args...))
	if err != nil {
		return err
	}
	if n == 0 {
		tr, ok, err := t.Turn(id)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w: turn %d", ErrNotFound, id)
		}
		return fmt.Errorf("%w: turn %d is %s", ErrState, id, tr.State)
	}
	return nil
}

// MarkStarting records the start attempt before turn/start is called.
func (t *Tx) MarkStarting(turnID int64, threadID string) error {
	return t.turnMove(turnID, []string{TurnQueued},
		`state = 'starting', thread_id = ?, attempt = attempt + 1, started_at = ?`, nullStr(threadID), t.now)
}

// MarkRunning records Codex's acceptance of the turn.
func (t *Tx) MarkRunning(turnID int64, codexTurnID string) error {
	return t.turnMove(turnID, []string{TurnStarting}, `state = 'running', codex_turn_id = ?`, nullStr(codexTurnID))
}

// RequeueTurn puts a starting/running turn back to queued for a capacity
// retry. retryDeadline is only recorded the first time, so the retry window
// is measured from the first capacity error.
func (t *Tx) RequeueTurn(turnID int64, retryDeadline int64) error {
	return t.turnMove(turnID, []string{TurnStarting, TurnRunning},
		`state = 'queued', retry_deadline = COALESCE(retry_deadline, ?)`, nullInt(retryDeadline))
}

// RequeueTurnNewWindow is RequeueTurn for a capacity failure after the
// attempt made progress: the retry window starts again at retryDeadline
// (the 5-minute bound limits waiting for capacity, not a long task).
func (t *Tx) RequeueTurnNewWindow(turnID int64, retryDeadline int64) error {
	return t.turnMove(turnID, []string{TurnStarting, TurnRunning},
		`state = 'queued', retry_deadline = ?`, nullInt(retryDeadline))
}

func terminalTurnState(s string) bool {
	switch s {
	case TurnCompleted, TurnFailed, TurnInterrupted, TurnNotStarted, TurnCancelled:
		return true
	}
	return false
}

// FinishTurn ends an open turn and enqueues the messages it produced in the
// same transaction. The prompt's inbound row becomes done.
func (t *Tx) FinishTurn(turnID int64, state, errText string, outputs []OutMsg) error {
	if !terminalTurnState(state) {
		return fmt.Errorf("%w: %q is not a terminal turn state", ErrState, state)
	}
	tr, ok, err := t.Turn(turnID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: turn %d", ErrNotFound, turnID)
	}
	if err := t.turnMove(turnID, []string{TurnQueued, TurnStarting, TurnRunning},
		`state = ?, finished_at = ?, error = ?`, state, t.now, nullStr(errText)); err != nil {
		return err
	}
	if tr.InboundID != 0 {
		if _, err := t.exec(`UPDATE inbound SET state = 'done' WHERE id = ? AND state IN ('queued','held')`, tr.InboundID); err != nil {
			return err
		}
	}
	_, err = t.Enqueue(tr.ConvID, outputs)
	return err
}

// OpenTurns lists every starting/running/queued turn (startup
// reconciliation), by conversation, in order.
func (s *Store) OpenTurns() ([]Turn, error) {
	return collect(scanTurn)(s.r.QueryContext(context.Background(), `SELECT `+turnCols+` FROM turns
WHERE state IN ('queued','starting','running') ORDER BY conv_id, id`))
}
