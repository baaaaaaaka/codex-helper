package store

import (
	"context"
	"fmt"
	"strings"
)

// Additive read and write helpers used by the engine (ENGINE-REPORT,
// "store additions"). None changes the schema or an existing method.

// OutboxByHash lists the sending/sent/unknown rows of a conversation whose
// body hash equals hash and that were created at or after since. The
// engine uses it to tell its own echo (sending/unknown, or sent with the
// same channel id) from a copy of a sent message under another id.
func (t *Tx) OutboxByHash(convID int64, hash []byte, since int64) ([]Outbox, error) {
	if len(hash) == 0 {
		return nil, nil
	}
	return collect(scanOutbox)(t.rows(`SELECT `+outboxCols+` FROM outbox
WHERE conv_id = ? AND body_hash = ? AND state IN ('sending','sent','unknown') AND created_at >= ? AND hash_algo IS NULL ORDER BY seq`,
		convID, hash, since))
}

// OutboxByToken finds the row carrying an attempt token (marker read-back).
func (t *Tx) OutboxByToken(convID int64, token string) (Outbox, bool, error) {
	return one(t.row(`SELECT `+outboxCols+` FROM outbox WHERE conv_id = ? AND attempt_token = ?`, convID, token), scanOutbox)
}

// Delivered reports whether any outbox row's delivery key starts with
// prefix (per-record mirror dedupe across the migration boundary,
// STORE-DESIGN §3.9). It is an index range scan on the unique key.
func (t *Tx) Delivered(prefix string) (bool, error) {
	var hit int
	err := t.row(`SELECT EXISTS(SELECT 1 FROM outbox WHERE delivery_key >= ? AND delivery_key < ?)`,
		prefix, prefix+"\U0010FFFF").Scan(&hit)
	return hit != 0, err
}

// DroppedUnder reports whether an outbox row whose delivery key starts with
// prefix was dropped (never confirmed delivered). kinds, when given, limits
// the rows considered to those kinds.
func (t *Tx) DroppedUnder(prefix string, kinds ...string) (bool, error) {
	q := `SELECT EXISTS(SELECT 1 FROM outbox WHERE delivery_key >= ? AND delivery_key < ? AND state = 'dropped'`
	args := []any{prefix, prefix + "\U0010FFFF"}
	if len(kinds) > 0 {
		q += ` AND kind IN (?` + strings.Repeat(`,?`, len(kinds)-1) + `)`
		for _, k := range kinds {
			args = append(args, k)
		}
	}
	var hit int
	err := t.row(q+`)`, args...).Scan(&hit)
	return hit != 0, err
}

// RetryTurn creates a new queued turn for an inbound prompt whose earlier
// turn ended (bare `helper retry`). The inbound row keeps its state; the
// new turn is linked to it.
func (t *Tx) RetryTurn(convID, inboundID int64) (int64, error) {
	in, ok, err := t.Inbound(inboundID)
	if err != nil {
		return 0, err
	}
	if !ok || in.ConvID != convID {
		return 0, fmt.Errorf("%w: inbound %d in conversation %d", ErrNotFound, inboundID, convID)
	}
	return t.newTurn(convID, inboundID)
}

// RecentTurns lists the newest turns of a conversation, newest first.
func (t *Tx) RecentTurns(convID int64, limit int) ([]Turn, error) {
	return collect(scanTurn)(t.rows(`SELECT `+turnCols+` FROM turns WHERE conv_id = ? ORDER BY id DESC LIMIT ?`, convID, limit))
}

// LastOutbox returns the newest outbox row of a conversation of one kind.
func (t *Tx) LastOutbox(convID int64, kind string) (Outbox, bool, error) {
	return one(t.row(`SELECT `+outboxCols+` FROM outbox WHERE conv_id = ? AND kind = ? ORDER BY seq DESC LIMIT 1`, convID, kind), scanOutbox)
}

// CodexTurnIDs lists the Codex turn ids of every turn the helper ran (the
// mirror skips those turns in rollouts that have no client id).
func (s *Store) CodexTurnIDs() ([]string, error) {
	rows, err := s.r.QueryContext(context.Background(), `SELECT codex_turn_id FROM turns WHERE codex_turn_id IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// OutboxCounts returns the number of outbox rows per state that are not
// sent yet (queued, sending, unknown; `teams service status`).
func (s *Store) OutboxCounts() (map[string]int, error) {
	rows, err := s.r.QueryContext(context.Background(), `SELECT state, COUNT(*) FROM outbox WHERE state IN ('queued','sending','unknown') GROUP BY state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[st] = n
	}
	return out, rows.Err()
}

// TurnCounts returns the number of turns per state (status and stats).
func (s *Store) TurnCounts() (map[string]int, error) {
	rows, err := s.r.QueryContext(context.Background(), `SELECT state, COUNT(*) FROM turns GROUP BY state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[st] = n
	}
	return out, rows.Err()
}

// QueuedTurnConvs lists the conversations that have a queued turn, in id
// order: the only ones the runner's safety sweep can start a turn in.
func (s *Store) QueuedTurnConvs() ([]int64, error) {
	// The first term matches the turns_open partial index.
	rows, err := s.r.QueryContext(context.Background(), `SELECT DISTINCT conv_id FROM turns
WHERE state IN ('queued','starting','running') AND state = 'queued' ORDER BY conv_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// RecentOwnerInbound lists the newest owner messages (prompts and
// commands) of a conversation, newest first (the control chat's Q&A
// history, gap batch B).
func (t *Tx) RecentOwnerInbound(convID int64, limit int) ([]Inbound, error) {
	return collect(scanInbound)(t.rows(`SELECT `+inboundCols+` FROM inbound
WHERE conv_id = ? AND sender = 'owner' AND kind IN ('prompt','command') ORDER BY id DESC LIMIT ?`, convID, limit))
}

// RecentSent lists the newest delivered outbox rows of a conversation,
// newest first (the control chat's Q&A history, gap batch B).
func (t *Tx) RecentSent(convID int64, limit int) ([]Outbox, error) {
	return collect(scanOutbox)(t.rows(`SELECT `+outboxCols+` FROM outbox
WHERE conv_id = ? AND state = 'sent' ORDER BY seq DESC LIMIT ?`, convID, limit))
}

// PendingOutboxConvs lists the conversations with outbox rows still to
// send (queued or sending; unknown rows wait for the resolver).
func (s *Store) PendingOutboxConvs() ([]int64, error) {
	rows, err := s.r.QueryContext(context.Background(), `SELECT DISTINCT conv_id FROM outbox WHERE state IN ('queued','sending') ORDER BY conv_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
