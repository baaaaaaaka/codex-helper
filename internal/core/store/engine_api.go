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
WHERE conv_id = ? AND body_hash = ? AND state IN ('sending','sent','unknown') AND created_at >= ? AND hash_algo IS NULL AND (batch_head IS NULL OR batch_head=id) ORDER BY seq`,
		convID, hash, since))
}

// OutboxByToken finds the row carrying an attempt token (marker read-back).
func (t *Tx) OutboxByToken(convID int64, token string) (Outbox, bool, error) {
	return one(t.row(`SELECT `+outboxCols+` FROM outbox WHERE conv_id = ? AND attempt_token = ? ORDER BY seq LIMIT 1`, convID, token), scanOutbox)
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
	rows, err := s.r.QueryContext(context.Background(), `SELECT codex_turn_id FROM turns WHERE codex_turn_id IS NOT NULL AND steering=0`)
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
	return s.countByState(`SELECT state, COUNT(*) FROM outbox WHERE state IN ('queued','sending','unknown') GROUP BY state`)
}

// TurnCounts returns the number of turns per state (stats).
func (s *Store) TurnCounts() (map[string]int, error) {
	return s.countByState(`SELECT state, COUNT(*) FROM turns GROUP BY state`)
}

// OpenTurnCounts returns the number of turns per state that are not
// finished (queued, starting, running; status). It reads only the
// turns_open index, not the whole table.
func (s *Store) OpenTurnCounts() (map[string]int, error) {
	return s.countByState(`SELECT state, COUNT(*) FROM turns WHERE state IN ('queued','starting','running') GROUP BY state`)
}

func (s *Store) countByState(query string) (map[string]int, error) {
	rows, err := s.r.QueryContext(context.Background(), query)
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
	rows, err := s.r.QueryContext(context.Background(), `SELECT DISTINCT conv_id FROM outbox WHERE state IN ('queued','sending','unknown') AND state IN ('queued','sending') ORDER BY conv_id`)
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

// LastSeq is the seq of conv's last outbox row (0: none).
func (t *Tx) LastSeq(conv int64) (int64, error) {
	var n int64
	err := t.row(`SELECT COALESCE(MAX(seq), 0) FROM outbox WHERE conv_id = ?`, conv).Scan(&n)
	return n, err
}

// UrgentConvs lists the conversations with a pending row of a kind the
// owner waits for: any kind but mirrored messages and progress (an ACK, a
// final, a notice). The sender ranks such a conversation by that row, also
// when an older mirrored or progress row is its head: rows go out in order,
// so a stale progress post must not hold an answer behind a catch-up
// backlog. The first term matches the outbox_pending partial index.
func (t *Tx) UrgentConvs() (map[int64]bool, error) {
	rows, err := t.rows(`SELECT DISTINCT conv_id FROM outbox
WHERE state IN ('queued','sending','unknown') AND state = 'queued' AND kind NOT IN ('mirror','progress')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]bool{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = true
	}
	return out, rows.Err()
}

// OutboxByKeyPrefix lists the outbox rows whose delivery key starts with
// prefix, in order (an index range scan on the unique key; read pool).
func (s *Store) OutboxByKeyPrefix(prefix string) ([]Outbox, error) {
	rows, err := s.r.QueryContext(context.Background(), `SELECT `+outboxCols+` FROM outbox
WHERE delivery_key >= ? AND delivery_key < ? ORDER BY seq`, prefix, prefix+"\U0010FFFF")
	return collect(scanOutbox)(rows, err)
}

// OutboxHasKeyPrefix checks publication without loading stored message bodies.
func (s *Store) OutboxHasKeyPrefix(prefix string) (bool, error) {
	var yes bool
	err := s.r.QueryRowContext(context.Background(), `SELECT EXISTS(SELECT 1 FROM outbox WHERE delivery_key>=? AND delivery_key<?)`, prefix, prefix+"\U0010FFFF").Scan(&yes)
	return yes, err
}

// CodexTurnModes loads mirror ownership once, without a second copy of turn IDs.
// The value distinguishes shared turns (keep local user input) from legacy ones.
func (s *Store) CodexTurnModes() (map[string]bool, error) {
	rows, err := s.r.QueryContext(context.Background(), `SELECT codex_turn_id,shared FROM turns WHERE codex_turn_id IS NOT NULL AND steering=0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	modes := map[string]bool{}
	for rows.Next() {
		var id string
		var shared bool
		if err := rows.Scan(&id, &shared); err != nil {
			return nil, err
		}
		modes[id] = modes[id] || shared
	}
	return modes, rows.Err()
}

// SetTranscriptProgress assigns commentary publication to the incremental reader.
// Old turns retain callback ownership, preventing automatic replay on upgrade.
func (t *Tx) SetTranscriptProgress(id int64) error {
	_, err := t.exec(`UPDATE turns SET transcript_progress=1,transcript_done=0 WHERE id=?`, id)
	return err
}

func (s *Store) TranscriptProgressTurns() (map[string]int64, map[string]int64, error) {
	// Capacity retries replace codex_turn_id. The stable client key still owns
	// unread records from earlier attempts, including after a restart.
	rows, err := s.r.QueryContext(context.Background(), `SELECT id,COALESCE(codex_turn_id,''),CASE WHEN state='starting' OR codex_turn_id IS NOT NULL THEN turn_key ELSE '' END FROM turns WHERE transcript_progress=1 AND steering=0`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	ids := map[string]int64{}
	clients := map[string]int64{}
	for rows.Next() {
		var id, client string
		var localID int64
		if err := rows.Scan(&localID, &id, &client); err != nil {
			return nil, nil, err
		}
		if id != "" {
			ids[id] = localID
		}
		if client != "" {
			clients[client] = localID
		}
	}
	return ids, clients, rows.Err()
}

// RecordedParts checks representation, not delivery confirmation. Queued and
// unknown parts must not be duplicated; a dropped part remains an explicit failure.
func (t *Tx) RecordedParts(msgs []OutMsg) (bool, error) {
	for _, m := range msgs {
		var yes bool
		if err := t.row(`SELECT EXISTS(SELECT 1 FROM outbox WHERE delivery_key=? OR (delivery_key=? AND body_hash IS ? AND body IS ?))`, m.DeliveryKey, m.AlternateKey, m.BodyHash, nullStr(m.Body)).Scan(&yes); err != nil {
			return false, err
		}
		if !yes {
			return false, nil
		}
	}
	return len(msgs) > 0, nil
}

// PendingAtLeast uses the partial pending index and reads no message bodies.
func (s *Store) PendingAtLeast(conv int64, limit int) (bool, error) {
	var n int
	err := s.r.QueryRowContext(context.Background(), `SELECT count(*) FROM (SELECT id FROM outbox WHERE conv_id=? AND state IN ('queued','sending','unknown') AND hold_turn IS NULL LIMIT ?)`, conv, limit).Scan(&n)
	return n >= limit, err
}

// PendingProgress prevents still-working notices from piling up behind content.
func (s *Store) PendingProgress(conv int64) (bool, error) {
	var yes bool
	err := s.r.QueryRowContext(context.Background(), `SELECT EXISTS(SELECT 1 FROM outbox WHERE conv_id=? AND state IN ('queued','sending','unknown') AND (kind='progress' OR progress_group IS NOT NULL OR delivery_key LIKE 'turn:%:visible:%' OR delivery_key LIKE 'turn:%:silence:%'))`, conv).Scan(&yes)
	return yes, err
}

// CompleteTranscriptProgress is committed with the terminal source position and
// all preceding output parts. The primary key keeps each terminal O(log history).
func (t *Tx) CompleteTranscriptProgress(id int64, nativeID string) error {
	tr, ok, err := t.Turn(id)
	if err != nil {
		return err
	}
	if !ok || tr.State == TurnStarting {
		return fmt.Errorf("native acceptance for turn %d is not durable yet", id)
	}
	// An earlier capacity attempt can end after its successor starts.
	if tr.CodexTurnID != nativeID || tr.State == TurnQueued {
		return nil
	}
	if !terminalTurnState(tr.State) {
		// The reader can observe the terminal before Run returns. Do not let
		// a subsequent local turn overtake the runner's not-yet-stored final.
		return fmt.Errorf("native outcome for turn %d is not durable yet", id)
	}
	_, err = t.exec(`UPDATE turns SET transcript_done=1 WHERE id=? AND codex_turn_id=? AND transcript_progress=1 AND transcript_done=0`, id, nativeID)
	if err != nil {
		return err
	}
	return t.ReleaseHeldProgress(tr.ConvID, id)
}
func (s *Store) TranscriptProgressDone(id int64) (bool, error) {
	var done bool
	err := s.r.QueryRowContext(context.Background(), `SELECT transcript_done FROM turns WHERE id=?`, id).Scan(&done)
	return done, err
}

// RepresentedRecord avoids rendering an already complete record during replay.
// It counts every declared part, including pending/unknown/explicit failures;
// it does not call that proof of delivery. The key range uses the unique index.
func (s *Store) RepresentedRecord(prefix string) (bool, error) {
	var minPart, maxPart, minTotal, maxTotal, count int
	err := s.r.QueryRowContext(context.Background(), `SELECT COALESCE(MIN(part),0),COALESCE(MAX(part),0),COALESCE(MIN(parts),0),COALESCE(MAX(parts),0),COUNT(DISTINCT part) FROM outbox WHERE delivery_key>=? AND delivery_key<? AND body IS NOT NULL`, prefix, prefix+"\U0010FFFF").Scan(&minPart, &maxPart, &minTotal, &maxTotal, &count)
	return minPart == 1 && minTotal > 0 && minTotal == maxTotal && maxPart == maxTotal && count == maxTotal, err
}
