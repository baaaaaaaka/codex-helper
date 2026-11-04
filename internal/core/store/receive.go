package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
)

// Stream reads a receive cursor.
func (t *Tx) Stream(key string) (Stream, bool, error) {
	var st Stream
	var conv sql.NullInt64
	var cur sql.NullString
	err := t.row(`SELECT key,conv_id,cursor,seeded,updated_at FROM streams WHERE key = ?`, key).
		Scan(&st.Key, &conv, &cur, &st.Seeded, &st.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Stream{}, false, nil
	}
	if err != nil {
		return Stream{}, false, err
	}
	st.ConvID, st.Cursor = conv.Int64, cur.String
	return st, true, nil
}

// Streams lists all cursors (read pool; loaded once at start so an unchanged
// poll can be decided in memory).
func (s *Store) Streams() ([]Stream, error) {
	rows, err := s.r.QueryContext(context.Background(), `SELECT key,conv_id,cursor,seeded,updated_at FROM streams ORDER BY key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Stream
	for rows.Next() {
		var st Stream
		var conv sql.NullInt64
		var cur sql.NullString
		if err := rows.Scan(&st.Key, &conv, &cur, &st.Seeded, &st.UpdatedAt); err != nil {
			return nil, err
		}
		st.ConvID, st.Cursor = conv.Int64, cur.String
		out = append(out, st)
	}
	return out, rows.Err()
}

// inboundState is the pure admission disposition of a message.
func inboundState(m InboundMsg, paused bool) string {
	if m.Sender == SenderSelf || m.Sender == SenderPeer || m.Kind == InSystem || m.Kind == InEcho {
		return InIgnored
	}
	if m.Kind == InPrompt && paused {
		return InHeld
	}
	return InQueued
}

// AdmitWindow persists one fully collected receive window (DC-3, finding F2):
// every message in ascending ext_time order, a queued turn for each new
// actionable prompt, and the cursor advance, all in this transaction.
// Already-known messages are skipped (UNIQUE(conv_id, ext_msg_id)); an
// unchanged window writes nothing.
func (t *Tx) AdmitWindow(streamKey string, convID int64, msgs []InboundMsg, newCursor string) ([]Admitted, error) {
	conv, ok, err := t.Conversation(convID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%w: conversation %d", ErrNotFound, convID)
	}
	sorted := append([]InboundMsg(nil), msgs...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].ExtTime != sorted[j].ExtTime {
			return sorted[i].ExtTime < sorted[j].ExtTime
		}
		return sorted[i].ExtMsgID < sorted[j].ExtMsgID
	})
	var out []Admitted
	for _, m := range sorted {
		if m.ExtMsgID == "" {
			return nil, errors.New("store: inbound message without ext id")
		}
		state := inboundState(m, conv.PausedAt != 0)
		var id int64
		err := t.row(`INSERT INTO inbound(conv_id,ext_msg_id,ext_time,received_at,sender,kind,state,body)
VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(conv_id, ext_msg_id) DO NOTHING RETURNING id`,
			convID, m.ExtMsgID, m.ExtTime, t.now, m.Sender, m.Kind, state, nullStr(m.Body)).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			continue // already admitted
		}
		if err != nil {
			return nil, err
		}
		for k, f := range m.Files {
			idx := f.Idx
			if idx == 0 {
				idx = k
			}
			fs := f.State
			if fs == "" {
				fs = "pending"
			}
			if _, err := t.exec(`INSERT INTO inbound_files(inbound_id,idx,name,ext_ref,local_path,kind,state,error)
VALUES (?,?,?,?,?,?,?,?)`, id, idx, f.Name, f.ExtRef, nullStr(f.LocalPath), f.Kind, fs, nullStr(f.Error)); err != nil {
				return nil, err
			}
		}
		a := Admitted{InboundID: id, ExtMsgID: m.ExtMsgID, Kind: m.Kind, State: state}
		if m.Kind == InPrompt && state == InQueued {
			if a.TurnID, err = t.newTurn(convID, id); err != nil {
				return nil, err
			}
			if _, err := t.exec(`UPDATE inbound SET turn_id = ? WHERE id = ?`, a.TurnID, id); err != nil {
				return nil, err
			}
		}
		out = append(out, a)
	}
	// Cursor advance in the same transaction; no write when unchanged.
	if _, err := t.exec(`INSERT INTO streams(key,conv_id,cursor,seeded,updated_at) VALUES (?,?,?,1,?)
ON CONFLICT(key) DO UPDATE SET cursor = excluded.cursor, seeded = 1, updated_at = excluded.updated_at
WHERE streams.cursor IS NOT excluded.cursor OR streams.seeded = 0`, streamKey, convID, nullStr(newCursor), t.now); err != nil {
		return nil, err
	}
	return out, nil
}

// SetInbound moves an inbound row to a new state (command executed, ignored,
// released). turnID 0 keeps the current link.
func (t *Tx) SetInbound(id int64, state string, turnID int64) error {
	var cur string
	if err := t.row(`SELECT state FROM inbound WHERE id = ?`, id).Scan(&cur); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: inbound %d", ErrNotFound, id)
		}
		return err
	}
	okMove := false
	switch cur {
	case InQueued:
		okMove = state == InDone || state == InIgnored || state == InHeld
	case InHeld:
		okMove = state == InQueued || state == InDone || state == InIgnored
	}
	if !okMove {
		return fmt.Errorf("%w: inbound %d %s -> %s", ErrState, id, cur, state)
	}
	_, err := t.exec(`UPDATE inbound SET state = ?, turn_id = COALESCE(?, turn_id) WHERE id = ?`, state, nullInt(turnID), id)
	return err
}

// Inbound reads one inbound row.
func (t *Tx) Inbound(id int64) (Inbound, bool, error) {
	return one(t.row(`SELECT `+inboundCols+` FROM inbound WHERE id = ?`, id), scanInbound)
}

// OpenInbound lists queued and held rows of a conversation in order.
func (t *Tx) OpenInbound(convID int64) ([]Inbound, error) {
	return collect(scanInbound)(t.rows(`SELECT `+inboundCols+` FROM inbound
WHERE conv_id = ? AND state IN ('queued','held') ORDER BY ext_time, id`, convID))
}

// InboundFiles lists the attachments of an inbound row.
func (t *Tx) InboundFiles(inboundID int64) ([]InboundFile, error) {
	rows, err := t.rows(`SELECT idx,name,ext_ref,local_path,kind,state,error FROM inbound_files WHERE inbound_id = ? ORDER BY idx`, inboundID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []InboundFile
	for rows.Next() {
		var f InboundFile
		var lp, e sql.NullString
		if err := rows.Scan(&f.Idx, &f.Name, &f.ExtRef, &lp, &f.Kind, &f.State, &e); err != nil {
			return nil, err
		}
		f.LocalPath, f.Error = lp.String, e.String
		out = append(out, f)
	}
	return out, rows.Err()
}

// SetInboundFile records the download/ASR outcome of one attachment.
func (t *Tx) SetInboundFile(inboundID int64, idx int, state, localPath, errText string) error {
	if state != "ready" && state != "failed" && state != "pending" {
		return fmt.Errorf("%w: file state %q", ErrState, state)
	}
	n, err := affected(t.exec(`UPDATE inbound_files SET state = ?, local_path = COALESCE(?, local_path), error = ?
WHERE inbound_id = ? AND idx = ?`, state, nullStr(localPath), nullStr(errText), inboundID, idx))
	if err == nil && n == 0 {
		err = fmt.Errorf("%w: inbound file %d/%d", ErrNotFound, inboundID, idx)
	}
	return err
}
