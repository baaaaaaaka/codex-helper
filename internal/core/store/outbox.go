package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
)

// Enqueue appends messages to a conversation's outbox. It is idempotent on
// delivery_key (DC-5): a key that already exists is skipped and does not
// consume a sequence number. It returns the number of new rows.
func (t *Tx) Enqueue(convID int64, msgs []OutMsg) (int, error) {
	n := 0
	for _, m := range msgs {
		if m.DeliveryKey == "" {
			return n, errors.New("store: outbox message without delivery key")
		}
		var exists int
		if err := t.row(`SELECT EXISTS(SELECT 1 FROM outbox WHERE delivery_key = ?)`, m.DeliveryKey).Scan(&exists); err != nil {
			return n, err
		}
		if exists != 0 {
			continue
		}
		part, parts := m.Part, m.Parts
		if part == 0 {
			part = 1
		}
		if parts == 0 {
			parts = 1
		}
		var id int64
		err := t.row(`INSERT INTO outbox(conv_id,delivery_key,seq,kind,part,parts,body,body_hash,mention,reply_to,state,created_at)
VALUES (?, ?, (SELECT COALESCE(MAX(seq), 0) + 1 FROM outbox WHERE conv_id = ?), ?, ?, ?, ?, ?, ?, ?, 'queued', ?)
RETURNING id`, convID, m.DeliveryKey, convID, m.Kind, part, parts, nullStr(m.Body), m.BodyHash, m.Mention,
			nullStr(m.ReplyTo), t.now).Scan(&id)
		if err != nil {
			return n, err
		}
		if f := m.File; f != nil {
			if _, err := t.exec(`INSERT INTO outbox_files(outbox_id,local_path,size,upload_url,upload_expires_at,uploaded_bytes,ext_item_id)
VALUES (?,?,?,?,?,?,?)`, id, f.LocalPath, f.Size, nullStr(f.UploadURL), nullInt(f.UploadExpiresAt), f.UploadedBytes, nullStr(f.ExtItemID)); err != nil {
				return n, err
			}
		}
		n++
	}
	return n, nil
}

// NextSendable returns, for every conversation, its head pending row when
// that row is queued and due. A sending or unknown head blocks only its own
// conversation (R-11); unknown rows are never returned.
func (t *Tx) NextSendable(now int64) ([]Outbox, error) {
	return collect(scanOutbox)(t.rows(`SELECT `+outboxCols+` FROM outbox
WHERE id IN (SELECT (SELECT p.id FROM outbox p WHERE p.conv_id = h.conv_id AND p.state IN ('queued','sending','unknown')
                     ORDER BY p.seq LIMIT 1)
             FROM (SELECT DISTINCT conv_id FROM outbox WHERE state IN ('queued','sending','unknown')) h)
  AND state = 'queued' AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
ORDER BY conv_id`, now))
}

// Outbox reads one outbox row.
func (t *Tx) Outbox(id int64) (Outbox, bool, error) {
	return one(t.row(`SELECT `+outboxCols+` FROM outbox WHERE id = ?`, id), scanOutbox)
}

// PendingOutbox lists the pending rows of a conversation in order.
func (t *Tx) PendingOutbox(convID int64) ([]Outbox, error) {
	return collect(scanOutbox)(t.rows(`SELECT `+outboxCols+` FROM outbox
WHERE conv_id = ? AND state IN ('queued','sending','unknown') ORDER BY seq`, convID))
}

// BeginSend marks the conversation's head queued row as sending with a fresh
// attempt token (embedded as the read-back marker).
func (t *Tx) BeginSend(id int64) (string, error) {
	o, ok, err := t.Outbox(id)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("%w: outbox %d", ErrNotFound, id)
	}
	if o.State != OutQueued {
		return "", fmt.Errorf("%w: outbox %d is %s", ErrState, id, o.State)
	}
	var ahead int
	if err := t.row(`SELECT EXISTS(SELECT 1 FROM outbox WHERE conv_id = ? AND state IN ('queued','sending','unknown') AND seq < ?)`,
		o.ConvID, o.Seq).Scan(&ahead); err != nil {
		return "", err
	}
	if ahead != 0 {
		return "", fmt.Errorf("%w: outbox %d is not the head of conversation %d", ErrState, id, o.ConvID)
	}
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	tok := hex.EncodeToString(b[:])
	return tok, t.outMove(id, []string{OutQueued}, `state = 'sending', attempt_token = ?`, tok)
}

// MarkSent records the channel message id of a sending (or resolved unknown) row.
func (t *Tx) MarkSent(id int64, extMsgID string) error {
	return t.outMove(id, []string{OutSending, OutUnknown},
		`state = 'sent', ext_msg_id = ?, sent_at = ?, next_attempt_at = NULL`, nullStr(extMsgID), t.now)
}

// MarkUnknown records an ambiguous send outcome; the row then blocks its
// conversation until resolved and is never re-sent automatically.
func (t *Tx) MarkUnknown(id int64, note string) error {
	return t.outMove(id, []string{OutSending}, `state = 'unknown', note = ?`, nullStr(note))
}

// Requeue returns a sending row (definite rejection) or an unknown row
// (authoritative read-back absence) to queued. at=0 means due now.
func (t *Tx) Requeue(id int64, at int64) error {
	return t.outMove(id, []string{OutSending, OutUnknown},
		`state = 'queued', next_attempt_at = ?, attempt_token = NULL`, nullInt(at))
}

// Drop gives up on a pending row.
func (t *Tx) Drop(id int64, note string) error {
	return t.outMove(id, []string{OutQueued, OutSending, OutUnknown}, `state = 'dropped', note = ?`, nullStr(note))
}

func (t *Tx) outMove(id int64, from []string, set string, args ...any) error {
	q := `UPDATE outbox SET ` + set + ` WHERE id = ? AND state IN (` + placeholders(len(from)) + `)`
	args = append(args, id)
	for _, s := range from {
		args = append(args, s)
	}
	n, err := affected(t.exec(q, args...))
	if err != nil {
		return err
	}
	if n == 0 {
		o, ok, err := t.Outbox(id)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w: outbox %d", ErrNotFound, id)
		}
		return fmt.Errorf("%w: outbox %d is %s", ErrState, id, o.State)
	}
	return nil
}

// IsOwn reports whether a received message is one of ours: its ext id is in
// our outbox, or its body hash equals a sending/sent/unknown row of the
// conversation created at or after since.
func (t *Tx) IsOwn(convID int64, extMsgID string, bodyHash []byte, since int64) (bool, error) {
	var hit int
	if extMsgID != "" {
		if err := t.row(`SELECT EXISTS(SELECT 1 FROM outbox WHERE conv_id = ? AND ext_msg_id = ?)`, convID, extMsgID).Scan(&hit); err != nil || hit != 0 {
			return hit != 0, err
		}
	}
	if len(bodyHash) > 0 {
		err := t.row(`SELECT EXISTS(SELECT 1 FROM outbox WHERE conv_id = ? AND body_hash = ?
AND state IN ('sending','sent','unknown') AND created_at >= ?)`, convID, bodyHash, since).Scan(&hit)
		return hit != 0, err
	}
	return false, nil
}

// UnknownOutbox lists unknown rows (read-back resolution at start).
func (s *Store) UnknownOutbox() ([]Outbox, error) {
	return collect(scanOutbox)(s.r.QueryContext(context.Background(), `SELECT `+outboxCols+` FROM outbox
WHERE state IN ('queued','sending','unknown') AND state = 'unknown' ORDER BY conv_id, seq`))
}

// OutboxFile reads the upload state of an outbox row.
func (t *Tx) OutboxFile(outboxID int64) (OutFile, bool, error) {
	var f OutFile
	var p nullPack
	p.raw(&f.LocalPath)
	p.raw(&f.Size)
	p.str(&f.UploadURL)
	p.int(&f.UploadExpiresAt)
	p.raw(&f.UploadedBytes)
	p.str(&f.ExtItemID)
	return one(t.row(`SELECT local_path,size,upload_url,upload_expires_at,uploaded_bytes,ext_item_id FROM outbox_files WHERE outbox_id = ?`, outboxID),
		func(sc scanner) (OutFile, error) { err := p.scan(sc); return f, err })
}

// SetUpload records upload-session progress of an outbound file.
func (t *Tx) SetUpload(outboxID int64, uploadURL string, expiresAt, uploaded int64, extItemID string) error {
	n, err := affected(t.exec(`UPDATE outbox_files SET upload_url = ?, upload_expires_at = ?, uploaded_bytes = ?,
ext_item_id = COALESCE(?, ext_item_id) WHERE outbox_id = ?`, nullStr(uploadURL), nullInt(expiresAt), uploaded, nullStr(extItemID), outboxID))
	if err == nil && n == 0 {
		err = fmt.Errorf("%w: outbox file %d", ErrNotFound, outboxID)
	}
	return err
}
