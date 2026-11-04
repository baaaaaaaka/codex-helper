package store

import (
	"errors"
	"fmt"
)

// Importer writes arbitrary typed rows into a fresh store. It exists only
// for the offline converter (STORE-DESIGN §7); the runtime API stays narrow.
type Importer struct{ t *Tx }

// ErrNotFresh means Import was called on a store that already holds data.
var ErrNotFresh = errors.New("store: import requires a fresh store")

// Import runs fn in one transaction on a store that has no conversations and
// no owner. It does not need Acquire.
func (s *Store) Import(fn func(*Importer) error) error {
	if s.w == nil {
		return errors.New("store: opened read-only")
	}
	return s.update(false, func(t *Tx) error {
		var n int
		if err := t.row(`SELECT (SELECT count(*) FROM conversations) + (SELECT count(*) FROM owner)`).Scan(&n); err != nil {
			return err
		}
		if n != 0 {
			return ErrNotFresh
		}
		return fn(&Importer{t: t})
	})
}

// Now returns the import transaction clock (unix ms).
func (im *Importer) Now() int64 { return im.t.now }

// SetMeta writes a meta key.
func (im *Importer) SetMeta(key, value string) error { return im.t.SetMeta(key, value) }

// Conversation inserts a conversation row as given (ID is assigned).
func (im *Importer) Conversation(c Conversation) (int64, error) {
	if c.CreatedAt == 0 {
		c.CreatedAt = im.t.now
	}
	if c.UpdatedAt == 0 {
		c.UpdatedAt = im.t.now
	}
	res, err := im.t.exec(`INSERT INTO conversations(label,kind,channel,ext_chat_id,ext_url,title,title_source,state,
parked_at,paused_at,paused_reason,paused_notified,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		c.Label, c.Kind, c.Channel, nullStr(c.ExtChatID), nullStr(c.ExtURL), nullStr(c.Title), nullStr(c.TitleSource),
		c.State, nullInt(c.ParkedAt), nullInt(c.PausedAt), nullStr(c.PausedReason), c.PausedNotified, c.CreatedAt, c.UpdatedAt)
	if err != nil {
		return 0, fmt.Errorf("conversation %s: %w", c.Label, err)
	}
	return res.LastInsertId()
}

// Thread inserts a thread binding.
func (im *Importer) Thread(th Thread) error { return im.t.Bind(th) }

// Stream inserts a receive cursor.
func (im *Importer) Stream(st Stream) error {
	_, err := im.t.exec(`INSERT INTO streams(key,conv_id,cursor,seeded,updated_at) VALUES (?,?,?,?,?)`,
		st.Key, nullInt(st.ConvID), nullStr(st.Cursor), st.Seeded, im.t.now)
	return err
}

// Inbound inserts an inbound row. It reports false when (conv, ext id) exists.
func (im *Importer) Inbound(in Inbound) (int64, bool, error) {
	var id int64
	err := im.t.row(`INSERT INTO inbound(conv_id,ext_msg_id,ext_time,received_at,sender,kind,state,body,turn_id)
VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(conv_id, ext_msg_id) DO NOTHING RETURNING id`,
		in.ConvID, in.ExtMsgID, in.ExtTime, in.ReceivedAt, in.Sender, in.Kind, in.State, nullStr(in.Body), nullInt(in.TurnID)).Scan(&id)
	if err != nil {
		if isNoRows(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return id, true, nil
}

// PauseConversation sets the per-chat breaker of an imported conversation
// (used when it holds rows the converter could not classify).
func (im *Importer) PauseConversation(convID, at int64, reason string) error {
	_, err := im.t.exec(`UPDATE conversations SET paused_at = COALESCE(paused_at, ?), paused_reason = COALESCE(paused_reason, ?) WHERE id = ?`,
		at, reason, convID)
	return err
}

// Park parks an imported conversation (the loser of a shared Codex thread).
func (im *Importer) Park(convID int64) error {
	_, err := im.t.exec(`UPDATE conversations SET state = 'parked', parked_at = COALESCE(parked_at, ?), updated_at = ?
WHERE id = ? AND state IN ('active','creating')`, im.t.now, im.t.now, convID)
	return err
}

// LinkInbound sets the turn of an inbound row.
func (im *Importer) LinkInbound(inboundID, turnID int64) error {
	_, err := im.t.exec(`UPDATE inbound SET turn_id = ? WHERE id = ?`, turnID, inboundID)
	return err
}

// Turn inserts a turn row as given.
func (im *Importer) Turn(tr Turn) (int64, error) {
	res, err := im.t.exec(`INSERT INTO turns(conv_id,inbound_id,turn_key,state,thread_id,codex_turn_id,model,effort,attempt,
retry_deadline,queued_at,started_at,finished_at,error) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		tr.ConvID, nullInt(tr.InboundID), tr.TurnKey, tr.State, nullStr(tr.ThreadID), nullStr(tr.CodexTurnID),
		nullStr(tr.Model), nullStr(tr.Effort), tr.Attempt, nullInt(tr.RetryDeadline), tr.QueuedAt,
		nullInt(tr.StartedAt), nullInt(tr.FinishedAt), nullStr(tr.Error))
	if err != nil {
		return 0, fmt.Errorf("turn %s: %w", tr.TurnKey, err)
	}
	return res.LastInsertId()
}

// Outbox inserts an outbox row as given (Seq must be set). It reports false
// when the delivery key already exists.
func (im *Importer) Outbox(o Outbox, f *OutFile) (int64, bool, error) {
	if o.Part == 0 {
		o.Part = 1
	}
	if o.Parts == 0 {
		o.Parts = 1
	}
	var id int64
	err := im.t.row(`INSERT INTO outbox(conv_id,delivery_key,seq,kind,part,parts,body,body_hash,mention,reply_to,state,
attempt_token,ext_msg_id,next_attempt_at,created_at,sent_at,note,hash_algo) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(delivery_key) DO NOTHING RETURNING id`,
		o.ConvID, o.DeliveryKey, o.Seq, o.Kind, o.Part, o.Parts, nullStr(o.Body), o.BodyHash, o.Mention, nullStr(o.ReplyTo),
		o.State, nullStr(o.AttemptToken), nullStr(o.ExtMsgID), nullInt(o.NextAttemptAt), o.CreatedAt, nullInt(o.SentAt), nullStr(o.Note),
		nullStr(o.HashAlgo)).Scan(&id)
	if err != nil {
		if isNoRows(err) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("outbox %s: %w", o.DeliveryKey, err)
	}
	if f != nil {
		if _, err := im.t.exec(`INSERT INTO outbox_files(outbox_id,local_path,size,upload_url,upload_expires_at,uploaded_bytes,ext_item_id)
VALUES (?,?,?,?,?,?,?)`, id, f.LocalPath, f.Size, nullStr(f.UploadURL), nullInt(f.UploadExpiresAt), f.UploadedBytes, nullStr(f.ExtItemID)); err != nil {
			return 0, false, err
		}
	}
	return id, true, nil
}

// Source inserts a transcript source as given.
func (im *Importer) Source(src TranscriptSource) (int64, error) {
	res, err := im.t.exec(`INSERT INTO transcript_sources(path,thread_id,conv_id,state,offset,line,size,mtime,inode,updated_at)
VALUES (?,?,?,?,?,?,?,?,?,?)`, src.Path, nullStr(src.ThreadID), nullInt(src.ConvID), src.State, src.Offset, src.Line,
		src.Size, src.MTime, nullInt(src.Inode), im.t.now)
	if err != nil {
		return 0, fmt.Errorf("source %s: %w", src.Path, err)
	}
	return res.LastInsertId()
}

// Fork inserts a fork row.
func (im *Importer) Fork(f Fork) (int64, error) {
	res, err := im.t.exec(`INSERT INTO forks(parent_conv,child_conv,phase,child_thread_id,created_at,updated_at) VALUES (?,?,?,?,?,?)`,
		f.ParentConv, nullInt(f.ChildConv), f.Phase, nullStr(f.ChildThreadID), f.CreatedAt, im.t.now)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// Notification inserts a notification row.
func (im *Importer) Notification(n Notification) error {
	_, err := im.t.exec(`INSERT INTO notifications(key,kind,payload,state,created_at,sent_at) VALUES (?,?,?,?,?,?)
ON CONFLICT(key) DO NOTHING`, n.Key, n.Kind, n.Payload, n.State, n.CreatedAt, nullInt(n.SentAt))
	return err
}

// Gate inserts a gate.
func (im *Importer) Gate(g Gate) error { return im.t.SetGate(g.Key, g.Until, g.Reason) }

// MaxSeq returns the highest outbox seq of a conversation.
func (im *Importer) MaxSeq(convID int64) (int64, error) {
	var n int64
	err := im.t.row(`SELECT COALESCE(MAX(seq), 0) FROM outbox WHERE conv_id = ?`, convID).Scan(&n)
	return n, err
}

// QueryInt runs a read-only scalar query inside the import transaction.
func (im *Importer) QueryInt(q string, args ...any) (int64, error) {
	var n int64
	err := im.t.row(q, args...).Scan(&n)
	return n, err
}
