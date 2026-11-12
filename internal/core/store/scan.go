package store

import (
	"database/sql"
	"errors"
)

type scanner interface{ Scan(dest ...any) error }

// ns/ni collect nullable columns and copy them out after Scan.
type nullPack struct {
	s   []*sql.NullString
	sp  []*string
	i   []*sql.NullInt64
	ip  []*int64
	dst []any
}

func (p *nullPack) str(dst *string) {
	n := &sql.NullString{}
	p.s, p.sp = append(p.s, n), append(p.sp, dst)
	p.dst = append(p.dst, n)
}

func (p *nullPack) int(dst *int64) {
	n := &sql.NullInt64{}
	p.i, p.ip = append(p.i, n), append(p.ip, dst)
	p.dst = append(p.dst, n)
}

func (p *nullPack) raw(dst any) { p.dst = append(p.dst, dst) }

func (p *nullPack) scan(sc scanner) error {
	if err := sc.Scan(p.dst...); err != nil {
		return err
	}
	for k, n := range p.s {
		*p.sp[k] = n.String
	}
	for k, n := range p.i {
		*p.ip[k] = n.Int64
	}
	return nil
}

const convCols = `id,label,kind,channel,ext_chat_id,ext_url,title,title_source,state,parked_at,paused_at,paused_reason,paused_notified,created_at,updated_at,auto_park_off`

func scanConv(sc scanner) (Conversation, error) {
	var c Conversation
	var p nullPack
	p.raw(&c.ID)
	p.raw(&c.Label)
	p.raw(&c.Kind)
	p.raw(&c.Channel)
	p.str(&c.ExtChatID)
	p.str(&c.ExtURL)
	p.str(&c.Title)
	p.str(&c.TitleSource)
	p.raw(&c.State)
	p.int(&c.ParkedAt)
	p.int(&c.PausedAt)
	p.str(&c.PausedReason)
	p.raw(&c.PausedNotified)
	p.raw(&c.CreatedAt)
	p.raw(&c.UpdatedAt)
	p.raw(&c.AutoParkOff)
	err := p.scan(sc)
	return c, err
}

const threadCols = `conv_id,thread_id,cwd,codex_home,profile,model,model_source,effort,effort_source,pending_model,pending_effort,sandbox,model_pin,fork_parent,updated_at`

func scanThread(sc scanner) (Thread, error) {
	var t Thread
	var p nullPack
	p.raw(&t.ConvID)
	p.str(&t.ThreadID)
	p.raw(&t.Cwd)
	p.str(&t.CodexHome)
	p.str(&t.Profile)
	p.str(&t.Model)
	p.str(&t.ModelSource)
	p.str(&t.Effort)
	p.str(&t.EffortSource)
	p.str(&t.PendingModel)
	p.str(&t.PendingEffort)
	p.str(&t.Sandbox)
	p.str(&t.ModelPin)
	p.int(&t.ForkParent)
	p.raw(&t.UpdatedAt)
	err := p.scan(sc)
	return t, err
}

const inboundCols = `id,conv_id,ext_msg_id,ext_time,received_at,sender,kind,state,body,turn_id,queued_only`

func scanInbound(sc scanner) (Inbound, error) {
	var in Inbound
	var p nullPack
	p.raw(&in.ID)
	p.raw(&in.ConvID)
	p.raw(&in.ExtMsgID)
	p.raw(&in.ExtTime)
	p.raw(&in.ReceivedAt)
	p.raw(&in.Sender)
	p.raw(&in.Kind)
	p.raw(&in.State)
	p.str(&in.Body)
	p.int(&in.TurnID)
	p.raw(&in.Queue)
	err := p.scan(sc)
	return in, err
}

const turnCols = `id,conv_id,inbound_id,turn_key,state,thread_id,codex_turn_id,model,effort,attempt,retry_deadline,queued_at,started_at,finished_at,error,queued_only,steering,transcript_progress,transcript_done`

func scanTurn(sc scanner) (Turn, error) {
	var t Turn
	var p nullPack
	p.raw(&t.ID)
	p.raw(&t.ConvID)
	p.int(&t.InboundID)
	p.raw(&t.TurnKey)
	p.raw(&t.State)
	p.str(&t.ThreadID)
	p.str(&t.CodexTurnID)
	p.str(&t.Model)
	p.str(&t.Effort)
	p.raw(&t.Attempt)
	p.int(&t.RetryDeadline)
	p.raw(&t.QueuedAt)
	p.int(&t.StartedAt)
	p.int(&t.FinishedAt)
	p.str(&t.Error)
	p.raw(&t.Queue)
	p.raw(&t.Steering)
	p.raw(&t.TranscriptProgress)
	p.raw(&t.TranscriptDone)
	err := p.scan(sc)
	return t, err
}

const outboxCols = `id,conv_id,delivery_key,seq,kind,part,parts,body,body_hash,mention,reply_to,state,attempt_token,ext_msg_id,next_attempt_at,created_at,sent_at,note,hash_algo,progress_group,batch_head,batch_body,hold_turn,append_to`

func scanOutbox(sc scanner) (Outbox, error) {
	var o Outbox
	var p nullPack
	p.raw(&o.ID)
	p.raw(&o.ConvID)
	p.raw(&o.DeliveryKey)
	p.raw(&o.Seq)
	p.raw(&o.Kind)
	p.raw(&o.Part)
	p.raw(&o.Parts)
	p.str(&o.Body)
	p.raw(&o.BodyHash)
	p.raw(&o.Mention)
	p.str(&o.ReplyTo)
	p.raw(&o.State)
	p.str(&o.AttemptToken)
	p.str(&o.ExtMsgID)
	p.int(&o.NextAttemptAt)
	p.raw(&o.CreatedAt)
	p.int(&o.SentAt)
	p.str(&o.Note)
	p.str(&o.HashAlgo)
	p.str(&o.ProgressGroup)
	p.int(&o.BatchHead)
	p.str(&o.BatchBody)
	p.int(&o.HoldTurn)
	p.str(&o.AppendTo)
	err := p.scan(sc)
	if o.BatchBody != "" {
		o.Body = o.BatchBody
	}
	return o, err
}

const sourceCols = `id,path,thread_id,conv_id,state,offset,line,size,mtime,inode,updated_at`

func scanSource(sc scanner) (TranscriptSource, error) {
	var s TranscriptSource
	var p nullPack
	p.raw(&s.ID)
	p.raw(&s.Path)
	p.str(&s.ThreadID)
	p.int(&s.ConvID)
	p.raw(&s.State)
	p.raw(&s.Offset)
	p.raw(&s.Line)
	p.raw(&s.Size)
	p.raw(&s.MTime)
	p.int(&s.Inode)
	p.raw(&s.UpdatedAt)
	err := p.scan(sc)
	return s, err
}

// collect returns a function that scans every row with scan; call it as
// collect(scanX)(db.Query(...)).
func collect[T any](scan func(scanner) (T, error)) func(*sql.Rows, error) ([]T, error) {
	return func(rows *sql.Rows, err error) ([]T, error) { return collectRows(rows, err, scan) }
}

func collectRows[T any](rows *sql.Rows, err error, scan func(scanner) (T, error)) ([]T, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []T
	for rows.Next() {
		v, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func one[T any](row *sql.Row, scan func(scanner) (T, error)) (T, bool, error) {
	v, err := scan(row)
	if err == sql.ErrNoRows {
		var zero T
		return zero, false, nil
	}
	if err != nil {
		var zero T
		return zero, false, err
	}
	return v, true, nil
}

func isNoRows(err error) bool { return errors.Is(err, sql.ErrNoRows) }
