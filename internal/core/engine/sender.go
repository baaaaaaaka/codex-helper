package engine

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// senderLoop delivers the outbox head of every conversation, in order per
// conversation, in parallel across conversations, control chat first.
// Rows in sending state belong to a worker; the store fences a second
// sender for the same conversation (BeginSend requires the head).
func (e *Engine) senderLoop(ctx context.Context) {
	sem := make(chan struct{}, e.cfg.Senders)
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.senderWake:
		case <-timer.C:
		}
		again := e.sendPass(ctx, sem)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		if again > 0 {
			timer.Reset(again)
		} else {
			timer.Reset(e.t.Maintenance) // safety net for timed requeues
		}
	}
}

// sendPass starts sends for every due head. It returns when to look again
// (0: only on wake).
func (e *Engine) sendPass(ctx context.Context, sem chan struct{}) time.Duration {
	now := e.now()
	if until := e.gate(e.gWrite); now.Before(until) {
		return until.Sub(now)
	}
	type job struct {
		row    store.Outbox
		chat   string
		token  string
		file   *store.OutFile // an outbound file row
		upload bool           // upload the file first (no send yet)
	}
	var (
		jobs  []job
		again time.Duration
	)
	soon := func(d time.Duration) {
		if d < e.t.Tick {
			d = e.t.Tick
		}
		if again == 0 || d < again {
			again = d
		}
	}
	err := e.update(func(tx *store.Tx) error {
		jobs = jobs[:0]
		rows, err := tx.NextSendable(ms(now))
		if err != nil {
			return err
		}
		e.mu.Lock()
		ctl := e.controlID
		e.mu.Unlock()
		// Control chat first, then round robin from the conversation after
		// the last one served, so a backlog in low-id chats never starves
		// the others (review H4, CORE-DESIGN §3).
		last := e.sendLast
		rank := func(r store.Outbox) int {
			switch {
			case r.ConvID == ctl:
				return 0
			case r.ConvID > last:
				return 1
			default:
				return 2
			}
		}
		sort.SliceStable(rows, func(i, j int) bool {
			if a, b := rank(rows[i]), rank(rows[j]); a != b {
				return a < b
			}
			return rows[i].ConvID < rows[j].ConvID
		})
		for _, r := range rows {
			e.mu.Lock()
			cs := e.convs[r.ConvID]
			var chat string
			wait := time.Duration(0)
			busy := cs == nil || cs.chat == "" || cs.sending
			if !busy {
				chat = cs.chat
				if gap := e.t.ChatRate - now.Sub(cs.lastSend); gap > 0 {
					wait = gap
				}
			}
			e.mu.Unlock()
			if busy {
				continue
			}
			f, isFile, err := tx.OutboxFile(r.ID)
			if err != nil {
				return err
			}
			if isFile {
				e.mu.Lock()
				at := e.uploadAt[r.ID]
				e.mu.Unlock()
				if d := at.Sub(now); d > 0 && f.ExtItemID == "" {
					soon(d)
					continue
				}
			}
			if wait > 0 {
				soon(wait)
				continue
			}
			// The send slot first: a write token taken while every slot
			// is busy would be wasted (review L9).
			select {
			case sem <- struct{}{}:
			default:
				soon(e.t.Tick)
				return nil
			}
			if ok, w := e.writeTokens.take(); !ok {
				<-sem
				soon(w)
				break
			}
			if r.ConvID != ctl {
				e.sendLast = r.ConvID
			}
			var fp *store.OutFile
			if isFile {
				fp = &f
				if f.ExtItemID == "" {
					jobs = append(jobs, job{row: r, chat: chat, file: fp, upload: true})
					continue
				}
			}
			tok, err := tx.BeginSend(r.ID)
			if err != nil {
				<-sem
				if errors.Is(err, store.ErrState) {
					continue
				}
				return err
			}
			jobs = append(jobs, job{row: r, chat: chat, token: tok, file: fp})
		}
		return nil
	})
	if err != nil {
		for range jobs {
			<-sem
		}
		e.logf("send pass: %v", err)
		return e.t.Hot
	}
	for _, j := range jobs {
		e.mu.Lock()
		if cs := e.convs[j.row.ConvID]; cs != nil {
			cs.sending = true
		}
		e.mu.Unlock()
		e.wg.Add(1)
		go func(j job) {
			defer e.wg.Done()
			defer func() { <-sem }()
			if j.upload {
				e.upload(ctx, j.row, *j.file)
				return
			}
			e.send(ctx, j.row, j.chat, j.token, j.file)
		}(j)
	}
	return again
}

// marker is the hidden marker of one send attempt.
func (e *Engine) marker(token string) string { return e.prefix + "." + token }

// send performs one Send and records its outcome.
func (e *Engine) send(ctx context.Context, r store.Outbox, chat, token string, file *store.OutFile) {
	p := channel.Part{Index: max(r.Part, 1), Count: max(r.Parts, 1), Body: r.Body, Hash: hex.EncodeToString(r.BodyHash),
		Mention: r.Mention, ReplyTo: r.ReplyTo, Size: len(r.Body)}
	if file != nil {
		fp, err := e.filePart(ctx, *file)
		if err != nil {
			e.logf("render file %s: %v; dropped", r.DeliveryKey, err)
			e.mu.Lock()
			if cs := e.convs[r.ConvID]; cs != nil {
				cs.sending = false
			}
			e.mu.Unlock()
			e.dropFile(r, filepath.Base(file.LocalPath))
			return
		}
		p = fp
	}
	sent, err := e.ch.Send(ctx, chat, p, e.marker(token))
	now := e.now()
	e.count(func(s *engineStats) { s.sends++ })
	defer func() {
		e.mu.Lock()
		if cs := e.convs[r.ConvID]; cs != nil {
			cs.sending = false
			cs.lastSend = now
		}
		e.mu.Unlock()
		e.wakeSender()
	}()
	if ctx.Err() != nil && err != nil {
		return // the row stays sending; the next start marks it unknown
	}
	var (
		rej  *channel.Rejected
		auth *channel.AuthRequired
	)
	th, throttled := channel.AsThrottled(err)
	switch {
	case err == nil:
		if err := e.update(func(tx *store.Tx) error { return tx.MarkSent(r.ID, sent.ID) }); err != nil && !errors.Is(err, store.ErrState) {
			e.logf("mark sent %d: %v", r.ID, err)
		}
		e.forgetFailures(r.ID)
		e.wakeRunner(r.ConvID)
		if r.Kind != store.OutAck {
			e.heat(r.ConvID)
		}
	case channel.IsAmbiguous(err):
		e.logf("send %s to conversation %d: outcome unknown (%v); resolving by read-back", r.DeliveryKey, r.ConvID, err)
		if err := e.update(func(tx *store.Tx) error { return tx.MarkUnknown(r.ID, clipText(err.Error(), 200)) }); err != nil {
			e.logf("mark unknown %d: %v", r.ID, err)
		}
		e.forgetFailures(r.ID)
		e.noteUnknown(r.ID, now)
		e.wakeResolver()
	case throttled:
		at := now.Add(e.retryAfter(th) + time.Duration(r.ConvID%5)*e.t.Tick)
		if th.Scope == channel.ScopeAccount || th.Scope == channel.ScopeUnknown {
			e.setGate(e.gWrite, at, "send throttled")
		}
		e.requeue(r.ID, at)
	case errors.As(err, &rej) && rej.Permanent:
		e.logf("send %s to conversation %d rejected permanently: %v; dropped", r.DeliveryKey, r.ConvID, err)
		if err := e.update(func(tx *store.Tx) error { return tx.Drop(r.ID, clipText(err.Error(), 200)) }); err != nil {
			e.logf("drop %d: %v", r.ID, err)
		}
		e.forgetFailures(r.ID)
		e.wakeRunner(r.ConvID)
	case errors.As(err, &auth):
		e.logf("send needs sign-in: %v", err)
		e.requeue(r.ID, now.Add(e.t.MaxBackoff))
	default:
		n := e.failure(r.ID)
		back := e.t.Hot << min(n, 8)
		if back > e.t.MaxBackoff {
			back = e.t.MaxBackoff
		}
		if n <= 3 || n%10 == 0 {
			e.logf("send %s to conversation %d failed (%d): %v; retry in %s", r.DeliveryKey, r.ConvID, n, err, back)
		}
		e.requeue(r.ID, now.Add(back))
	}
}

// retryAfter is the wait a throttle response asks for, at least
// ThrottleFloor when it names none.
func (e *Engine) retryAfter(th *channel.Throttled) time.Duration {
	if th.RetryAfter > 0 {
		return th.RetryAfter
	}
	return e.t.ThrottleFloor
}

func (e *Engine) requeue(id int64, at time.Time) {
	if err := e.update(func(tx *store.Tx) error { return tx.Requeue(id, ms(at)) }); err != nil {
		e.logf("requeue %d: %v", id, err)
		return
	}
	d := at.Sub(e.now())
	if d < 0 {
		d = 0
	}
	time.AfterFunc(d, e.wakeSender)
}

func (e *Engine) failure(id int64) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.sendFailures == nil {
		e.sendFailures = map[int64]int{}
	}
	e.sendFailures[id]++
	return e.sendFailures[id]
}

func (e *Engine) forgetFailures(id int64) {
	e.mu.Lock()
	delete(e.sendFailures, id)
	e.mu.Unlock()
}

// unknownState tracks the read-back attempts of an unknown send.
type unknownState struct {
	since time.Time
	tries int
	next  time.Time
}

func (e *Engine) noteUnknown(id int64, at time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.unknown == nil {
		e.unknown = map[int64]*unknownState{}
	}
	if e.unknown[id] == nil {
		e.unknown[id] = &unknownState{since: at, next: at.Add(e.t.ResolveDelay)}
	}
}

// resolverLoop resolves unknown sends by reading the chat back. A send
// found there (by its marker, or by its content hash when markers are not
// trusted) becomes sent. One that is still not found after ResolveTries
// reads spanning at least ResolveAbsent is absent: a final or mirror row is
// sent again once (STORE-DESIGN §3.7, owner decision "read back; resend the
// final if it is absent"); anything else, or a resend that is again absent,
// is dropped with one visible notice.
func (e *Engine) resolverLoop(ctx context.Context) {
	if rows, err := e.st.UnknownOutbox(); err == nil {
		for _, r := range rows {
			e.noteUnknown(r.ID, e.now())
		}
	}
	timer := time.NewTimer(e.t.ResolveDelay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.resolverWake:
		case <-timer.C:
		}
		next := e.resolvePass(ctx)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		if next <= 0 {
			next = time.Hour
		}
		timer.Reset(next)
	}
}

func (e *Engine) resolvePass(ctx context.Context) time.Duration {
	now := e.now()
	e.mu.Lock()
	var due []int64
	var wait time.Duration
	for id, u := range e.unknown {
		if d := u.next.Sub(now); d > 0 {
			if wait == 0 || d < wait {
				wait = d
			}
			continue
		}
		due = append(due, id)
	}
	e.mu.Unlock()
	if len(due) == 0 {
		return wait
	}
	if until := e.gate(e.gRead); now.Before(until) {
		return until.Sub(now)
	}
	sort.Slice(due, func(i, j int) bool { return due[i] < due[j] })
	for _, id := range due {
		if ctx.Err() != nil {
			return 0
		}
		e.resolveOne(ctx, id)
	}
	return e.t.Tick
}

func (e *Engine) resolveOne(ctx context.Context, id int64) {
	var r store.Outbox
	var ok bool
	if err := e.update(func(tx *store.Tx) error {
		var err error
		r, ok, err = tx.Outbox(id)
		return err
	}); err != nil {
		return
	}
	if !ok || r.State != store.OutUnknown {
		e.mu.Lock()
		delete(e.unknown, id)
		e.mu.Unlock()
		return
	}
	cs := e.conv(r.ConvID)
	if cs == nil {
		return
	}
	e.mu.Lock()
	chat := cs.chat
	u := e.unknown[id]
	e.mu.Unlock()
	msgs, err := e.ch.ReadBack(ctx, chat, fromMS(r.CreatedAt).Add(-time.Minute))
	if ctx.Err() != nil {
		return
	}
	now := e.now()
	if err != nil {
		if th, ok := channel.AsThrottled(err); ok {
			e.mu.Lock()
			u.next = now.Add(e.retryAfter(th))
			e.mu.Unlock()
			if th.Scope != channel.ScopeChat {
				e.setGate(e.gRead, now.Add(e.retryAfter(th)), "read-back throttled")
			}
			return
		}
		e.logf("read-back of %d: %v", id, err)
		e.mu.Lock()
		u.next = now.Add(e.t.ResolveDelay)
		e.mu.Unlock()
		return
	}
	marker := ""
	if r.AttemptToken != "" {
		marker = e.marker(r.AttemptToken)
	}
	hash := hex.EncodeToString(r.BodyHash)
	var found string
	err = e.update(func(tx *store.Tx) error {
		found = ""
		markers := map[string]bool{}
		if marker != "" {
			markers[marker] = true
		}
		// A resent row also counts as delivered when the first attempt
		// shows up late.
		if prev, ok, err := tx.Meta(resentKey(r.ID)); err != nil {
			return err
		} else if ok && prev != resentNoToken && prev != "" {
			markers[e.marker(prev)] = true
		}
		for _, m := range msgs {
			if m.Marker != "" && markers[m.Marker] {
				found = m.ID
				break
			}
		}
		if found == "" && !e.markersTrusted() && len(r.BodyHash) > 0 {
			for _, m := range msgs {
				if !m.ByOwner || m.Hash != hash || m.Created.Before(fromMS(r.CreatedAt).Add(-time.Minute)) {
					continue
				}
				own, err := tx.IsOwn(r.ConvID, m.ID, nil, 0)
				if err != nil {
					return err
				}
				if !own {
					found = m.ID
					break
				}
			}
		}
		if found != "" {
			return tx.MarkSent(r.ID, found)
		}
		return nil
	})
	if err != nil {
		e.logf("resolve %d: %v", id, err)
		return
	}
	if found != "" {
		e.logf("unknown send %s resolved by read-back: delivered", r.DeliveryKey)
		e.mu.Lock()
		delete(e.unknown, id)
		e.mu.Unlock()
		e.wakeRunner(r.ConvID)
		e.wakeSender()
		return
	}
	e.mu.Lock()
	u.tries++
	tries := u.tries
	u.next = now.Add(e.t.ResolveDelay << min(tries, 4))
	absentAt := u.since.Add(e.t.ResolveAbsent)
	if tries >= e.t.ResolveTries && now.Before(absentAt) && u.next.Before(absentAt) {
		u.next = absentAt
	}
	e.mu.Unlock()
	if tries < e.t.ResolveTries || now.Before(absentAt) {
		return
	}
	// Authoritatively absent.
	resent := false
	err = e.update(func(tx *store.Tx) error {
		resent = false
		text := textUnconfirmed
		if r.Kind == store.OutFinal || r.Kind == store.OutMirror {
			_, done, err := tx.Meta(resentKey(r.ID))
			if err != nil {
				return err
			}
			text = textUnconfirmedRetried
			if !done {
				tok := r.AttemptToken
				if tok == "" {
					tok = resentNoToken
				}
				if err := tx.SetMeta(resentKey(r.ID), tok); err != nil {
					return err
				}
				resent = true
				return tx.Requeue(r.ID, 0) // BeginSend gives a fresh token and marker
			}
		}
		if err := tx.Drop(r.ID, "not found by read-back"); err != nil {
			return err
		}
		if r.Kind == store.OutAck || strings.HasPrefix(r.DeliveryKey, "unconfirmed:") {
			return nil
		}
		return e.enqueueNotice(tx, r.ConvID, "unconfirmed:"+r.DeliveryKey, text)
	})
	if err != nil {
		e.logf("settle unknown %d: %v", id, err)
		return
	}
	if resent {
		e.logf("unknown send %s not found after %d read-backs over %s; sending it again once", r.DeliveryKey, tries, now.Sub(u.since).Round(time.Second))
	} else {
		e.logf("unknown send %s not found after %d read-backs; dropped", r.DeliveryKey, tries)
	}
	e.mu.Lock()
	delete(e.unknown, id)
	e.mu.Unlock()
	e.wakeRunner(r.ConvID)
	e.wakeSender()
}

// resentKey is the engine meta key recording that an absent final or mirror
// row was sent again (value: the first attempt's token, or resentNoToken).
func resentKey(id int64) string { return fmt.Sprintf("%s%d", metaResentPrefix, id) }

const (
	metaResentPrefix = "engine_resent:"
	resentNoToken    = "-"
)

// filePart renders the message that carries an uploaded outbound file.
func (e *Engine) filePart(ctx context.Context, f store.OutFile) (channel.Part, error) {
	var ref fileRef
	if err := json.Unmarshal([]byte(f.ExtItemID), &ref); err != nil {
		return channel.Part{}, fmt.Errorf("upload reference: %w", err)
	}
	name := firstNonEmpty(ref.Name, filepath.Base(f.LocalPath))
	parts, err := e.ch.Render(ctx, channel.RichDoc{Label: labelCodex, Markdown: "📎 " + name,
		Files: []channel.FileRef{{Kind: ref.Kind, Name: name, Size: ref.Size, ContentType: ref.ContentType, Ref: ref.Ref}}})
	if err != nil {
		return channel.Part{}, err
	}
	if len(parts) != 1 {
		return channel.Part{}, fmt.Errorf("file message rendered as %d parts", len(parts))
	}
	return parts[0], nil
}

// maxUploadFailures bounds transient upload failures of one file.
const maxUploadFailures = 8

// upload uploads an outbound file (resuming a saved session) and records
// the uploaded item; the row is then sent like any other.
func (e *Engine) upload(ctx context.Context, r store.Outbox, f store.OutFile) {
	e.mu.Lock()
	if cs := e.convs[r.ConvID]; cs != nil {
		cs.sending = true
	}
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		if cs := e.convs[r.ConvID]; cs != nil {
			cs.sending = false
		}
		e.mu.Unlock()
		e.wakeSender()
	}()
	name := filepath.Base(f.LocalPath)
	var sess *channel.UploadSession
	if f.UploadURL != "" && (f.UploadExpiresAt == 0 || fromMS(f.UploadExpiresAt).After(e.now().Add(time.Minute))) {
		sess = &channel.UploadSession{URL: f.UploadURL, ExpiresAt: fromMS(f.UploadExpiresAt), Offset: f.UploadedBytes}
	}
	save := func(s channel.UploadSession) error {
		var exp int64
		if !s.ExpiresAt.IsZero() {
			exp = ms(s.ExpiresAt)
		}
		return e.update(func(tx *store.Tx) error { return tx.SetUpload(r.ID, s.URL, exp, s.Offset, "") })
	}
	ref, err := e.ch.UploadFile(ctx, f.LocalPath, name, sess, save)
	if ctx.Err() != nil {
		return
	}
	now := e.now()
	var rej *channel.Rejected
	th, throttled := channel.AsThrottled(err)
	switch {
	case err == nil:
		data, _ := json.Marshal(fileRef{Kind: ref.Kind, Name: name, Size: ref.Size, ContentType: ref.ContentType, Ref: ref.Ref})
		if err := e.update(func(tx *store.Tx) error { return tx.SetUpload(r.ID, "", 0, f.Size, string(data)) }); err != nil {
			e.logf("record upload %s: %v", r.DeliveryKey, err)
		}
		e.mu.Lock()
		delete(e.uploadAt, r.ID)
		e.mu.Unlock()
		e.forgetFailures(r.ID)
	case throttled:
		if th.Scope == channel.ScopeAccount || th.Scope == channel.ScopeUnknown {
			e.setGate(e.gWrite, now.Add(e.retryAfter(th)), "upload throttled")
		}
		e.retryUpload(r.ID, now.Add(e.retryAfter(th)))
	case errors.As(err, &rej) && rej.Permanent:
		e.logf("upload %s rejected: %v", r.DeliveryKey, err)
		e.dropFile(r, name)
	default:
		n := e.failure(r.ID)
		if n >= maxUploadFailures {
			e.logf("upload %s failed %d times: %v", r.DeliveryKey, n, err)
			e.dropFile(r, name)
			return
		}
		back := e.t.Hot << min(n, 8)
		if back > e.t.MaxBackoff {
			back = e.t.MaxBackoff
		}
		e.logf("upload %s failed (%d): %v; retry in %s", r.DeliveryKey, n, err, back)
		e.retryUpload(r.ID, now.Add(back))
	}
}

func (e *Engine) retryUpload(id int64, at time.Time) {
	e.mu.Lock()
	e.uploadAt[id] = at
	e.mu.Unlock()
	d := at.Sub(e.now())
	if d < 0 {
		d = 0
	}
	time.AfterFunc(d, e.wakeSender)
}

// dropFile gives up on an outbound file with one visible notice.
func (e *Engine) dropFile(r store.Outbox, name string) {
	err := e.update(func(tx *store.Tx) error {
		if err := tx.Drop(r.ID, "file not sent"); err != nil {
			return err
		}
		return e.enqueueNotice(tx, r.ConvID, "file-failed:"+r.DeliveryKey, "The file "+name+" could not be sent.")
	})
	if err != nil {
		e.logf("drop file %s: %v", r.DeliveryKey, err)
	}
	e.mu.Lock()
	delete(e.uploadAt, r.ID)
	e.mu.Unlock()
	e.forgetFailures(r.ID)
	e.wakeRunner(r.ConvID)
	e.wakeSender()
}
