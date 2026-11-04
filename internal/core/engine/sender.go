package engine

import (
	"context"
	"encoding/hex"
	"errors"
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
	if until := e.gate(gateAccountWrite); now.Before(until) {
		return until.Sub(now)
	}
	type job struct {
		row   store.Outbox
		chat  string
		token string
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
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].ConvID == ctl && rows[j].ConvID != ctl })
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
			if wait > 0 {
				soon(wait)
				continue
			}
			if ok, w := e.writeTokens.take(); !ok {
				soon(w)
				break
			}
			select {
			case sem <- struct{}{}:
			default:
				soon(e.t.Tick)
				return nil
			}
			tok, err := tx.BeginSend(r.ID)
			if err != nil {
				<-sem
				if errors.Is(err, store.ErrState) {
					continue
				}
				return err
			}
			jobs = append(jobs, job{row: r, chat: chat, token: tok})
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
			e.send(ctx, j.row, j.chat, j.token)
		}(j)
	}
	return again
}

// marker is the hidden marker of one send attempt.
func (e *Engine) marker(token string) string { return e.prefix + "." + token }

// send performs one Send and records its outcome.
func (e *Engine) send(ctx context.Context, r store.Outbox, chat, token string) {
	p := channel.Part{Index: max(r.Part, 1), Count: max(r.Parts, 1), Body: r.Body, Hash: hex.EncodeToString(r.BodyHash),
		Mention: r.Mention, ReplyTo: r.ReplyTo, Size: len(r.Body)}
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
		e.noteUnknown(r.ID, now)
		e.wakeResolver()
	case throttled:
		at := now.Add(th.RetryAfter + time.Duration(r.ConvID%5)*e.t.Tick)
		if th.Scope == channel.ScopeAccount || th.Scope == channel.ScopeUnknown {
			e.setGate(gateAccountWrite, at, "send throttled")
		}
		e.requeue(r.ID, at)
	case errors.As(err, &rej) && rej.Permanent:
		e.logf("send %s to conversation %d rejected permanently: %v; dropped", r.DeliveryKey, r.ConvID, err)
		if err := e.update(func(tx *store.Tx) error { return tx.Drop(r.ID, clipText(err.Error(), 200)) }); err != nil {
			e.logf("drop %d: %v", r.ID, err)
		}
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
// trusted) becomes sent. One that is not found after ResolveTries reads is
// dropped with one visible notice: re-posting could duplicate it.
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
	if until := e.gate(gateAccountRead); now.Before(until) {
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
			u.next = now.Add(th.RetryAfter)
			e.mu.Unlock()
			if th.Scope != channel.ScopeChat {
				e.setGate(gateAccountRead, now.Add(th.RetryAfter), "read-back throttled")
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
		for _, m := range msgs {
			if marker != "" && m.Marker == marker {
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
	e.mu.Unlock()
	if tries < e.t.ResolveTries {
		return
	}
	// Not found: never re-post (it may still appear); tell the user once.
	e.logf("unknown send %s not found after %d read-backs; dropped", r.DeliveryKey, tries)
	err = e.update(func(tx *store.Tx) error {
		if err := tx.Drop(r.ID, "not found by read-back"); err != nil {
			return err
		}
		if r.Kind == store.OutAck || strings.HasPrefix(r.DeliveryKey, "unconfirmed:") {
			return nil
		}
		return e.enqueueNotice(tx, r.ConvID, "unconfirmed:"+r.DeliveryKey, textUnconfirmed)
	})
	if err != nil {
		e.logf("drop %d: %v", id, err)
		return
	}
	e.mu.Lock()
	delete(e.unknown, id)
	e.mu.Unlock()
	e.wakeRunner(r.ConvID)
	e.wakeSender()
}
