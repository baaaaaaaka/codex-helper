package engine

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// receiverLoop schedules chat reads from in-memory state: no store access
// and no request for a chat that is not due. Reads run on Readers workers.
func (e *Engine) receiverLoop(ctx context.Context) {
	jobs := make(chan int64)
	for i := 0; i < e.cfg.Readers; i++ {
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			for id := range jobs {
				e.read(ctx, id)
			}
		}()
	}
	defer close(jobs)
	tick := time.NewTicker(e.t.Tick)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		now := e.now()
		if until := e.gate(e.gRead); now.Before(until) {
			continue
		}
		due := e.dueReads(now)
		for i, id := range due {
			if ok, _ := e.readTokens.take(); !ok {
				// Out of read budget: the rest wait for the next tick,
				// still ordered by how long they are overdue.
				e.mu.Lock()
				for _, rest := range due[i:] {
					if cs := e.convs[rest]; cs != nil {
						cs.reading = false
					}
				}
				e.mu.Unlock()
				break
			}
			select {
			case jobs <- id:
			case <-ctx.Done():
				return
			}
		}
	}
}

// dueReads returns the conversations whose next read is due, most overdue
// first, and marks them as being read. The order matters when the account
// read budget is short: a chat polled often (the control chat) must not
// starve one that has waited longer.
func (e *Engine) dueReads(now time.Time) []int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	var due []int64
	for id, cs := range e.convs {
		if cs.chat == "" || cs.reading || now.Before(cs.nextRead) {
			continue
		}
		if cs.kind == store.KindWork && cs.state != store.ConvActive {
			continue // parked, closed, hold: zero reads
		}
		if cs.kind == store.KindControl && cs.state == store.ConvClosed {
			continue
		}
		cs.reading = true
		due = append(due, id)
	}
	sort.Slice(due, func(i, j int) bool {
		a, b := e.convs[due[i]], e.convs[due[j]]
		if !a.nextRead.Equal(b.nextRead) {
			return a.nextRead.Before(b.nextRead)
		}
		return due[i] < due[j]
	})
	return due
}

// interval is the read interval of a conversation (caller holds e.mu).
func (e *Engine) intervalLocked(cs *convState, now time.Time) time.Duration {
	if cs.kind == store.KindControl {
		return e.t.Control
	}
	if cs.running != nil {
		return e.t.Hot
	}
	if e.scanner != nil {
		// Scan mode: the activity scan finds new messages in idle chats;
		// a direct read is only a safety net.
		if now.Sub(cs.lastUser) < e.t.HotFor {
			return e.t.Hot
		}
		return e.t.SafetyRead
	}
	idle := now.Sub(cs.lastActivity)
	switch {
	case idle < e.t.HotFor:
		return e.t.Hot
	case idle < e.t.WarmFor:
		return e.t.Warm
	case idle < e.t.CoolFor:
		return e.t.Cool
	default:
		return e.t.Cold
	}
}

// heat marks activity in a conversation (own post, mirror).
func (e *Engine) heat(id int64) { e.heatAs(id, false) }

// heatUser marks owner activity (prompt, command, turn end, wake): the
// conversation is polled directly for HotFor even in scan mode.
func (e *Engine) heatUser(id int64) { e.heatAs(id, true) }

func (e *Engine) heatAs(id int64, user bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if cs := e.convs[id]; cs != nil {
		now := e.now()
		cs.lastActivity = now
		if user {
			cs.lastUser = now
		} else if e.scanner != nil && cs.kind == store.KindWork && cs.running == nil && now.Sub(cs.lastUser) >= e.t.HotFor {
			return // scan mode: our own posts do not make an idle chat hot
		}
		if next := now.Add(e.t.Hot); cs.nextRead.After(next) {
			cs.nextRead = next
		}
	}
}

// read performs one Receive of a conversation and admits the window.
func (e *Engine) read(ctx context.Context, id int64) {
	e.mu.Lock()
	cs := e.convs[id]
	chat, cursor := cs.chat, cs.cursor
	e.mu.Unlock()
	start := e.now()
	w, err := e.ch.Receive(ctx, chat, cursor)
	e.count(func(s *engineStats) { s.reads++ })
	if ctx.Err() != nil {
		return
	}
	if err == nil {
		err = e.admit(ctx, id, w)
	}
	if err == nil {
		e.confirmRestart(id)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	cs.reading = false
	now := e.now()
	if err == nil {
		cs.failures = 0
		cs.nextRead = start.Add(e.intervalLocked(cs, now))
		if cs.nextRead.Before(now) {
			cs.nextRead = now
		}
		return
	}
	var tl *channel.WindowTooLarge
	if th, ok := channel.AsThrottled(err); ok {
		until := now.Add(th.RetryAfter + time.Duration(id%7)*e.t.Tick)
		if th.Scope == channel.ScopeChat {
			cs.nextRead = until
		} else {
			cs.nextRead = until
			e.mu.Unlock()
			e.setGate(e.gRead, until, "read throttled")
			e.mu.Lock()
		}
		e.logf("read %s throttled for %s (%s scope)", cs.label, th.RetryAfter, th.Scope)
		return
	}
	if errors.As(err, &tl) {
		e.logf("read %s: %v", cs.label, err)
	}
	cs.failures++
	back := e.t.Hot << min(cs.failures, 10)
	if back > e.t.MaxBackoff {
		back = e.t.MaxBackoff
	}
	cs.nextRead = now.Add(back)
	if cs.failures <= 3 || cs.failures%20 == 0 {
		e.logf("read %s failed (%d in a row): %v", cs.label, cs.failures, err)
	}
}

// classified is a received message with its classification.
type classified struct {
	msg       channel.InboundMsg
	in        store.InboundMsg
	command   bool   // an owner command (control chat, or "helper …")
	suspect   bool   // counts toward the breaker
	ownMarker bool   // carried our own marker (self-test passed)
	quoteID   string // ext id to quote in replies (group chats)
	other     bool   // sent by someone else than the owner
	chatter   bool   // someone else's message not addressed to Codex
	resolved  bool   // resolved one of our unknown sends
}

// admit classifies and persists one receive window and acts on the newly
// admitted rows, all in one transaction. Commands run afterwards.
func (e *Engine) admit(ctx context.Context, id int64, w channel.Window) error {
	e.mu.Lock()
	cs := e.convs[id]
	label, kind, group := cs.label, cs.kind, cs.group
	var fresh []channel.InboundMsg
	now := e.now()
	for _, m := range w.Msgs {
		if _, ok := cs.seen[m.ID]; ok {
			continue
		}
		fresh = append(fresh, m)
	}
	unchanged := len(fresh) == 0 && w.Cursor == cs.cursor && cs.seeded
	e.mu.Unlock()
	if unchanged {
		return nil
	}
	var (
		jobs     []cmdJob
		newTurns bool
		becameGr bool
		trip     bool
		activity bool
		hasFiles bool
	)
	err := e.update(func(tx *store.Tx) error {
		jobs, newTurns, becameGr, trip, activity, hasFiles = nil, false, false, false, false, false
		var cls []classified
		byID := map[string]*classified{}
		for _, m := range fresh {
			c, keep, err := e.classify(tx, id, kind, group, m)
			if err != nil {
				return err
			}
			if c.resolved {
				newTurns = true // the conversation may be runnable now
			}
			if !keep {
				continue
			}
			if c.other && !group {
				group, becameGr = true, true
			}
			if c.in.Kind == store.InPrompt && !c.chatter && (c.in.Sender == store.SenderOwner || c.in.Sender == store.SenderOther) {
				c.in.Files = inboundFiles(m.Files)
				if len(c.in.Files) > 0 {
					hasFiles = true
				}
			}
			cls = append(cls, c)
		}
		msgs := make([]store.InboundMsg, 0, len(cls))
		for i := range cls {
			msgs = append(msgs, cls[i].in)
			byID[cls[i].in.ExtMsgID] = &cls[i]
		}
		admitted, err := tx.AdmitWindow(e.streamKey(cs), id, msgs, w.Cursor)
		if err != nil {
			return err
		}
		if becameGr {
			if err := tx.SetMeta(fmt.Sprintf("%s%d", metaGroupPrefix, id), "1"); err != nil {
				return err
			}
		}
		for _, a := range admitted {
			c := byID[a.ExtMsgID]
			if c == nil {
				continue
			}
			if c.suspect && e.breakerHit(id, now) {
				trip = true
			}
			switch {
			case c.chatter && a.State == store.InQueued:
				if err := tx.SetInbound(a.InboundID, store.InIgnored, 0); err != nil {
					return err
				}
			case a.Kind == store.InCommand && a.State == store.InQueued:
				activity = true
				jobs = append(jobs, cmdJob{conv: id, inbound: a.InboundID, ext: a.ExtMsgID, text: c.in.Body, quote: c.quoteID})
			case a.Kind == store.InPrompt && a.State == store.InHeld:
				activity = true
				if err := e.ackHeld(tx, id, a, c); err != nil {
					return err
				}
			case a.Kind == store.InPrompt && a.TurnID != 0:
				activity = true
				dup, err := e.ackPrompt(tx, id, a, c)
				if err != nil {
					return err
				}
				if !dup {
					newTurns = true
				}
			}
		}
		if trip {
			if err := e.tripBreaker(tx, id, label); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	e.mu.Lock()
	cs.cursor, cs.seeded = w.Cursor, true
	if becameGr {
		cs.group = true
	}
	for _, m := range fresh {
		cs.seen[m.ID] = now
	}
	for k, t := range cs.seen {
		if now.Sub(t) > 30*time.Minute {
			delete(cs.seen, k)
		}
	}
	if trip {
		cs.paused = true
	}
	e.mu.Unlock()
	if activity {
		e.heatUser(id)
	}
	e.wakeSender()
	if hasFiles {
		wake(e.asrWake)
	}
	if newTurns {
		e.wakeRunner(id)
	}
	for _, j := range jobs {
		select {
		case e.cmdQueue <- j:
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

func (e *Engine) streamKey(cs *convState) string {
	return store.ChatStreamKey(e.cfg.ChannelName, cs.chat)
}

// classify decides sender and kind of one message (pure, plus store reads
// for own-message recognition). keep=false drops it (deleted messages).
func (e *Engine) classify(tx *store.Tx, conv int64, kind string, group bool, m channel.InboundMsg) (classified, bool, error) {
	c := classified{msg: m, in: store.InboundMsg{ExtMsgID: m.ID, ExtTime: ms(m.Created), Body: m.Text}}
	switch m.Kind {
	case channel.KindDeleted:
		return c, false, nil
	case channel.KindSystem:
		c.in.Sender, c.in.Kind = store.SenderOther, store.InSystem
		return c, true, nil
	}
	c.in.Kind = store.InPrompt
	if m.Marker != "" {
		if strings.HasPrefix(m.Marker, e.prefix+".") {
			c.in.Sender, c.ownMarker = store.SenderSelf, true
			if err := e.markerSeen(tx); err != nil {
				return c, false, err
			}
			// An unknown send that came back through the chat is delivered.
			r, ok, err := tx.OutboxByToken(conv, strings.TrimPrefix(m.Marker, e.prefix+"."))
			if err != nil {
				return c, false, err
			}
			if ok && r.State == store.OutUnknown {
				if err := tx.MarkSent(r.ID, m.ID); err != nil {
					return c, false, err
				}
				c.resolved = true // the resolver drops its entry when it sees the row sent
			}
			return c, true, nil
		}
		// Another helper instance's marker.
		c.in.Sender = store.SenderPeer
		if !e.markersTrusted() {
			c.suspect = true
		}
		return c, true, nil
	}
	if !m.ByOwner {
		c.other = true
		c.in.Sender = store.SenderOther
		c.quoteID = m.ID
		if m.MentionsOwner || strings.Contains(strings.ToLower(m.Text), "@codex") {
			return c, true, nil
		}
		c.in.Kind, c.chatter = store.InCommand, true
		return c, true, nil
	}
	own, err := tx.IsOwn(conv, m.ID, nil, 0)
	if err != nil {
		return c, false, err
	}
	if own {
		c.in.Sender = store.SenderSelf
		return c, true, nil
	}
	if hash := hashBytes(m.Hash); len(hash) > 0 {
		rows, err := tx.OutboxByHash(conv, hash, ms(m.Created.Add(-24*time.Hour)))
		if err != nil {
			return c, false, err
		}
		for _, r := range rows {
			if r.State != store.OutSent || r.ExtMsgID == m.ID {
				c.in.Sender = store.SenderSelf // our message, its id not recorded yet
				return c, true, nil
			}
		}
		if len(rows) > 0 {
			// A copy of something we already posted under another id.
			c.in.Sender, c.in.Kind, c.suspect = store.SenderOwner, store.InEcho, true
			return c, true, nil
		}
	}
	c.in.Sender = store.SenderOwner
	if group {
		c.quoteID = m.ID
	}
	text := strings.TrimSpace(m.Text)
	if conv == e.inboxID && conv != 0 {
		// Delegation requests are not executed yet: recorded, never run.
		c.in.Kind, c.chatter = store.InCommand, true
		return c, true, nil
	}
	if helperShaped(text) {
		c.suspect = true // still runs: genuine look-alike text is allowed
	}
	if kind == store.KindControl || isHelperCommand(text) {
		c.in.Kind = store.InCommand
		return c, true, nil
	}
	if group && !strings.Contains(strings.ToLower(text), "@codex") {
		// With other people in the chat only @codex addresses Codex.
		c.in.Kind, c.chatter = store.InCommand, true
	}
	return c, true, nil
}

func hashBytes(h string) []byte {
	b, err := hex.DecodeString(h)
	if err != nil {
		return nil
	}
	return b
}

// helperShaped reports text that looks like a helper message.
func helperShaped(text string) bool {
	t := strings.TrimLeft(text, "🔧🤖⚠️✅⏳ \t")
	return strings.HasPrefix(t, "Helper:") || strings.HasPrefix(text, labelHelper)
}

// isHelperCommand reports a work-chat command ("helper …").
func isHelperCommand(text string) bool {
	low := strings.ToLower(strings.TrimSpace(text))
	return low == "helper" || strings.HasPrefix(low, "helper ")
}

// markerSeen records the passive marker self-test: one of our markers
// came back through the channel, so markers are preserved and a foreign
// marker identifies another helper instance.
func (e *Engine) markerSeen(tx *store.Tx) error {
	e.mu.Lock()
	done := e.preserved
	e.preserved = true
	e.mu.Unlock()
	if done {
		return nil
	}
	return tx.SetMeta(metaMarkerSelfTest, "preserved")
}

func (e *Engine) markersTrusted() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.preserved
}

// ackPrompt acknowledges a newly admitted prompt (or ignores a duplicate).
// It reports whether the prompt was a duplicate.
func (e *Engine) ackPrompt(tx *store.Tx, conv int64, a store.Admitted, c *classified) (bool, error) {
	norm := normalize(c.in.Body)
	at := c.msg.Created
	e.mu.Lock()
	cs := e.convs[conv]
	prev, seen := cs.recent[norm]
	dup := seen && at.Sub(prev.at) < e.t.DuplicateFor && at.Sub(prev.at) >= -e.t.DuplicateFor && norm != ""
	if !dup {
		cs.recent[norm] = recentPrompt{at: at, inbound: a.InboundID}
		for k, r := range cs.recent {
			if at.Sub(r.at) > e.t.DuplicateFor {
				delete(cs.recent, k)
			}
		}
	} else {
		cs.lastDup = a.InboundID
	}
	e.mu.Unlock()
	if dup {
		msgs, err := e.noticeReply(store.OutNotice, fmt.Sprintf("dup:%d", a.InboundID), textDuplicate, c.quoteID)
		if err != nil {
			return true, err
		}
		return true, tx.FinishTurn(a.TurnID, store.TurnCancelled, "duplicate", msgs)
	}
	open, err := tx.OpenTurnsOf(conv)
	if err != nil {
		return false, err
	}
	ahead := 0
	for _, t := range open {
		if t.ID < a.TurnID {
			ahead++
		}
	}
	text := "Request accepted."
	if ahead > 0 {
		text += " It is queued behind " + plural(ahead, "earlier request", "earlier requests") + "."
	}
	msgs, err := e.noticeReply(store.OutAck, fmt.Sprintf("ack:%d", a.InboundID), text, c.quoteID)
	if err != nil {
		return false, err
	}
	_, err = tx.Enqueue(conv, msgs)
	return false, err
}

func (e *Engine) ackHeld(tx *store.Tx, conv int64, a store.Admitted, c *classified) error {
	msgs, err := e.noticeReply(store.OutAck, fmt.Sprintf("ack:%d", a.InboundID), textHeld, c.quoteID)
	if err != nil {
		return err
	}
	_, err = tx.Enqueue(conv, msgs)
	return err
}

// breakerHit counts one suspicious message and reports a trip.
func (e *Engine) breakerHit(conv int64, now time.Time) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	cs := e.convs[conv]
	keep := cs.breaker[:0]
	for _, t := range cs.breaker {
		if now.Sub(t) < e.t.BreakerWindow {
			keep = append(keep, t)
		}
	}
	cs.breaker = append(keep, now)
	return len(cs.breaker) >= e.t.BreakerCount && !cs.paused
}

// tripBreaker pauses auto-start in a conversation and notifies once.
func (e *Engine) tripBreaker(tx *store.Tx, conv int64, label string) error {
	c, ok, err := tx.Conversation(conv)
	if err != nil || !ok || c.PausedAt != 0 {
		return err // already paused: it was notified then
	}
	if err := tx.Pause(conv, "possible self-echo loop"); err != nil {
		return err
	}
	base := fmt.Sprintf("pause:%d:%d", conv, e.now().UnixMilli())
	if err := e.enqueueNotice(tx, conv, base, "Paused auto-start of new requests: several messages that look like helper output arrived in a short time (possible self-echo loop). Messages are still read and held. Send `helper unpause` to resume."); err != nil {
		return err
	}
	if e.controlID != 0 && e.controlID != conv {
		if err := e.enqueueNotice(tx, e.controlID, base+":control", fmt.Sprintf("Paused auto-start in %s (possible self-echo loop). Send `helper unpause` in that chat to resume.", label)); err != nil {
			return err
		}
	}
	e.logf("breaker tripped in %s", label)
	return tx.MarkPauseNotified(conv)
}

// scanLoop runs the activity scan (scan mode): one account-wide request per
// ScanInterval lists the chats with new messages, and only those are read.
// Parked and closed conversations are not woken by it (they get no reads).
func (e *Engine) scanLoop(ctx context.Context) {
	tick := time.NewTicker(e.t.ScanInterval)
	defer tick.Stop()
	// Every conversation is read once at start, so the scan only needs
	// what arrives from now on (with a margin for clock skew).
	mark := e.now().Add(-time.Minute)
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		now := e.now()
		if until := e.gate(e.gRead); now.Before(until) {
			continue
		}
		e.readTokens.borrow()
		acts, err := e.scanner.ScanActivity(ctx, mark.Add(-scanOverlap))
		e.count(func(s *engineStats) { s.reads++ })
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			if th, ok := channel.AsThrottled(err); ok {
				e.setGate(e.gRead, e.now().Add(th.RetryAfter), "activity scan throttled")
				continue
			}
			failures++
			if failures <= 3 || failures%100 == 0 {
				e.logf("activity scan failed (%d in a row): %v", failures, err)
			}
			continue
		}
		failures = 0
		e.mu.Lock()
		for _, a := range acts {
			if a.Last.After(mark) {
				mark = a.Last
			}
			id, ok := e.byChat[a.Chat]
			if !ok {
				continue
			}
			cs := e.convs[id]
			if cs == nil || cs.scanLast == a.LastID {
				continue
			}
			cs.scanLast = a.LastID
			if cs.kind == store.KindWork && cs.state != store.ConvActive {
				continue // parked or closed: no reads until woken
			}
			// Our own posts also change the newest message; reading after
			// them costs one read but never misses a user message posted
			// just before.
			if cs.nextRead.After(now) {
				cs.nextRead = now
			}
		}
		e.mu.Unlock()
	}
}

// scanOverlap re-lists chats around the watermark; repeats are skipped by
// message id.
const scanOverlap = 5 * time.Second
