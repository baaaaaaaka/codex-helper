package engine

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// After an account read throttle (S3), the control and hot polls are
// stretched readStretch times for readStretchFor after the gate opens.
const (
	readStretch    = 4
	readStretchFor = 5 * time.Minute
)

// readThrottled records an account read throttle of a request that started
// at started: the read gate closes until resume, the read pacer steps down
// (pace.go) and, in scan mode, the activity scan gets the first read when
// the gate opens.
func (e *Engine) readThrottled(started, resume time.Time, reason string) {
	e.setGate(e.gRead, resume, reason)
	if e.scanner != nil {
		e.scanFirst.Store(true)
		wake(e.scanWake)
	}
	if cut, rate := e.readTokens.throttled(started, resume); cut {
		e.logf("account reads paced to %.2f/s", rate)
	}
}

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
	// The loop sleeps until the earliest next read (between Tick and Hot)
	// instead of waking every Tick: an idle helper reads every few
	// seconds, and each wake walks every conversation. Whatever moves a
	// next read earlier wakes it (wakeReader); a missed wake costs at most
	// Hot.
	timer := time.NewTimer(e.t.Tick)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-e.readWake:
		}
		wait, ok := e.dispatchReads(ctx, jobs)
		if !ok {
			return
		}
		timer.Reset(wait)
	}
}

// dispatchReads hands the due reads to the readers and returns how long
// the receiver loop may sleep; false when ctx ended.
func (e *Engine) dispatchReads(ctx context.Context, jobs chan<- int64) (time.Duration, bool) {
	now := e.now()
	if e.stopping.Load() {
		return e.t.Tick, true // shutdown: no chat is read again
	}
	if until := e.gate(e.gRead); now.Before(until) {
		return e.readSleep(until.Sub(now)), true
	}
	if e.scanFirst.Load() {
		// After a read throttle the activity scan, which serves every
		// chat, gets the first read (scanLoop clears this).
		return e.t.Tick, true
	}
	due, live := e.orderReads(now)
	for i, id := range due {
		if ok, wait := e.readTokens.take(); !ok {
			// Out of read budget: the rest wait for a token, live reads
			// still first.
			e.mu.Lock()
			for _, rest := range due[i:] {
				if cs := e.convs[rest]; cs != nil {
					cs.reading = false
				}
			}
			e.mu.Unlock()
			return e.readSleep(wait), true
		}
		select {
		case jobs <- id:
		case <-ctx.Done():
			return 0, false
		}
		if live[i] {
			e.liveRun = min(e.liveRun+1, liveReadsInARow)
		} else {
			e.liveRun = 0
		}
	}
	return e.readSleep(e.untilNextRead(e.now())), true
}

// readSleep bounds a receiver loop sleep to [Tick, Hot].
func (e *Engine) readSleep(d time.Duration) time.Duration {
	return max(e.t.Tick, min(e.t.Hot, d))
}

// untilNextRead is the time until the earliest next read of a conversation
// that is not being read (Hot when there is none).
func (e *Engine) untilNextRead(now time.Time) time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	d := e.t.Hot
	for _, cs := range e.convs {
		if !e.readableLocked(cs) {
			continue
		}
		d = min(d, cs.nextRead.Sub(now))
	}
	return d
}

// readableLocked reports whether a conversation is read at its next read
// time (not being read already, and in a state that is read).
func (e *Engine) readableLocked(cs *convState) bool {
	if cs.chat == "" || cs.reading {
		return false
	}
	if cs.kind == store.KindWork && cs.state != store.ConvActive {
		return false // parked, closed, hold: zero reads
	}
	return !(cs.kind == store.KindControl && cs.state == store.ConvClosed)
}

// liveReadsInARow is how many live reads the receiver dispatches in a row
// while background reads wait: every fourth read then goes to the oldest
// background read, so owner activity that alone fills the read budget (many
// running turns) does not starve the start-up reads, read-backs and safety
// reads. A live read waits for at most one background read.
const liveReadsInARow = 3

// dueReads returns the conversations whose next read is due in the order
// they are read (orderReads) and marks them as being read.
func (e *Engine) dueReads(now time.Time) []int64 {
	due, _ := e.orderReads(now)
	return due
}

// orderReads returns the conversations whose next read is due, in the
// order they are read, with whether each is a live read, and marks them as
// being read.
//
// Live reads are what the owner waits for: the control chat, a work chat
// with a running turn or recent owner activity (polledLocked), and a chat
// the activity scan showed with a new message that is not this helper's
// own post (scanLive). They go before the background reads: the start-up
// read of every chat, the read-back of a chat whose newest message is the
// helper's own post (catch-up), and the safety reads of idle chats. A
// catch-up queues hundreds of background reads at the account's read pace;
// the owner's next message must not wait behind them (N-3: the control chat
// every 5 s regardless of the backlog, never starved). Background reads
// still get every fourth read while live ones wait (liveReadsInARow).
//
// The split applies in scan mode only (liveReadLocked). Within each class
// a change the scan showed goes before timed reads, the oldest flag first
// (S7b), then the most overdue first: under a short read budget a chat
// polled often (the control chat) must not starve one that has waited
// longer.
func (e *Engine) orderReads(now time.Time) ([]int64, []bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	var live, bg []int64
	for id, cs := range e.convs {
		if !e.readableLocked(cs) || now.Before(cs.nextRead) {
			continue
		}
		cs.reading = true
		if e.liveReadLocked(cs, now) {
			live = append(live, id)
		} else {
			bg = append(bg, id)
		}
	}
	byAge := func(due []int64) {
		sort.Slice(due, func(i, j int) bool {
			a, b := e.convs[due[i]], e.convs[due[j]]
			if a.scanDue != b.scanDue {
				return a.scanDue // a change the scan showed goes before timed reads
			}
			if a.scanDue && !a.scanSince.Equal(b.scanSince) {
				return a.scanSince.Before(b.scanSince) // the oldest flag first (S7b)
			}
			if !a.nextRead.Equal(b.nextRead) {
				return a.nextRead.Before(b.nextRead)
			}
			return due[i] < due[j]
		})
	}
	byAge(live)
	byAge(bg)
	due := make([]int64, 0, len(live)+len(bg))
	isLive := make([]bool, 0, len(live)+len(bg))
	run := e.liveRun
	for len(live) > 0 || len(bg) > 0 {
		if len(bg) > 0 && (len(live) == 0 || run >= liveReadsInARow) {
			due, isLive, bg, run = append(due, bg[0]), append(isLive, false), bg[1:], 0
			continue
		}
		due, isLive, live, run = append(due, live[0]), append(isLive, true), live[1:], run+1
	}
	return due, isLive
}

// liveReadLocked reports whether a due read of a conversation is a live
// read (orderReads; caller holds e.mu). Without an activity scan (polling
// mode) every read is live: a chat's timed poll is the only way the
// owner's message in it is found, so the overdue-first order applies to
// all of them, as before.
func (e *Engine) liveReadLocked(cs *convState, now time.Time) bool {
	if e.scanner == nil {
		return true
	}
	return e.polledLocked(cs, now) || (cs.scanDue && cs.scanLive != 0)
}

// interval is the read interval of a conversation (caller holds e.mu).
func (e *Engine) intervalLocked(cs *convState, now time.Time) time.Duration {
	// While the account's reads were throttled within readStretchFor, the
	// frequent polls (control chat, hot chats) are stretched: they are
	// what the read budget is spent on.
	hot, control := e.t.Hot, e.t.Control
	if until := e.gates[e.gRead]; !until.IsZero() && now.Sub(until) < readStretchFor {
		hot, control = hot*readStretch, control*readStretch
	}
	if cs.kind == store.KindControl {
		return control
	}
	if cs.running != nil {
		return hot
	}
	if e.scanner != nil {
		// Scan mode: the activity scan finds new messages in idle chats;
		// a direct read is only a safety net.
		if now.Sub(cs.lastUser) < e.t.HotFor {
			return hot
		}
		return e.t.SafetyRead
	}
	idle := now.Sub(cs.lastActivity)
	switch {
	case idle < e.t.HotFor:
		return hot
	case idle < e.t.WarmFor:
		return e.t.Warm
	case idle < e.t.CoolFor:
		return e.t.Cool
	default:
		return e.t.Cold
	}
}

// nextReadLocked is the next timed read of a conversation read
// successfully at start (caller holds e.mu): one read interval later. In
// scan mode the read of an idle chat is a safety net, at most SafetyRead
// apart. A read that was not itself the scheduled safety read (safety: a
// start-up read, a read-back, a read the scan or the owner's activity
// caused) brings the next one forward by up to half the interval, by a
// fraction spread over the chat ids (safetyJitter). Chats read together, as
// a catch-up reads hundreds at the account's read pace, come back spread
// over half the interval instead of as one wave every SafetyRead, and stay
// spread: a safety read schedules the next exactly one interval later.
func (e *Engine) nextReadLocked(cs *convState, start, now time.Time, safety bool) time.Time {
	iv := e.intervalLocked(cs, now)
	if e.scanner == nil || e.polledLocked(cs, now) {
		cs.safeAt = time.Time{}
		return start.Add(iv)
	}
	next := start.Add(iv)
	if !safety {
		next = next.Add(-safetyJitter(cs.id, iv))
	}
	cs.safeAt = next
	return next
}

// safetyReadLocked reports whether a read starting now is the safety read
// nextReadLocked scheduled: nothing moved the chat's next read since, and
// the scan has not marked it (caller holds e.mu).
func safetyReadLocked(cs *convState) bool {
	return !cs.safeAt.IsZero() && cs.nextRead.Equal(cs.safeAt) && !cs.scanDue
}

// safetyJitter is how much sooner than period the first safety read after
// other reads comes, in [0, period/2): spread over the chat ids by the
// golden ratio, so consecutive ids land far apart.
func safetyJitter(id int64, period time.Duration) time.Duration {
	_, frac := math.Modf(math.Abs(float64(id)) * 0.6180339887498949)
	return time.Duration(frac * float64(period/2))
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
			e.wakeReader()
		}
	}
}

// read performs one Receive of a conversation and admits the window.
func (e *Engine) read(ctx context.Context, id int64) {
	e.mu.Lock()
	cs := e.convs[id]
	chat, cursor, gen := cs.chat, cs.cursor, cs.scanGen
	safety := safetyReadLocked(cs)
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
	defer e.wakeReader() // the next read time is set below
	cs.reading = false
	now := e.now()
	if err == nil {
		cs.failures = 0
		cs.nextRead = e.nextReadLocked(cs, start, now, safety)
		if cs.scanLive <= gen {
			cs.scanLive = 0 // read: the change was before the read started
		}
		if cs.nextRead.Before(now) || cs.scanGen != gen {
			// The scan marked the chat while this read ran: the change
			// may be newer than what it returned.
			cs.nextRead = now
		}
		cs.scanDue = cs.scanGen != gen
		if !cs.scanDue {
			cs.scanSince = time.Time{}
		}
		return
	}
	var tl *channel.WindowTooLarge
	if th, ok := channel.AsThrottled(err); ok {
		until := now.Add(e.retryAfter(th) + time.Duration(id%7)*e.t.Tick)
		cs.nextRead = until
		if cs.scanDue && th.Scope != channel.ScopeChat {
			// A flagged chat is due again the moment the account gate
			// reopens, and goes first then (S7b): the gate holds it.
			cs.nextRead = now
		}
		if th.Scope != channel.ScopeChat {
			e.mu.Unlock()
			e.readThrottled(start, until, "read throttled")
			e.mu.Lock()
		}
		e.logf("read %s throttled for %s (%s scope)", cs.label, e.retryAfter(th), th.Scope)
		return
	}
	if errors.As(err, &tl) {
		label := cs.label
		e.mu.Unlock()
		moved := e.skipBacklog(id, cursor, tl)
		e.mu.Lock()
		cs.failures = 0
		if moved {
			cs.nextRead = now
		} else {
			// Nothing to skip to: retry rarely, the read is expensive.
			cs.nextRead = now.Add(e.t.SafetyRead)
		}
		e.logf("read %s: %v (skipped: %v)", label, err, moved)
		return
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
	// delegation is the id of a delegation request this engine takes (inbox).
	delegation string
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
		stg      *admitStage
	)
	inbox := id == e.inboxID && id != 0
	var cancels []string
	err := e.update(func(tx *store.Tx) error {
		jobs, newTurns, becameGr, trip, activity, hasFiles, cancels = nil, false, false, false, false, false, nil
		stg = &admitStage{recent: map[string]recentPrompt{}} // a retried attempt starts over
		var cls []classified
		byID := map[string]*classified{}
		var iw *inboxWindow
		if inbox {
			var err error
			if iw, err = e.scanInbox(tx, fresh, now); err != nil {
				return err
			}
			cancels = iw.cancel
		}
		for _, m := range fresh {
			c, keep, err := e.classify(tx, id, kind, group, m)
			if err != nil {
				return err
			}
			if inbox && keep {
				if c, err = e.classifyInbox(tx, iw, c, now); err != nil {
					return err
				}
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
			if c.suspect && e.breakerHit(id, now, stg) {
				trip = true
			}
			switch {
			case c.delegation != "" && a.TurnID != 0:
				activity, newTurns = true, true
				if err := e.admitDelegation(tx, id, a, c); err != nil {
					return err
				}
			case c.delegation != "":
				// Held (the inbox is paused): not taken; the request expires
				// at the requester.
				if err := tx.SetInbound(a.InboundID, store.InIgnored, 0); err != nil {
					return err
				}
				if err := tx.SetMeta(metaDelegPrefix+c.delegation, "paused"); err != nil {
					return err
				}
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
				dup, err := e.ackPrompt(tx, id, a, c, stg)
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
	e.applyStage(cs, stg, now)
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
	if len(cancels) > 0 {
		e.cancelDelegations(ctx, cancels)
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

// skipBacklog handles a receive window too large to admit in order (review
// H3): it posts one notice in the chat saying what was skipped and how to
// see it, and moves the cursor to the channel's SkipCursor in the same
// transaction, so the chat is not left deaf and the walk is not repeated.
// It reports whether the cursor moved.
func (e *Engine) skipBacklog(id int64, cursor string, tl *channel.WindowTooLarge) bool {
	e.mu.Lock()
	cs := e.convs[id]
	key := e.streamKey(cs)
	e.mu.Unlock()
	move := tl.SkipCursor != "" && tl.SkipCursor != cursor
	err := e.update(func(tx *store.Tx) error {
		base := fmt.Sprintf("skip:%d:%s", id, cursor)
		if err := e.enqueueNotice(tx, id, base, textSkippedBacklog(tl, move)); err != nil {
			return err
		}
		if !move {
			return nil
		}
		_, err := tx.AdmitWindow(key, id, nil, tl.SkipCursor)
		return err
	})
	if err != nil {
		e.logf("skip backlog of conversation %d: %v", id, err)
		return false
	}
	if move {
		e.mu.Lock()
		cs.cursor, cs.seeded = tl.SkipCursor, true
		e.mu.Unlock()
	}
	e.wakeSender()
	return move
}

// textSkippedBacklog says how many messages were skipped, from when, and
// how to see them.
func textSkippedBacklog(tl *channel.WindowTooLarge, moved bool) string {
	var b strings.Builder
	b.WriteString("Too many new messages arrived in this chat for the helper to read them in order")
	const layout = "2006-01-02 15:04"
	var span []string
	if tl.Seen > 0 {
		span = append(span, fmt.Sprintf("at least %d messages", tl.Seen))
	}
	if !tl.OldestSeen.IsZero() && !tl.NewestSeen.IsZero() {
		span = append(span, fmt.Sprintf("from %s or earlier to %s UTC", tl.OldestSeen.UTC().Format(layout), tl.NewestSeen.UTC().Format(layout)))
	}
	if len(span) > 0 {
		b.WriteString(" (" + strings.Join(span, ", ") + ")")
	}
	if moved {
		b.WriteString(". They were skipped and Codex will not answer them; messages sent from now on are handled normally. Scroll up in this chat to read them, and resend anything you still need.")
	} else {
		b.WriteString(". New messages here are not answered for now; the helper will try again later. Scroll up in this chat to read them.")
	}
	return b.String()
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
				e.rememberOwnPost(conv, m.ID)
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
		// The control chat and the delegation inbox ignore non-owners, even
		// when they mention Codex (R-27).
		if kind != store.KindControl && conv != e.inboxID && (m.MentionsOwner || strings.Contains(strings.ToLower(m.Text), "@codex")) {
			return c, true, nil
		}
		c.in.Kind, c.chatter = store.InCommand, true
		return c, true, nil
	}
	own, err := tx.IsOwn(conv, m.ID, nil, 0)
	if err != nil {
		return c, false, err
	}
	if !own {
		// A post of the previous helper whose send it could not confirm
		// (no marker, no recorded id).
		if own, err = e.legacyOwn(tx, conv, m); err != nil {
			return c, false, err
		}
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
		// Inbox records: classifyInbox picks the requests this engine runs.
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
	if group && !strings.Contains(strings.ToLower(text), "@codex") && !hasVoice(m.Files) {
		// With other people in the chat only @codex, or a voice or video
		// clip from the owner, addresses Codex.
		c.in.Kind, c.chatter = store.InCommand, true
	}
	return c, true, nil
}

func hasVoice(files []channel.FileRef) bool {
	for _, f := range files {
		if f.Kind == channel.FileVoice {
			return true
		}
	}
	return false
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
func (e *Engine) ackPrompt(tx *store.Tx, conv int64, a store.Admitted, c *classified, stg *admitStage) (bool, error) {
	norm := normalize(c.in.Body)
	at := c.msg.Created
	e.mu.Lock()
	cs := e.convs[conv]
	prev, seen := stg.recent[norm]
	if !seen {
		prev, seen = cs.recent[norm]
	}
	e.mu.Unlock()
	dup := seen && at.Sub(prev.at) < e.t.DuplicateFor && at.Sub(prev.at) >= -e.t.DuplicateFor && norm != ""
	// As in v0.1.20 (recentDuplicateSessionPrompt): a message with files or
	// pasted images is never a duplicate (the same caption with another
	// screenshot is a new request), and only an earlier prompt that is still
	// open or completed suppresses a repeat; resending one that failed, was
	// interrupted or cancelled runs it.
	if dup && len(c.msg.Files) > 0 {
		dup = false
	}
	if dup {
		var err error
		if dup, err = e.earlierSuppresses(tx, prev.inbound); err != nil {
			return false, err
		}
	}
	if !dup {
		stg.recent[norm] = recentPrompt{at: at, inbound: a.InboundID}
		if at.After(stg.latest) {
			stg.latest = at
		}
	} else {
		stg.lastDup = a.InboundID
	}
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
	text := textAccepted
	var (
		running   string
		isRunning bool
		ahead     bool
		before    int // other queued requests ahead of this one
	)
	// A turn the previous process left open is not running: it is
	// settled, with its own notice, before the next turn starts.
	e.mu.Lock()
	live := open[:0]
	for _, t := range open {
		if !e.orphans[t.ID] {
			live = append(live, t)
		}
	}
	e.mu.Unlock()
	for _, t := range live {
		if t.ID >= a.TurnID {
			continue
		}
		ahead = true
		// A turn waiting to retry at Codex capacity is the current one.
		if t.State == store.TurnQueued && t.RetryDeadline == 0 {
			before++
			continue
		}
		if isRunning {
			continue
		}
		isRunning = true
		if in, ok, err := tx.Inbound(t.InboundID); err != nil {
			return false, err
		} else if ok {
			running = in.Body
		}
	}
	if ahead {
		text = textQueuedAck(running, isRunning, before)
	}
	msgs, err := e.noticeReply(store.OutAck, fmt.Sprintf("ack:%d", a.InboundID), text, c.quoteID)
	if err != nil {
		return false, err
	}
	_, err = tx.Enqueue(conv, msgs)
	return false, err
}

// earlierSuppresses reports whether the earlier prompt (inbound id) still
// suppresses a repeat: its turn is open or completed.
func (e *Engine) earlierSuppresses(tx *store.Tx, inbound int64) (bool, error) {
	in, ok, err := tx.Inbound(inbound)
	if err != nil || !ok || in.TurnID == 0 {
		return ok && err == nil, err
	}
	tr, ok, err := tx.Turn(in.TurnID)
	if err != nil || !ok {
		return ok && err == nil, err
	}
	switch tr.State {
	case store.TurnFailed, store.TurnInterrupted, store.TurnCancelled, store.TurnNotStarted:
		return false, nil
	}
	return true, nil
}

func (e *Engine) ackHeld(tx *store.Tx, conv int64, a store.Admitted, c *classified) error {
	msgs, err := e.noticeReply(store.OutAck, fmt.Sprintf("ack:%d", a.InboundID), textHeld, c.quoteID)
	if err != nil {
		return err
	}
	_, err = tx.Enqueue(conv, msgs)
	return err
}

// admitStage holds the in-memory effects of one admit attempt. They are
// applied only after the transaction commits, so a failed or retried
// attempt neither marks its own prompts as seen (which would cancel them as
// duplicates on the retry) nor counts breaker hits twice.
type admitStage struct {
	recent  map[string]recentPrompt
	latest  time.Time // newest prompt time staged in recent
	lastDup int64
	hits    int // suspicious messages
}

// applyStage applies a committed admit's effects. Called with e.mu held.
func (e *Engine) applyStage(cs *convState, stg *admitStage, now time.Time) {
	if stg == nil {
		return
	}
	for k, r := range stg.recent {
		cs.recent[k] = r
	}
	if len(stg.recent) > 0 {
		for k, r := range cs.recent {
			if stg.latest.Sub(r.at) > e.t.DuplicateFor {
				delete(cs.recent, k)
			}
		}
	}
	if stg.lastDup != 0 {
		cs.lastDup = stg.lastDup
	}
	if stg.hits > 0 {
		cs.breaker = append(e.breakerWindow(cs, now), make([]time.Time, stg.hits)...)
		for i := len(cs.breaker) - stg.hits; i < len(cs.breaker); i++ {
			cs.breaker[i] = now
		}
	}
}

// breakerWindow returns the breaker hits still inside the window. Called
// with e.mu held.
func (e *Engine) breakerWindow(cs *convState, now time.Time) []time.Time {
	keep := cs.breaker[:0]
	for _, t := range cs.breaker {
		if now.Sub(t) < e.t.BreakerWindow {
			keep = append(keep, t)
		}
	}
	return keep
}

// breakerHit stages one suspicious message and reports a trip.
func (e *Engine) breakerHit(conv int64, now time.Time, stg *admitStage) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	cs := e.convs[conv]
	cs.breaker = e.breakerWindow(cs, now)
	stg.hits++
	return len(cs.breaker)+stg.hits >= e.t.BreakerCount && !cs.paused
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
	// reopen fires when a closed read gate opens: the scan goes first.
	reopen := time.NewTimer(time.Hour)
	reopen.Stop()
	defer reopen.Stop()
	// Every conversation is read once at start, so the scan only needs
	// what arrives from now on (with a margin for clock skew).
	mark := e.now().Add(-time.Minute)
	failures := 0
	var last time.Time // the last scan request
	waiting := false   // the scan waits for a read token (the receiver waits for it)
	for {
		reopened := false
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-reopen.C:
			reopened = true
		case <-e.scanWake:
		}
		if e.stopping.Load() {
			continue // shutdown: no chat is read again
		}
		now := e.now()
		if until := e.gate(e.gRead); now.Before(until) {
			reopen.Reset(until.Sub(now))
			waiting = false // a read throttle: the scan goes first when it ends
			continue
		}
		every := e.scanEvery(now)
		if !reopened && !last.IsZero() && now.Sub(last) < every {
			// One scan per interval: the tick (or a wake) right after
			// the scan the gate's reopening ran would spend a second
			// read of the scarce budget on the same change list.
			continue
		}
		if e.scanFirst.Load() && !waiting {
			// The scan serves every chat: after a read throttle it
			// goes first.
			e.readTokens.borrow()
		} else if ok, wait := e.readTokens.take(); !ok {
			// Out of read budget (S6): the scan takes the next token
			// and the chat reads wait for it, instead of borrowing
			// every interval and spending a small budget on scans.
			waiting = true
			e.scanFirst.Store(true)
			reopen.Reset(max(wait, e.t.Tick))
			continue
		}
		waiting = false
		last = now
		tick.Reset(every) // the cadence counts from this scan
		started := e.now()
		acts, err := e.scanner.ScanActivity(ctx, mark.Add(-scanOverlap))
		e.count(func(s *engineStats) { s.reads++ })
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			if th, ok := channel.AsThrottled(err); ok {
				until := e.now().Add(e.retryAfter(th))
				if th.Scope == channel.ScopeChat {
					e.setGate(e.gRead, until, "activity scan throttled")
				} else {
					e.readThrottled(started, until, "activity scan throttled")
				}
				reopen.Reset(until.Sub(e.now()))
				continue
			}
			e.scanFirst.Store(false)
			failures++
			if failures <= 3 || failures%100 == 0 {
				e.logf("activity scan failed (%d in a row): %v", failures, err)
			}
			continue
		}
		failures = 0
		e.scanFirst.Store(false)
		var woke bool
		mark, woke = e.markScanned(acts, mark, now)
		if woke {
			e.wakeReader()
		}
	}
}

// markScanned applies one activity scan: the chats it shows with a new
// newest message are marked for a read. It returns the advanced watermark
// and whether any chat was marked.
func (e *Engine) markScanned(acts []channel.ChatActivity, mark, now time.Time) (time.Time, bool) {
	woke := false
	e.mu.Lock()
	defer e.mu.Unlock()
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
		own := a.LastID != "" && slices.Contains(cs.ownPosts, a.LastID)
		if own && e.polledLocked(cs, now) {
			// The newest message is one this helper sent (S7): about
			// half the scan's reads went to such chats, mostly acks,
			// progress and finals in chats with a running or recent
			// request. Such a chat is read at its poll interval anyway;
			// a user message posted just before the helper's is read
			// by the next poll, which is not later than one poll
			// interval from now. A chat that is not polled is marked:
			// its next timed read is the safety read.
			if next := now.Add(e.intervalLocked(cs, now)); cs.nextRead.After(next) {
				cs.nextRead = next
				woke = true
			}
			continue
		}
		if !cs.scanDue {
			cs.scanSince = now
		}
		cs.scanDue = true
		cs.scanGen++
		if !own {
			// Not the helper's own post: a message the owner (or someone
			// else) sent, read at live priority (orderReads).
			cs.scanLive = cs.scanGen
		}
		if cs.nextRead.After(now) {
			cs.nextRead = now
		}
		woke = true
	}
	return mark, woke
}

// polledLocked reports whether a conversation is read at the hot or
// control interval without the scan: the control chat, and a work chat with
// a running request or recent owner activity (caller holds e.mu).
func (e *Engine) polledLocked(cs *convState, now time.Time) bool {
	return cs.kind == store.KindControl || cs.running != nil || now.Sub(cs.lastUser) < e.t.HotFor
}

// ownPostsKept is how many sent message ids a conversation remembers.
const ownPostsKept = 16

// rememberOwnPost records the id of a message this helper sent.
func (e *Engine) rememberOwnPost(conv int64, id string) {
	if id == "" {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if cs := e.convs[conv]; cs != nil {
		cs.ownPosts = append(cs.ownPosts, id)
		if n := len(cs.ownPosts); n > ownPostsKept {
			cs.ownPosts = append(cs.ownPosts[:0], cs.ownPosts[n-ownPostsKept:]...)
		}
	}
}

// scanEvery is the activity scan interval: stretched like the control and
// hot polls (S3) while the account's reads were throttled recently.
func (e *Engine) scanEvery(now time.Time) time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	if until := e.gates[e.gRead]; !until.IsZero() && now.Sub(until) < readStretchFor {
		return e.t.ScanInterval * readStretch
	}
	return e.t.ScanInterval
}

// scanOverlap re-lists chats this far behind the watermark; repeats are
// skipped by message id. The chat list is eventually consistent: a chat's
// preview can appear after a newer one of another chat (review M6), so the
// overlap matches the receive overlap (2 min).
const scanOverlap = 2 * time.Minute
