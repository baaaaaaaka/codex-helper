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
	yielded := false
	for i, id := range due {
		if !live[i] && e.readWaiters.Load() > 0 {
			// A chat edit waits for a read token (edits.go waitRead): it
			// goes before the background reads, after the live ones.
			e.mu.Lock()
			if cs := e.convs[id]; cs != nil {
				cs.reading = false
			}
			e.mu.Unlock()
			yielded = true
			continue
		}
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
	if yielded {
		return e.t.Tick, true // waitRead wakes the receiver when it is done
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
	return !cs.reading && readStateLocked(cs)
}

// readStateLocked reports whether a conversation is in a state that is
// read: bound to a chat, and not a parked, closed or held work chat or a
// closed control chat (caller holds e.mu).
func readStateLocked(cs *convState) bool {
	if cs.chat == "" {
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
			if fa, fb := due[i] == e.controlID && !a.readOnce, due[j] == e.controlID && !b.readOnce; fa != fb {
				// The control chat's first read goes first: until it,
				// this process starts no turn and sends nothing
				// (controlReadLocked), not even in a control chat it
				// just created behind a start-up read of every chat.
				return fa
			}
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
		if e.scanBlind > 0 && cs.kind == store.KindWork {
			return e.scanBlind // the scan is failing (scanFallback)
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
	if err == nil {
		err = e.currentParticipants(ctx, id, chat, &w)
	}
	e.count(func(s *engineStats) { s.reads++; s.read(err, e.now()) })
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
	defer func() {
		// The next read time is set below. The receiver loop wakes at
		// least every max(Tick, Hot) and then looks at this chat again,
		// so only a sooner next read needs a wake: an idle chat's safety
		// read does not wake the loop for a read ten minutes away.
		if cs.nextRead.Before(now.Add(max(e.t.Tick, e.t.Hot))) {
			e.wakeReader()
		}
	}()
	if err == nil {
		cs.failures = 0
		if !cs.readOnce && cs.firstRead != nil {
			cs.readOnce = true
			close(cs.firstRead) // reconcileTurn waits for it
			if id == e.controlID || e.tookOver != "" {
				e.runnerAll = true // turns wait for it (readyLocked)
				wake(e.runnerWake)
			}
			if id == e.controlID {
				wake(e.senderWake) // sends wait for it (controlReadLocked)
			}
		}
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
		// Keep the chat out of the read loop until skipBacklog has moved
		// the cursor: a read started now would walk the too-large window
		// again from the old cursor.
		cs.reading = true
		e.mu.Unlock()
		moved := e.skipBacklog(id, cursor, tl)
		e.mu.Lock()
		cs.reading = false
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
	receivedAt int64 // admission transaction time, persisted in the receipt body
	confirmed  bool  // initial receipt owns the user label
	msg        channel.InboundMsg
	in         store.InboundMsg
	control    bool   // received in the control chat
	cmd        parsed // the owner's command as parsed at admission (parse.go)
	suspect    bool   // counts toward the breaker
	ownMarker  bool   // carried our own marker (self-test passed)
	sibling    bool   // our marker, not our send: another copy of this store (takeover.go)
	quoteID    string // ext id to quote in replies (group chats)
	other      bool   // sent by someone else than the owner
	chatter    bool   // someone else's message not addressed to Codex
	resolved   bool   // resolved one of our unknown sends
	// delegation is the id of a delegation request this engine takes (inbox).
	delegation string
	// reject answers a refused message instead of running it, quoting
	// rejectQuote (screen, envelope.go).
	reject, rejectQuote string
}

// admit classifies and persists one receive window and acts on the newly
// admitted rows, all in one transaction. Commands run afterwards.
func (e *Engine) admit(ctx context.Context, id int64, w channel.Window) error {
	e.mu.Lock()
	cs := e.convs[id]
	label, kind, group := cs.label, cs.kind, cs.group
	oldGroup := group
	if w.OtherParticipants != nil {
		group = *w.OtherParticipants
	}
	var fresh []channel.InboundMsg
	now := e.now()
	for _, m := range w.Msgs {
		if _, ok := cs.seen[m.ID]; ok {
			continue
		}
		fresh = append(fresh, m)
	}
	unchanged := len(fresh) == 0 && w.Cursor == cs.cursor && cs.seeded && group == oldGroup
	// The first read of this process holds the backlog: messages that
	// arrived while no helper read the chat. A self-echo loop needs this
	// helper's live posts, so the backlog never counts toward the breaker
	// (finding 8: a burst queued during a 7.5 min gap paused every chat).
	backlog := !cs.live
	if unchanged {
		cs.live = true
	}
	e.mu.Unlock()
	if unchanged {
		return nil
	}
	var (
		jobs     []cmdJob
		newTurns bool
		trip     bool
		activity bool
		hasFiles bool
		stg      *admitStage
		by       string  // a sibling's takeover notice names this host (takeover.go)
		closed   []int64 // turns the copy left open, closed by rule 2 (takeover.go)
	)
	inbox := id == e.inboxID && id != 0
	// The first read after taking over by a copy: requests the other copy
	// took are closed (takeover.go, rule 2).
	moved := backlog && e.tookOver != "" && !inbox
	var cancels []string
	err := e.update(func(tx *store.Tx) error {
		jobs, newTurns, trip, activity, hasFiles, cancels, by, closed = nil, false, false, false, false, nil, "", nil
		var sibling time.Time // the newest sibling post of the window
		var skipped []string
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
			if c.sibling && m.Created.After(sibling) {
				sibling = m.Created
			}
			if c.other && !group && w.OtherParticipants == nil {
				group = true
			}
			screen(&c) // raw API keys, attachment limits (envelope.go)
			if c.in.Kind == store.InPrompt && !c.chatter && (c.in.Sender == store.SenderOwner || c.in.Sender == store.SenderOther) {
				c.in.Files = admittedFiles(m) // files, then quotes (envelope.go)
				if len(c.in.Files) > 0 {
					hasFiles = true
				}
			}
			cls = append(cls, c)
		}
		if moved && !sibling.IsZero() {
			// The turns the copy left open were taken before the copy:
			// the other copy, which posted here since, finishes them.
			open, err := tx.OpenTurnsOf(id)
			if err != nil {
				return err
			}
			for _, tr := range open {
				in, _, err := tx.Inbound(tr.InboundID)
				if err != nil {
					return err
				}
				skipped, closed = append(skipped, in.Body), append(closed, tr.ID)
				if err := e.closeCopied(tx, store.Admitted{TurnID: tr.ID}); err != nil {
					return err
				}
			}
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
		if group != oldGroup {
			value := "0"
			if group {
				value = "1"
			}
			if err := tx.SetMeta(fmt.Sprintf("%s%d", metaGroupPrefix, id), value); err != nil {
				return err
			}
		}
		for _, a := range admitted {
			c := byID[a.ExtMsgID]
			if c == nil {
				continue
			}
			c.receivedAt = tx.Now()
			if c.suspect && !backlog && e.breakerHit(id, hitTime(c.msg, now), stg) {
				trip = true
			}
			if c.sibling && kind == store.KindControl && by == "" {
				by = e.supersededBy(c.msg)
			}
			if moved && c.msg.Created.Before(sibling) && !c.chatter && c.reject == "" &&
				(a.TurnID != 0 || a.Kind == store.InCommand && a.State == store.InQueued) {
				skipped = append(skipped, c.in.Body)
				if err := e.closeCopied(tx, a); err != nil {
					return err
				}
				continue
			}
			switch {
			case c.reject != "" && a.State == store.InQueued:
				activity = true
				if err := e.refuse(tx, id, a, c); err != nil {
					return err
				}
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
			e.labelAdmitted(id, group, a, c) // F-RND-4: the user label (annotations.go)
		}
		if trip {
			if err := e.tripBreaker(tx, id, label); err != nil {
				return err
			}
		}
		if len(skipped) > 0 {
			base := fmt.Sprintf("copied:%d:%d", id, e.st.Generation())
			return e.enqueueNotice(tx, id, base, textCopySkipped(e.tookOver, skipped))
		}
		return nil
	})
	if err != nil {
		return err
	}
	if by != "" {
		e.standDown(by)
		return nil
	}
	e.mu.Lock()
	for _, t := range closed {
		delete(e.orphans, t) // settled: reconcileTurn leaves it
	}
	cs.cursor, cs.seeded, cs.live = w.Cursor, true, true
	cs.group = group
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
		cs.cursor, cs.seeded, cs.live = tl.SkipCursor, true, true
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
				tx.NotificationDeliveryChanged(r.ConvID, r.DeliveryKey)
				if err := e.forkSettled(tx, r); err != nil { // fork.go
					return c, false, err
				}
				e.rememberOwnPost(conv, m.ID)
				c.resolved = true // the resolver drops its entry when it sees the row sent
			}
			c.sibling = !ok
			return c, true, nil
		}
		// A marker of the previous helper on this store (the converted
		// v0.1.20 or rc.59 outbox keeps its message ids): this helper's own
		// post, not a peer's. Without this its last posts, re-read after
		// the conversion, counted toward the breaker (finding 8).
		own, err := tx.IsOwn(conv, m.ID, nil, 0)
		if err != nil {
			return c, false, err
		}
		if own {
			c.in.Sender = store.SenderSelf
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
		c.other = m.From != "" && !strings.HasPrefix(m.From, "app:")
		c.in.Sender = store.SenderOther
		if !c.other {
			// Workflows/bots and missing identities are not other human
			// members, nor may they invoke Codex by mentioning the owner.
			c.in.Kind = store.InSystem
			return c, true, nil
		}
		c.quoteID = m.ID
		// The control chat and the delegation inbox ignore non-owners, even
		// when they mention Codex (R-27).
		if kind != store.KindControl && conv != e.inboxID && (m.MentionsOwner || codexMention.MatchString(m.Text)) {
			text, mentioned := stripCodexMention(strings.TrimSpace(m.Text))
			switch {
			case mentioned && bareMention(text, m):
				c.in.Kind, c.reject = store.InCommand, textBareMention
			case parseWork(text).isCommand():
				// v0.1.20 rejectExternalWorkCommand (F-WRK-13): commands
				// are the owner's; it quotes the sender's message (the
				// core can only mention the owner).
				c.in.Kind, c.reject, c.rejectQuote = store.InCommand, textCoworkerCommand, c.quoteID
			}
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
	// A quote is an explicit question target. Known helper commands retain
	// their meaning, including queue; forwarded text never selects a question.
	if p := parseWork(text); p.form == formText && len(m.Quotes) > 0 {
		var target int64
		for _, q := range m.Quotes {
			if q.Forwarded {
				continue
			}
			_, id, err := tx.QuestionByMessage(conv, q.ID)
			if err != nil {
				return c, false, err
			}
			if id != 0 {
				if target != 0 && target != id {
					c.in.Kind, c.reject = store.InCommand, "Quote one question, or use `helper answer Q<number> <answer>`."
					return c, true, nil
				}
				target = id
			}
		}
		if target != 0 {
			c.control = kind == store.KindControl
			c.in.Kind = store.InCommand
			c.in.Body = fmt.Sprintf("helper answer Q%d %s", target, text)
			// Quoted --skip is a literal answer, not an operation.
			if text == "--skip" {
				c.in.Body = fmt.Sprintf("helper answer Q%d -- --skip", target)
			}
			c.cmd = parsed{form: formCommand, name: cmdAnswer, arg: strings.TrimPrefix(c.in.Body, "helper answer ")}
			return c, true, nil
		}
	}
	if conv == e.inboxID && conv != 0 {
		// Inbox records: classifyInbox picks the requests this engine runs.
		c.in.Kind, c.chatter = store.InCommand, true
		return c, true, nil
	}
	if helperShaped(text) {
		c.suspect = true // still runs: genuine look-alike text is allowed
	}
	if kind == store.KindControl {
		c.in.Kind, c.control, c.cmd = store.InCommand, true, parseControl(text)
		// Batch B: a question for Codex (control_qa.go, F-CTL-10).
		if q, ok := controlQuestion(c.cmd, text, m); ok && e.controlLive(conv, m) {
			c.in.Kind, c.in.Body = store.InPrompt, q
		}
		return c, true, nil
	}
	// With other people in the chat the @codex mention is not part of the
	// command (v0.1.20 prepareWorkChatMessageForAudience). The prompt body
	// keeps it.
	mentioned := false
	if group {
		text, mentioned = stripCodexMention(text)
		if mentioned && bareMention(text, m) {
			c.in.Kind, c.reject = store.InCommand, textBareMention
			return c, true, nil
		}
	}
	if p := parseWork(text); p.name == cmdQueue && (p.arg != "" || len(m.Files) > 0) && (!group || mentioned) {
		c.in.Queue = true
		c.in.Body = p.arg
		return c, true, nil
	}
	if c.cmd = parseWork(text); c.cmd.isCommand() {
		c.in.Kind = store.InCommand
		return c, true, nil
	}
	if group && !mentioned && !hasVoice(m.Files) {
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

// bareMention reports an @codex mention with nothing else (v0.1.20
// B:2031: no request text and no attachment).
func bareMention(stripped string, m channel.InboundMsg) bool {
	return strings.TrimSpace(stripped) == "" && len(m.Files) == 0 && len(m.Quotes) == 0 && len(m.Attachments) == 0
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
	if c.control { // batch B: a control-chat question (control_qa.go)
		return false, e.ackControl(tx, conv, a, c)
	}
	norm := normalize(c.in.Body)
	if norm != "" {
		if c.in.Queue {
			norm = "queue\x00" + norm
		} else {
			norm = "auto\x00" + norm
		}
	}
	at := c.msg.Created
	e.mu.Lock()
	cs := e.convs[conv]
	prev, seen := stg.recent[norm]
	if !seen {
		prev, seen = cs.recent[norm]
	}
	e.mu.Unlock()
	dup := seen && at.Sub(prev.at) < e.t.DuplicateFor && at.Sub(prev.at) >= -e.t.DuplicateFor && norm != ""
	// As in v0.1.20 (recentDuplicateSessionPrompt): a message with files,
	// pasted images or quotes is never a duplicate (the same caption with
	// another screenshot is a new request), and only an earlier prompt that
	// is still open or completed suppresses a repeat; resending one that
	// failed, was interrupted or cancelled runs it.
	if dup && len(c.msg.Files)+len(c.msg.Quotes) > 0 {
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
		msgs, err := e.promptConfirmation(c, store.OutNotice, fmt.Sprintf("dup:%d", a.InboundID), textDuplicate, c.quoteID)
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
		if !c.in.Queue && isRunning {
			if _, ok := e.ag.(SteeringAgent); ok {
				text = "Received. Adding this input to Codex; use `helper queue <prompt>` to wait for the next task."
			}
		}
	} else if c.in.Sender == store.SenderOther {
		text = textCoworkerAccepted // v0.1.20 queueTeamsPromptAckForMessage (R-27)
	}
	if notice := e.drainNotice(); notice != "" {
		text = notice // batch E, F-SVC-10: restart.go
	}
	msgs, err := e.promptConfirmation(c, store.OutAck, fmt.Sprintf("ack:%d", a.InboundID), text, c.quoteID)
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
	msgs, err := e.promptConfirmation(c, store.OutAck, fmt.Sprintf("ack:%d", a.InboundID), textHeld, c.quoteID)
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
	hits    []time.Time // message times of the suspicious messages
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
	if len(stg.hits) > 0 {
		cs.breaker = append(cs.breaker, stg.hits...)
		newest := cs.breaker[0]
		for _, t := range cs.breaker {
			if t.After(newest) {
				newest = t
			}
		}
		keep := cs.breaker[:0]
		for _, t := range cs.breaker {
			if newest.Sub(t) < e.t.BreakerWindow {
				keep = append(keep, t)
			}
		}
		cs.breaker = keep
	}
}

// hitTime is the time a suspicious message counts at: when it was posted,
// as in v0.1.20 (its activity time). A burst read at once after a gap is
// spread over the time it was written in; a loop's echoes are seconds apart.
func hitTime(m channel.InboundMsg, now time.Time) time.Time {
	if m.Created.IsZero() {
		return now
	}
	return m.Created
}

// breakerHit stages one suspicious message posted at at and reports a trip:
// BreakerCount suspicious messages posted within BreakerWindow of it.
func (e *Engine) breakerHit(conv int64, at time.Time, stg *admitStage) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	cs := e.convs[conv]
	stg.hits = append(stg.hits, at)
	n := 0
	for _, hits := range [][]time.Time{cs.breaker, stg.hits} {
		for _, t := range hits {
			if d := at.Sub(t); d < e.t.BreakerWindow && d > -e.t.BreakerWindow {
				n++
			}
		}
	}
	return n >= e.t.BreakerCount && !cs.paused
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
	okAt := e.now()    // the last successful scan (the start-up reads cover before)
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
		e.mu.Lock()
		ready := e.controlReadLocked()
		e.mu.Unlock()
		if !ready {
			// The control chat's first read goes first (nothing is
			// sent before it, takeover.go): a small read budget is not
			// spent on scans meanwhile; the start-up reads cover what
			// a scan would show. Nor do the reads wait for the scan
			// (scanFirst), which would wait for them in turn.
			e.scanFirst.Store(false)
			waiting = false
			continue
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
		e.count(func(s *engineStats) { s.reads++; s.read(err, e.now()) })
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
			if blind := e.now().Sub(okAt); blind >= e.scanBlindAfter() {
				e.scanFallback(blind)
			}
			continue
		}
		failures = 0
		okAt = e.now()
		e.scanFirst.Store(false)
		e.scanWorks()
		var woke bool
		mark, woke = e.markScanned(acts, mark, now)
		if woke {
			e.wakeReader()
		}
	}
}

// scanBlindAfter is how long the activity scan may fail before the idle
// active work chats are read directly (scanFallback): a few failed scans in
// a row (the live canary saw 502/504 clusters of up to 101 s) cost a short
// delay, a longer outage must not leave the owner's prompts to the
// SafetyRead (10 min).
func (e *Engine) scanBlindAfter() time.Duration { return 6 * e.t.ScanInterval }

// scanFallback runs while the activity scan has failed for blind (at least
// scanBlindAfter): a change it would show is not lost (the watermark stays
// until a scan succeeds) but would be seen only by the next successful
// scan or the SafetyRead. The idle active work chats are read every
// fallback interval instead: max(12 scan intervals, two read tokens per
// chat), so the reads stay background reads under the read pacer and take
// at most half the account's read budget. A safety read due later is
// brought forward, spread over the interval by chat id.
func (e *Engine) scanFallback(blind time.Duration) {
	now := e.now()
	e.mu.Lock()
	var idle []*convState
	for _, cs := range e.convs {
		if cs.kind == store.KindWork && e.readableLocked(cs) && !e.polledLocked(cs, now) {
			idle = append(idle, cs)
		}
	}
	every := max(12*e.t.ScanInterval, time.Duration(float64(len(idle))*2/e.cfg.ReadRate*float64(time.Second)))
	first := e.scanBlind == 0
	e.scanBlind = every
	moved := 0
	for _, cs := range idle {
		if cs.reading {
			continue
		}
		at := now.Add(every - 2*safetyJitter(cs.id, every)) // spread over [0, every)
		if cs.nextRead.After(at) {
			cs.nextRead, cs.safeAt = at, at
			moved++
		}
	}
	e.mu.Unlock()
	if first {
		e.logf("activity scan failing for %s: reading the %d idle work chats every %s until it works", blind.Round(100*time.Millisecond), len(idle), every)
	}
	if moved > 0 {
		e.wakeReader()
	}
}

// scanWorks ends scanFallback after a successful scan: idle chats return to
// the SafetyRead after their next read.
func (e *Engine) scanWorks() {
	e.mu.Lock()
	was := e.scanBlind != 0
	e.scanBlind = 0
	e.mu.Unlock()
	if was {
		e.logf("activity scan works again")
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
