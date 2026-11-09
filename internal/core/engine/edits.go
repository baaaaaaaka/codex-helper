package engine

import (
	"context"
	"errors"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

// Chat edits: the writes v0.1.20 made besides posts — the user label on
// the owner's message (F-RND-4 / R-8), a voice clip's transcript
// (F-RND-11), the freeze notice of an auto-parked chat (F-WRK-5 / R-22),
// a chat title (F-WRK-2, F-CTL-8) and the unread mark after an answer
// (F-TRN-1).
//
// An edit is queued per conversation in memory and made by the sender
// ahead of that conversation's next outbox row, under the account's write
// pacing (one write token; a read inside it waits for a read token): an
// edit queued in the transaction that enqueues a reply is made before the
// reply goes out, and nothing of it bypasses the pacing. Edits are not
// durable: one lost to a crash is not made again (a missing label or
// title is cosmetic, and nothing is ever posted again because of it). One
// that fails is dropped with a log line; it never affects another message.

type chatEdit struct {
	key       string // dedupes the edit while it waits
	what      string // for the log
	notBefore time.Time
	tries     int
	// deferred is when the edit was first deferred (errEditLater) and
	// defers how often: a deferral is not a failure and is retried with
	// backoff until editDeferFor has passed (runEdit).
	deferred time.Time
	defers   int
	// run makes the edit in chat. errEditLater asks for another try.
	run func(ctx context.Context, chat string) error
	// class ranks an edit made in a chat with nothing to send (sendReply
	// unless set: the startup title repair goes with the mirrored
	// messages).
	class int
}

// errEditLater: the edit could not be made now (its read would wait too
// long behind the account's read pacing) and is tried again later.
var errEditLater = errors.New("edit deferred")

const (
	editTries   = 5
	editTimeout = 2 * time.Minute
	// editReadWait bounds the wait for a read token inside an edit: the
	// conversation's replies wait behind the edit.
	editReadWait = 5 * time.Second
	// flushEditsMax bounds the edits made after a graceful stop.
	flushEditsMax = 3 * time.Second
	// editDeferFor bounds how long a deferred edit (its read waited too
	// long: a read throttle, or live reads using the whole budget) keeps
	// being tried, editDeferMax the gap between two tries (from Cool,
	// doubling).
	editDeferFor = 15 * time.Minute
	editDeferMax = 2 * time.Minute
)

// queueEdit adds an edit for conv unless one with the same key waits. It
// may be called inside a store transaction.
func (e *Engine) queueEdit(conv int64, ed chatEdit) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.edits == nil {
		e.edits = map[int64][]*chatEdit{}
	}
	for _, q := range e.edits[conv] {
		if q.key == ed.key {
			return
		}
	}
	e.edits[conv] = append(e.edits[conv], &ed)
}

// queueEditLast refreshes a coalesced edit after newer edits to the same
// message, while retaining one queued operation per key.
func (e *Engine) queueEditLast(conv int64, ed chatEdit) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.edits == nil {
		e.edits = map[int64][]*chatEdit{}
	}
	queue := e.edits[conv]
	for i, queued := range queue {
		if queued.key == ed.key {
			queue = append(queue[:i], queue[i+1:]...)
			break
		}
	}
	e.edits[conv] = append(queue, &ed)
}

// withEdits marks the send items of conversations with a due edit as
// edits, and adds an item for such a conversation that has no row to
// send. An edit not due yet sets when to look again.
func (e *Engine) withEdits(items []sendItem, now time.Time, soon func(time.Duration)) []sendItem {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.edits) == 0 {
		return items
	}
	due := map[int64]int{}
	for conv, q := range e.edits {
		cs := e.convs[conv]
		if cs == nil || len(q) == 0 {
			delete(e.edits, conv)
			continue
		}
		if cs.chat == "" || cs.sending {
			continue
		}
		if d := q[0].notBefore.Sub(now); d > 0 {
			soon(d)
			continue
		}
		due[conv] = q[0].class
	}
	for i := range items {
		if _, ok := due[items[i].conv]; ok && !items[i].create {
			items[i].edit = true
			delete(due, items[i].conv)
		}
	}
	for conv, class := range due {
		items = append(items, sendItem{edit: true, conv: conv, class: class})
	}
	return items
}

// runEdit makes the first due edit of conv (the sender holds its slot).
func (e *Engine) runEdit(ctx context.Context, conv int64) {
	e.mu.Lock()
	cs := e.convs[conv]
	q := e.edits[conv]
	if cs == nil || len(q) == 0 {
		e.mu.Unlock()
		return
	}
	ed, chat := q[0], cs.chat
	e.edits[conv] = q[1:]
	if len(e.edits[conv]) == 0 {
		delete(e.edits, conv)
	}
	e.mu.Unlock()
	started := e.now()
	cctx, cancel := context.WithTimeout(ctx, editTimeout)
	err := ed.run(cctx, chat)
	cancel()
	if err == nil {
		return
	}
	if ctx.Err() != nil {
		// The engine is stopping: flushEdits makes it again (every edit is
		// idempotent, so one the channel made before the stop is not
		// made twice).
		e.mu.Lock()
		if e.edits == nil {
			e.edits = map[int64][]*chatEdit{}
		}
		e.edits[conv] = append([]*chatEdit{ed}, e.edits[conv]...)
		e.mu.Unlock()
		return
	}
	now := e.now()
	retry := time.Time{}
	var rej *channel.Rejected
	th, throttled := channel.AsThrottled(err)
	switch {
	case throttled:
		retry = now.Add(e.retryAfter(th))
		ed.tries++
		if th.Scope == channel.ScopeAccount || th.Scope == channel.ScopeUnknown {
			e.setGate(e.gWrite, retry, ed.what+" throttled")
			e.paceThrottled(started, retry)
		}
	case errors.Is(err, errEditLater):
		// Not a failure: the edit was not tried. It is tried again with
		// backoff for editDeferFor, so a label is not lost to a long
		// catch-up or a read throttle.
		if ed.deferred.IsZero() {
			ed.deferred = now
		}
		if now.Sub(ed.deferred) < editDeferFor {
			retry = now.Add(min(e.t.Cool<<min(ed.defers, 6), editDeferMax))
			ed.defers++
		}
	case errors.Is(err, channel.ErrNotEditable), errors.Is(err, channel.ErrNotFound),
		errors.As(err, &rej) && rej.Permanent:
		ed.tries++
	default:
		retry = now.Add(e.t.Hot << min(ed.tries+1, 6))
		ed.tries++
	}
	if retry.IsZero() || ed.tries >= editTries {
		e.logf("%s in conversation %d not made: %v", ed.what, conv, err)
		return
	}
	ed.notBefore = retry
	e.mu.Lock()
	if e.edits == nil {
		e.edits = map[int64][]*chatEdit{}
	}
	e.edits[conv] = append([]*chatEdit{ed}, e.edits[conv]...)
	e.mu.Unlock()
	time.AfterFunc(retry.Sub(now), e.wakeSender)
}

// flushEdits makes the queued edits that are due once the engine has
// stopped gracefully (a stop, a restart or the kill switch), within limit
// and under the account write pacing: the queue is in memory, and the
// label or title of the last moments before a stop would otherwise be
// lost. Failures are dropped (they are cosmetic); nothing is retried.
func (e *Engine) flushEdits(limit time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()
	for ctx.Err() == nil {
		now := e.now()
		var (
			conv int64
			ed   *chatEdit
			chat string
		)
		e.mu.Lock()
		for id, q := range e.edits {
			cs := e.convs[id]
			if len(q) == 0 || cs == nil || cs.chat == "" || now.Before(q[0].notBefore) || (ed != nil && id > conv) {
				continue
			}
			conv, ed, chat = id, q[0], cs.chat
		}
		if ed != nil {
			e.edits[conv] = e.edits[conv][1:]
			if len(e.edits[conv]) == 0 {
				delete(e.edits, conv)
			}
		}
		e.mu.Unlock()
		if ed == nil || e.waitWrite(ctx) != nil {
			return
		}
		if err := ed.run(ctx, chat); err != nil {
			e.logf("%s in conversation %d not made at stop: %v", ed.what, conv, err)
		}
	}
}

// waitRead waits for a read token of the account's read pacing (and its
// gate) for at most limit; errEditLater when that is not enough. While it
// waits, the receiver dispatches no background read (start-up reads,
// read-backs, safety reads) and the check of the previous helper's
// answers (legacy_check.go) reads nothing. An edit is the owner's (the
// label on the message they just sent), and behind a catch-up of hundreds
// of reads it would otherwise lose every token to them: the receiver polls
// the bucket each time a read ends, so it takes a token the moment it
// accrues. Live reads (the control chat, chats with a running turn or
// recent owner activity) and the activity scan still compete with it.
func (e *Engine) waitRead(ctx context.Context, limit time.Duration) error {
	e.readWaiters.Add(1)
	defer func() {
		e.readWaiters.Add(-1)
		e.wakeReader() // the background reads it held back
	}()
	deadline := e.now().Add(limit)
	for {
		now := e.now()
		var wait time.Duration
		if until := e.gate(e.gRead); now.Before(until) {
			wait = until.Sub(now)
		} else if ok, w := e.readTokens.take(); ok {
			return nil
		} else {
			wait = w
		}
		wait = max(wait, e.t.Tick)
		if now.Add(wait).After(deadline) {
			return errEditLater
		}
		if !sleepCtx(ctx, wait) {
			return ctx.Err()
		}
	}
}

// annotator is the channel's in-place annotation, when it has one.
func (e *Engine) annotator() (channel.Annotator, bool) {
	a, ok := e.cfg.Channel.(channel.Annotator)
	return a, ok && e.ch.Caps().EditMessages
}
