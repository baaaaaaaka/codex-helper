package engine

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// errStalled is the turn error of a turn stopped by the stall bound.
const errStalled = "no Codex update within the stall bound"

// watchSilence posts the R-19 hints while a turn's Codex stays silent and
// stops the turn at the stall bound. The returned func ends the watch.
func (e *Engine) watchSilence(ctx context.Context, cs *convState, tr store.Turn, rs *runState) func() {
	every := time.Duration(0)
	for _, d := range []time.Duration{e.t.SilenceHint, e.t.SilenceStuck, e.t.StallLimit} {
		if d > 0 && (every == 0 || d < every) {
			every = d
		}
	}
	if every == 0 {
		return func() {}
	}
	every /= 10
	if every < e.t.Tick {
		every = e.t.Tick
	}
	stop := make(chan struct{})
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		tick := time.NewTicker(every)
		defer tick.Stop()
		episode, hinted := rs.lastEvent.Load(), 0
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			last := rs.lastEvent.Load()
			if last != episode {
				episode, hinted = last, 0 // Codex spoke: a new silence starts
			}
			quiet := e.now().Sub(fromMS(last))
			switch {
			case e.t.StallLimit > 0 && quiet >= e.t.StallLimit:
				e.stopStalled(ctx, cs, rs)
				return
			case e.t.SilenceStuck > 0 && quiet >= e.t.SilenceStuck && hinted < 2:
				hinted = 2
				e.silenceNotice(cs.id, fmt.Sprintf("turn:%d:silence:%d:2", tr.ID, episode), textStuck(quiet, e.t.StallLimit))
			case e.t.SilenceHint > 0 && quiet >= e.t.SilenceHint && hinted < 1:
				hinted = 1
				e.silenceNotice(cs.id, fmt.Sprintf("turn:%d:silence:%d:1", tr.ID, episode), textSilent)
			}
		}
	}()
	return func() { close(stop) }
}

func (e *Engine) silenceNotice(conv int64, key, text string) {
	if conv == e.inboxID {
		return // the inbox carries protocol records only
	}
	if err := e.update(func(tx *store.Tx) error { return e.enqueueNotice(tx, conv, key, text) }); err != nil {
		e.logf("silence hint in %d: %v", conv, err)
		return
	}
	e.wakeSender()
}

// stopStalled stops a turn whose Codex stayed silent for StallLimit:
// turn/interrupt, then kill if it does not end within CancelWait. The turn
// then ends with one notice (finish, errStalled).
func (e *Engine) stopStalled(ctx context.Context, cs *convState, rs *runState) {
	e.mu.Lock()
	if cs.running != rs || rs.cancelled {
		e.mu.Unlock()
		return
	}
	rs.stalled = true
	label := cs.label
	e.mu.Unlock()
	e.logf("turn in %s: no Codex update for %s; stopping it", label, e.t.StallLimit)
	ictx, cancel := context.WithTimeout(ctx, e.t.CancelWait)
	err := e.ag.Interrupt(ictx, cs.id)
	cancel()
	if err == nil {
		select {
		case <-rs.done:
			return
		case <-ctx.Done():
			return
		case <-time.After(e.t.CancelWait):
		}
	}
	e.ag.Kill(cs.id)
}

// remindQueued posts a reminder for every turn still queued after each
// QueueReminder period (R-35).
func (e *Engine) remindQueued() {
	if e.t.QueueReminder <= 0 {
		return
	}
	turns, err := e.st.OpenTurns()
	if err != nil {
		e.logf("queue reminders: %v", err)
		return
	}
	now := e.now()
	open := map[int64]bool{}
	for _, tr := range turns {
		open[tr.ID] = true
		if tr.State != store.TurnQueued || tr.QueuedAt == 0 {
			continue
		}
		waited := now.Sub(fromMS(tr.QueuedAt))
		n := int(waited / e.t.QueueReminder)
		cs := e.conv(tr.ConvID)
		if n < 1 || cs == nil || tr.ConvID == e.inboxID {
			continue
		}
		e.mu.Lock()
		posted := e.reminded[tr.ID]
		running, paused := cs.running != nil, cs.paused
		e.mu.Unlock()
		// Not blocked by a turn: it starts once earlier replies are
		// delivered, and a reminder would only delay it. A paused chat
		// is reminded once (no spam, R-22).
		if posted >= n || (!running && !paused) || (paused && posted >= 1) {
			continue
		}
		err := e.update(func(tx *store.Tx) error {
			in, _, err := tx.Inbound(tr.InboundID)
			if err != nil {
				return err
			}
			return e.enqueueNotice(tx, tr.ConvID, fmt.Sprintf("turn:%d:waiting:%d", tr.ID, n), textStillQueued(in.Body, waited, paused))
		})
		if err != nil {
			e.logf("queue reminder in %d: %v", tr.ConvID, err)
			continue
		}
		e.mu.Lock()
		e.reminded[tr.ID] = n
		e.mu.Unlock()
		e.wakeSender()
	}
	e.mu.Lock()
	for id := range e.reminded {
		if !open[id] {
			delete(e.reminded, id)
		}
	}
	e.mu.Unlock()
}

// Texts of R-19 and R-35 (wording from the previous core).
const textSilent = "Still working. No new Codex update yet.\n\nThis is taking longer than usual. To stop the running request, send `helper cancel` in this chat."

func textStuck(quiet, limit time.Duration) string {
	s := "Codex has not produced any update for " + minutes(quiet) + ". It may be stuck.\n\nTo stop the running request, send `helper cancel` in this chat."
	if limit > 0 {
		s += " If it stays silent for " + minutes(limit) + ", the helper stops it."
	}
	return s
}

func textStalled(limit time.Duration) string {
	return "Codex produced no update for " + minutes(limit) + ", so the helper stopped the request. It was not re-run; resend it if you still need it."
}

// textStarting names a queued request at the head of its first progress
// message; quoted is a quotePrompt.
func textStarting(quoted string) string {
	return "▶️ Codex is starting this queued request: " + quoted
}

// textQueuedOutcome names a queued request at the head of its answer or
// outcome notice when no progress message named it.
func textQueuedOutcome(quoted string) string {
	return "▶️ Queued request: " + quoted
}

// textStartingBatch is the one notice when several queued requests start
// one after another (the first is starting now).
func textStartingBatch(quoted []string) string {
	const show = 10
	var b strings.Builder
	fmt.Fprintf(&b, "▶️ Codex is starting your %d queued requests, one after another:\n", len(quoted))
	for i, q := range quoted {
		if i == show {
			fmt.Fprintf(&b, "\n…and %d more.", len(quoted)-show)
			break
		}
		fmt.Fprintf(&b, "\n%d. %s", i+1, q)
	}
	return b.String()
}

func textStillQueued(prompt string, waited time.Duration, paused bool) string {
	reason := "An earlier request in this chat is still running."
	if paused {
		reason = "This chat is paused; `helper unpause` resumes it."
	}
	return "⏳ Still waiting (queued for " + minutes(waited) + "): " + quotePrompt(prompt) + "\n\n" + reason +
		" It runs after this clears. If this looks stale, send `helper status` here. To drop the last queued request, send `helper cancel last`."
}

// quotePrompt is a short one-line quote of the owner's prompt.
func quotePrompt(s string) string {
	s = clipText(strings.Join(strings.Fields(s), " "), 80)
	if s == "" {
		return "(attachments only)"
	}
	return "“" + s + "”"
}

// minutes renders a duration for chat text ("7 min"; seconds below one
// minute).
func minutes(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%d s", int(d.Round(time.Second)/time.Second))
	}
	return fmt.Sprintf("%d min", int(d.Round(time.Minute)/time.Minute))
}
