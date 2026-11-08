package engine

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// watchSilence posts the R-19 hints while a turn's Codex stays silent. It
// never stops the turn (F-TRN-18, v0.1.20 parity: "I will not retry
// automatically"; only the user's `helper cancel running` stops it). The
// returned func ends the watch.
func (e *Engine) watchSilence(ctx context.Context, cs *convState, tr store.Turn, rs *runState) func() {
	every := time.Duration(0)
	for _, d := range []time.Duration{e.t.SilenceHint, e.t.SilenceStuck} {
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
			e.mu.Lock()
			waiting := e.questionWaiting[cs.id]
			e.mu.Unlock()
			if waiting {
				rs.lastEvent.Store(ms(e.now()))
				continue
			}
			if last != episode {
				episode, hinted = last, 0 // Codex spoke: a new silence starts
			}
			quiet := e.now().Sub(fromMS(last))
			switch {
			case e.t.SilenceStuck > 0 && quiet >= e.t.SilenceStuck && hinted < 2:
				hinted = 2
				e.silenceNotice(cs.id, fmt.Sprintf("turn:%d:silence:%d:2", tr.ID, episode), textStuck(quiet))
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
	if err := e.update(func(tx *store.Tx) error { return e.enqueueStatus(tx, conv, key, text) }); err != nil {
		e.logf("silence hint in %d: %v", conv, err)
		return
	}
	e.wakeSender()
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

// Texts of R-19 (v0.1.20 codexIdleStatusMessage + codexIdleStatusCancelHint
// and codexSuspectedStuckMessage, B:92-99).
const textSilent = "Still working. No new Codex update yet.\n\nThis is taking longer than usual. To stop the running request, send `helper cancel running` in this chat."

func textStuck(quiet time.Duration) string {
	return "Codex has not produced any update for " + quietDuration(quiet) + ". It may be stuck.\n\nI will not retry automatically. To stop the running request, send `helper cancel running` in this chat."
}

// quietDuration is v0.1.20's formatCodexQuietDuration (B:12065).
func quietDuration(d time.Duration) string {
	r := d.Round(time.Minute)
	if r < time.Minute {
		return "less than 1m"
	}
	return strings.TrimSuffix(r.String(), "0s")
}

// textStarting names a queued request at the head of its first progress
// message; quoted is a quotePrompt.
func textStarting(quoted string) string {
	return "▶️ Codex is starting this queued request: " + quoted
}

// textQueuedAck acknowledges a request that waits behind another one in
// the same chat (owner ruling 2026-11-05: short, says when it runs and how
// to cancel it). isRunning reports whether a request of the chat runs (or
// waits for Codex capacity), running is its prompt ("" if unknown, as for
// an attachment-only request), ahead the number of other queued requests
// before this one. `helper cancel` cancels the newest queued request (this
// one, until another arrives); `helper cancel running` interrupts the
// running one.
func textQueuedAck(running string, isRunning bool, ahead int) string {
	head := "⏳ **Queued.** Your request will run after the current one finishes"
	if !isRunning {
		head = "⏳ **Queued.** Your request will run when the current work in this chat finishes"
	}
	switch {
	case ahead == 1:
		head += ", behind 1 other queued request"
	case ahead > 1:
		head += fmt.Sprintf(", behind %d other queued requests", ahead)
	}
	lines := []string{head + "."}
	cancel := "To cancel it: `helper cancel` (cancels the newest queued request)."
	if p := promptLine(running); isRunning && p != "" {
		lines = append(lines, "", "**Running now:** "+p)
	}
	if isRunning {
		cancel += " To interrupt the running one: `helper cancel running`."
	}
	lines = append(lines, "", cancel)
	return strings.Join(lines, "\n")
}

// promptLine is a prompt on one line as v0.1.20 listed it: at most 180
// characters, backquotes made plain quotes.
func promptLine(p string) string {
	p = strings.ReplaceAll(strings.Join(strings.Fields(p), " "), "`", "'")
	if r := []rune(p); len(r) > 180 {
		p = strings.TrimSpace(string(r[:179])) + "…"
	}
	return p
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
