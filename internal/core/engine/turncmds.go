package engine

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Turn commands of a work chat (gap batch E): `helper cancel` (F-TRN-3)
// and `helper retry` (F-TRN-4), with v0.1.20's grammar and texts
// (cancelTurnCommandForScope B:9490, retryTurnCommand B:8236).
//
// A turn is named to the user as `turn-<n>` (its store id). Every reply
// is computed and committed in the transaction that changes the turns, so
// a crash either runs the whole command again (requeueCommands; the reply
// key dedupes) or leaves it done.

// turnRef is the user-facing name of a turn.
func turnRef(id int64) string { return fmt.Sprintf("turn-%d", id) }

// parseTurnRef reads `turn-12`, `turn:12`, `#12` or `12`.
func parseTurnRef(s string) (int64, bool) {
	s = strings.Trim(strings.ToLower(strings.TrimSpace(s)), "`'\"")
	for _, p := range []string{"turn-", "turn:", "turn_", "#"} {
		s = strings.TrimPrefix(s, p)
	}
	id, err := strconv.ParseInt(s, 10, 64)
	return id, err == nil && id > 0
}

// replyIn finishes a command with a reply whose text is computed in the
// transaction that also applies the command's change; on an error nothing
// is committed and the error is logged.
func (e *Engine) replyIn(j cmdJob, fn func(tx *store.Tx) (string, error)) error {
	err := e.update(func(tx *store.Tx) error {
		text, err := fn(tx)
		if err != nil {
			return err
		}
		msgs, err := e.noticeReply(store.OutControl, fmt.Sprintf("cmd:%d", j.inbound), text, j.quote)
		if err != nil {
			return err
		}
		if _, err := tx.Enqueue(j.conv, msgs); err != nil {
			return err
		}
		return tx.SetInbound(j.inbound, store.InDone, 0)
	})
	if err != nil {
		e.logf("command reply in %d: %v", j.conv, err)
	}
	e.wakeSender()
	return err
}

// turnPrompt is v0.1.20's turnPromptSummary: the prompt on one line,
// backticks replaced, at most 180 characters ("" for attachments only).
func turnPrompt(tx *store.Tx, tr store.Turn) (string, error) {
	if tr.InboundID == 0 {
		return "", nil
	}
	in, ok, err := tx.Inbound(tr.InboundID)
	if err != nil || !ok {
		return "", err
	}
	return promptLine(in.Body), nil
}

// turnPromptList is v0.1.20's formatTurnPromptList.
func turnPromptList(tx *store.Tx, turns []store.Turn) ([]string, error) {
	lines := make([]string, 0, len(turns))
	for i, tr := range turns {
		p, err := turnPrompt(tx, tr)
		if err != nil {
			return nil, err
		}
		if p == "" {
			p = "Prompt unavailable for `" + turnRef(tr.ID) + "`"
		}
		lines = append(lines, fmt.Sprintf("%d. %s", i+1, p))
	}
	return lines, nil
}

// Texts of `helper cancel` (v0.1.20).
const (
	textCancelUsage      = "usage: `helper cancel last`, `helper cancel queued`, `helper cancel running`, `helper cancel all`, `helper cancel <turn-id>`, or `!cancel <turn-id>`\n\nThese commands apply to this Work chat."
	textCancelNone       = "no running or queued turn is available to cancel in this session."
	textCancelNoQueued   = "no queued turn is available to cancel in this session."
	textCancelNoRunning  = "no running turn is available to cancel in this session."
	textCancelledRunning = "Codex request canceled."
)

// cmdCancel is `helper cancel [last|queued|running|all|<turn-id>]`. A bare
// `helper cancel` is `last` (owner ruling 2026-11-05: the queued ACK says
// it cancels the newest queued request): the newest queued request, else
// the running one. A queued request is cancelled in the reply's
// transaction and never reaches Codex; the running one is interrupted
// after the reply is committed, so the reply precedes its "Codex request
// canceled." (R-20-33).
func (e *Engine) cmdCancel(ctx context.Context, j cmdJob, cs *convState, arg string) {
	sel := strings.ToLower(strings.TrimSpace(arg))
	var interrupt bool
	err := e.replyIn(j, func(tx *store.Tx) (string, error) {
		interrupt = false
		e.mu.Lock()
		var runningID int64
		if cs.running != nil {
			runningID = cs.running.turnID
		}
		e.mu.Unlock()
		open, err := tx.OpenTurnsOf(cs.id)
		if err != nil {
			return "", err
		}
		var queued []store.Turn
		var running *store.Turn
		for i := range open {
			switch {
			case open[i].ID == runningID:
				running = &open[i]
			case open[i].State == store.TurnQueued:
				queued = append(queued, open[i])
			}
		}
		newestQueued := func() *store.Turn {
			if len(queued) == 0 {
				return nil
			}
			return &queued[len(queued)-1]
		}
		var target *store.Turn
		switch sel {
		case "all":
			if len(queued) == 0 && running == nil {
				return textCancelNone, nil
			}
			return e.cancelAll(tx, queued, running, &interrupt)
		case "", "last":
			if target = newestQueued(); target == nil {
				target = running
			}
			if target == nil && sel == "" {
				return "Nothing to cancel: no request is running or queued.\n\n" + textCancelUsage, nil
			}
			if target == nil {
				return textCancelNone, nil
			}
		case "queued":
			if target = newestQueued(); target == nil {
				return textCancelNoQueued, nil
			}
		case "running":
			if target = running; target == nil {
				return textCancelNoRunning, nil
			}
		default:
			id, ok := parseTurnRef(sel)
			if !ok {
				return "turn not found in this session: " + strings.TrimSpace(arg), nil
			}
			tr, found, err := tx.Turn(id)
			if err != nil {
				return "", err
			}
			if !found || tr.ConvID != cs.id {
				return "turn not found in this session: " + strings.TrimSpace(arg), nil
			}
			switch {
			case running != nil && tr.ID == running.ID:
				target = running
			case tr.State == store.TurnQueued:
				target = &tr
			default:
				return fmt.Sprintf("turn %s is %s and cannot be canceled.", turnRef(tr.ID), turnStateText(tr.State)), nil
			}
		}
		return e.cancelOne(tx, cs, *target, running != nil && target.ID == running.ID, queued, &interrupt)
	})
	if err == nil && interrupt {
		e.cancelRunning(ctx, cs.id)
	}
	e.wakeRunner(cs.id)
}

// cancelOne cancels one turn (v0.1.20 formatCancelOneResponse).
func (e *Engine) cancelOne(tx *store.Tx, cs *convState, tr store.Turn, isRunning bool, queued []store.Turn, interrupt *bool) (string, error) {
	action, label := "turn canceled", "**Canceled prompt:**"
	if isRunning {
		action, label = "cancel requested for running turn", "**Prompt being canceled:**"
		*interrupt = true
	} else if err := tx.FinishTurn(tr.ID, store.TurnCancelled, "cancelled by the user", nil); err != nil {
		return "", err
	}
	lines := []string{action + ": `" + turnRef(tr.ID) + "`"}
	p, err := turnPrompt(tx, tr)
	if err != nil {
		return "", err
	}
	if p != "" {
		lines = append(lines, "", label, p)
	}
	var rest []store.Turn
	for _, q := range queued {
		if q.ID != tr.ID {
			rest = append(rest, q)
		}
	}
	lines = append(lines, "")
	if len(rest) == 0 {
		lines = append(lines, "No other prompts are queued.")
	} else {
		list, err := turnPromptList(tx, rest)
		if err != nil {
			return "", err
		}
		lines = append(lines, "**Still queued:**")
		lines = append(lines, list...)
	}
	return strings.Join(lines, "\n"), nil
}

// cancelAll cancels every queued turn and interrupts the running one
// (v0.1.20 formatCancelAllResponse).
func (e *Engine) cancelAll(tx *store.Tx, queued []store.Turn, running *store.Turn, interrupt *bool) (string, error) {
	for _, q := range queued {
		if err := tx.FinishTurn(q.ID, store.TurnCancelled, "cancelled by the user", nil); err != nil {
			return "", err
		}
	}
	lines := []string{"cancel all requested for this Work chat."}
	if running != nil {
		*interrupt = true
		list, err := turnPromptList(tx, []store.Turn{*running})
		if err != nil {
			return "", err
		}
		lines = append(lines, "", "**Running requests cancel requested:**")
		lines = append(lines, list...)
	}
	if len(queued) > 0 {
		list, err := turnPromptList(tx, queued)
		if err != nil {
			return "", err
		}
		lines = append(lines, "", "**Queued requests canceled:**")
		lines = append(lines, list...)
	}
	lines = append(lines, "", "No queued prompts remain.")
	return strings.Join(lines, "\n"), nil
}

// turnStateText names a turn state in a reply.
func turnStateText(state string) string {
	switch state {
	case store.TurnCancelled:
		return "canceled"
	case store.TurnNotStarted:
		return "not started"
	}
	return state
}

// Texts of `helper retry` (v0.1.20).
const (
	textRetryUsage = "usage: `helper retry last`, `helper retry <turn-id>`, or `!retry <turn-id>`"
	textRetryNone  = "no failed or interrupted turn is available to retry in this session."
)

// retryable reports whether a turn that ended may be run again: it failed,
// was interrupted, did not start, or was cancelled (v0.1.20 recorded a
// cancel as interrupted). A completed turn never is: its final was posted.
func retryable(tr store.Turn) bool {
	switch tr.State {
	case store.TurnFailed, store.TurnInterrupted, store.TurnNotStarted, store.TurnCancelled:
		return tr.InboundID != 0
	}
	return false
}

// retryHorizon bounds how far back `helper retry last` looks.
const retryHorizon = 200

// cmdRetry is `helper retry [last|<turn-id>]`. A bare `helper retry` runs
// the last duplicate-ignored prompt (F-TRN-6, owner 第二级 6), else shows
// the usage. `last` is the newest failed, interrupted, not-started or
// cancelled request (not a duplicate); `<turn-id>` names one. The new
// turn is created in the reply's transaction, for the persisted inbound
// prompt, and only when no later run of that prompt exists: a prompt is
// never run twice by retries and an answered prompt never again.
func (e *Engine) cmdRetry(j cmdJob, cs *convState, arg string) {
	sel := strings.ToLower(strings.TrimSpace(arg))
	e.mu.Lock()
	dup := cs.lastDup
	e.mu.Unlock()
	if sel == "" && dup != 0 {
		e.retryDuplicate(j, cs, dup)
		return
	}
	if sel == "" {
		e.reply(j, textRetryUsage, nil)
		return
	}
	var started, usedDup bool
	err := e.replyIn(j, func(tx *store.Tx) (string, error) {
		started, usedDup = false, false
		recent, err := tx.RecentTurns(cs.id, retryHorizon)
		if err != nil {
			return "", err
		}
		// newer[inbound] is the newest turn of each prompt (recent is
		// newest first).
		newer := map[int64]store.Turn{}
		var target *store.Turn
		for i := range recent {
			tr := recent[i]
			if _, seen := newer[tr.InboundID]; seen && tr.InboundID != 0 {
				continue // an older run of a prompt that ran again
			}
			newer[tr.InboundID] = tr
			if sel == "last" && target == nil && retryable(tr) && tr.Error != "duplicate" {
				target = &recent[i]
			}
		}
		if sel != "last" {
			id, ok := parseTurnRef(sel)
			if !ok {
				return "turn not found in this session: " + strings.TrimSpace(arg), nil
			}
			tr, found, err := tx.Turn(id)
			if err != nil {
				return "", err
			}
			if !found || tr.ConvID != cs.id {
				return "turn not found in this session: " + strings.TrimSpace(arg), nil
			}
			if !retryable(tr) {
				if tr.InboundID == 0 && tr.State != store.TurnCompleted && !openTurnState(tr.State) {
					return "retry cannot find the original Teams message for " + turnRef(tr.ID), nil
				}
				return fmt.Sprintf("turn %s is %s; only failed or interrupted turns can be retried.", turnRef(tr.ID), turnStateText(tr.State)), nil
			}
			if last, ok := newer[tr.InboundID]; ok && last.ID != tr.ID {
				return fmt.Sprintf("turn %s was already run again as `%s`; only failed or interrupted turns can be retried.", turnRef(tr.ID), turnRef(last.ID)), nil
			} else if !ok {
				// Older than the horizon: check the prompt's own turn.
				if in, ok, err := tx.Inbound(tr.InboundID); err != nil {
					return "", err
				} else if ok && in.TurnID != 0 && in.TurnID != tr.ID {
					return fmt.Sprintf("turn %s was already run again; only failed or interrupted turns can be retried.", turnRef(tr.ID)), nil
				}
			}
			target = &tr
		}
		if target == nil {
			if dup != 0 {
				usedDup = true
				return e.retryDuplicateIn(tx, cs, dup, &started)
			}
			return textRetryNone, nil
		}
		in, ok, err := tx.Inbound(target.InboundID)
		if err != nil {
			return "", err
		}
		if !ok || in.ConvID != cs.id {
			return "retry cannot find the original Teams message for " + turnRef(target.ID), nil
		}
		id, err := tx.RetryTurn(cs.id, target.InboundID)
		if err != nil {
			return "", err
		}
		started = true
		lines := []string{fmt.Sprintf("Running `%s` again as `%s`.", turnRef(target.ID), turnRef(id))}
		if p := promptLine(in.Body); p != "" {
			lines = append(lines, "", "**Prompt:**", p)
		}
		return strings.Join(lines, "\n"), nil
	})
	if err == nil && started {
		if usedDup {
			e.clearDup(cs, dup)
		}
		e.wakeRunner(cs.id)
	}
}

func (e *Engine) clearDup(cs *convState, dup int64) {
	e.mu.Lock()
	if cs.lastDup == dup {
		cs.lastDup = 0
	}
	e.mu.Unlock()
}

func openTurnState(state string) bool {
	switch state {
	case store.TurnQueued, store.TurnStarting, store.TurnRunning:
		return true
	}
	return false
}

// retryDuplicate runs the last duplicate-ignored prompt again.
func (e *Engine) retryDuplicate(j cmdJob, cs *convState, dup int64) {
	var started bool
	if err := e.replyIn(j, func(tx *store.Tx) (string, error) { return e.retryDuplicateIn(tx, cs, dup, &started) }); err == nil && started {
		e.clearDup(cs, dup)
		e.wakeRunner(cs.id)
	}
}

func (e *Engine) retryDuplicateIn(tx *store.Tx, cs *convState, dup int64, started *bool) (string, error) {
	if _, err := tx.RetryTurn(cs.id, dup); err != nil {
		return "", err
	}
	*started = true
	return "Running the ignored request again.", nil
}
