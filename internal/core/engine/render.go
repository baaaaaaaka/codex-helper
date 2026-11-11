package engine

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
)

// render turns doc into outbox messages keyed base:<part>. Rendering is
// CPU work in the adapter (formula images may load fonts once); it is never
// done while a store transaction is open, except for helper notices
// (no mention, no math), which are pure.
func (e *Engine) render(ctx context.Context, doc channel.RichDoc, kind, base string) ([]store.OutMsg, error) {
	parts, err := e.ch.Render(ctx, doc)
	if err != nil {
		return nil, fmt.Errorf("render %s: %w", base, err)
	}
	out := make([]store.OutMsg, 0, len(parts))
	for _, p := range parts {
		h, _ := hex.DecodeString(p.Hash)
		out = append(out, store.OutMsg{
			DeliveryKey: fmt.Sprintf("%s:%d", base, p.Index),
			Kind:        kind, Part: p.Index, Parts: p.Count,
			Body: p.Body, BodyHash: h, Mention: p.Mention, ReplyTo: p.ReplyTo,
		})
	}
	return out, nil
}

// answerDoc is a Codex answer: the answer label and, after the last part,
// v0.1.20's completion footer "🔧 Helper: ✅ Codex finished responding."
// (without an @mention: v0.1.20 notified the owner by the Workflow card or
// the control chat, never in a work chat that has a control chat).
func answerDoc(text string) channel.RichDoc {
	return channel.RichDoc{Label: labelAnswer, Markdown: text, Math: true, FooterLabel: labelHelper, Footer: textFinished}
}

// statusDoc is a Codex status post (progress, commentary, compaction).
func statusDoc(text string) channel.RichDoc {
	return channel.RichDoc{Label: labelStatus, Markdown: text, Math: true}
}

// userDoc is a local prompt mirrored into its work chat.
func userDoc(text string) channel.RichDoc {
	return channel.RichDoc{Label: labelUser, Markdown: text}
}

// statusNotice renders a turn outcome that v0.1.20 posted as Codex status
// (interrupted, failed, not run, artifact problems).
func (e *Engine) statusNotice(base, text string) ([]store.OutMsg, error) {
	return e.render(context.Background(), channel.RichDoc{Label: labelStatus, Markdown: text}, store.OutNotice, base)
}

// notice renders a helper notice ("🔧 Helper: text").
func (e *Engine) notice(base, text string) ([]store.OutMsg, error) {
	return e.render(context.Background(), channel.RichDoc{Label: labelHelper, Markdown: text}, store.OutNotice, base)
}

// noticeReply is notice quoting the message it answers (group chats).
func (e *Engine) noticeReply(kind, base, text, replyTo string) ([]store.OutMsg, error) {
	return e.render(context.Background(), channel.RichDoc{Label: labelHelper, Markdown: text, ReplyTo: replyTo}, kind, base)
}

// promptConfirmation marks only the owner's initial prompt response for append.
// Multi-part confirmations keep the ordinary delivery path as one complete reply.
func (e *Engine) promptConfirmation(c *classified, kind, base, text, replyTo string) ([]store.OutMsg, error) {
	if c.receivedAt != 0 {
		text += "\n\nReceived at " + fromMS(c.receivedAt).Local().Format("2006-01-02 15:04:05 UTC-07:00")
	}
	msgs, err := e.noticeReply(kind, base, text, replyTo)
	if err == nil && len(msgs) == 1 && c.in.Sender == store.SenderOwner && c.delegation == "" {
		msgs[0].AppendTo = c.in.ExtMsgID
		c.confirmed = true
	}
	return msgs, err
}

// enqueueNotice renders and enqueues a notice in tx.
// enqueueStatus enqueues a turn-progress notice that v0.1.20 posted as
// Codex status (its codex-status kind: still working, may be stuck,
// capacity retry).
func (e *Engine) enqueueStatus(tx *store.Tx, conv int64, base, text string) error {
	msgs, err := e.statusNotice(base, text)
	if err != nil {
		return err
	}
	_, err = tx.Enqueue(conv, msgs)
	return err
}

func (e *Engine) enqueueNotice(tx *store.Tx, conv int64, base, text string) error {
	msgs, err := e.notice(base, text)
	if err != nil {
		return err
	}
	_, err = tx.Enqueue(conv, msgs)
	return err
}

// User-visible texts. They never carry internal ids or paths.
const (
	// textAccepted is the v0.1.20 ACK, word for word (fidelity run).
	textAccepted = "⏳ Codex is working. Request accepted."
	textFinished = "✅ Codex finished responding."
	// textCompacted is v0.1.20's status post for a context compaction.
	textCompacted = transcript.CompactText
	// textInterrupted is v0.1.20's interrupted-turn notice (B:11189),
	// pointing to `helper retry last` (F-TRN-4/F-TRN-5).
	textInterrupted = "Codex request interrupted because the Teams helper stopped, restarted, or lost its execution context before it could verify a final Codex result.\n\nCheck recent messages and changed files first. If no final answer appears, resend the message or use `helper retry last`."
	textNotStarted  = "The previous request did not run: the helper stopped before Codex accepted it. Send `helper retry last` to run it, or resend it."
	// textRestarted is v0.1.20's post-restart confirmation (F-CTL-11).
	textRestarted   = "✅ Helper restart completed\n\nThe Teams helper is back online. Send `st` to check status."
	textHeld        = "Request held: this chat is paused, so new requests do not run. It will not run later by itself; resend it once the chat is resumed."
	textDuplicate   = "Duplicate request ignored (same text within 3 minutes). Send `helper retry` to run it again."
	textActive      = "Codex did not run this request: the session is open in another Codex window that is writing to it. Close it there (or wait until it is idle) and resend."
	textUnconfirmed = "A reply could not be confirmed as delivered, so it was not sent again to avoid a duplicate. Ask again if it is missing."
	// textUnconfirmedRetried: an answer was sent a second time and that copy
	// could not be confirmed either.
	textUnconfirmedRetried = "A reply could not be confirmed as delivered, even after sending it a second time. Ask again if it is missing."
)

// textReconnecting is v0.1.20's status when Codex retries within a turn
// (B:formatCodexStreamRetryStatus), word for word.
func textReconnecting(message, code string) string {
	message, code = strings.TrimSpace(message), strings.TrimSpace(code)
	if message == "" {
		message = "Reconnecting..."
	}
	lines := []string{"Connection dropped. Codex is reconnecting."}
	if !strings.EqualFold(message, lines[0]) {
		lines = append(lines, "", message)
	}
	if code != "" {
		lines = append(lines, "Reason: "+code)
	}
	return strings.Join(lines, "\n")
}

// Goal and model lines (owner request 2026-11-07): every new Codex message
// (status or answer) of a thread with an active goal closes with
// "🎯 Active goal: <objective, clipped>", and every new answer with
// "🔧 Helper: model - X, effort - Y, tier - Z", both before the "Codex
// finished responding" footer. Messages rendered before are not changed.
const (
	labelGoal    = "🎯 Active goal"
	goalNoteMax  = 80
	textModelFmt = "model - %s, effort - %s, tier - %s"
)

// infoNotes are the closing lines of a Codex message given what is known
// of its thread (answer: the message is an answer, not a status).
func infoNotes(info TurnInfo, answer bool) []channel.Note {
	var out []channel.Note
	if g := strings.TrimSpace(info.Goal); g != "" {
		out = append(out, channel.Note{
			Label: labelGoal, Text: shortLine(strings.Join(strings.Fields(g), " "), goalNoteMax),
		})
	}
	if answer && strings.TrimSpace(info.Model) != "" {
		effort := strings.TrimSpace(info.Effort)
		if effort == "" {
			effort = "default"
		}
		out = append(out, channel.Note{
			Label: labelHelper, Text: fmt.Sprintf(textModelFmt, strings.TrimSpace(info.Model), effort, tierName(info.Tier)),
		})
	}
	if len(out) > 0 {
		out[0].SeparatorBefore = true
	}
	return out
}

// tierName is the service tier as the owner knows it: Codex sends
// "priority" for fast (older Codex "fast"), "flex", and nothing or
// "default" for the standard tier.
func tierName(tier string) string {
	switch t := strings.ToLower(strings.TrimSpace(tier)); t {
	case "", "default", "standard", "auto":
		return "standard"
	case "priority", "fast":
		return "fast"
	default:
		return t
	}
}

// turnInfo is what the agent knows now of the thread of conv (zero
// without a GoalAgent).
func (e *Engine) turnInfo(conv int64) TurnInfo {
	ga, ok := e.ag.(GoalAgent)
	if !ok {
		return TurnInfo{}
	}
	th := e.threadOf(conv)
	if th == "" {
		return TurnInfo{}
	}
	return ga.Info(conv, th)
}
