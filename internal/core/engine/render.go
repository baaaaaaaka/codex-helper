package engine

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
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

// notice renders a helper notice ("🔧 Helper: text").
func (e *Engine) notice(base, text string) ([]store.OutMsg, error) {
	return e.render(context.Background(), channel.RichDoc{Label: labelHelper, Markdown: text}, store.OutNotice, base)
}

// noticeReply is notice quoting the message it answers (group chats).
func (e *Engine) noticeReply(kind, base, text, replyTo string) ([]store.OutMsg, error) {
	return e.render(context.Background(), channel.RichDoc{Label: labelHelper, Markdown: text, ReplyTo: replyTo}, kind, base)
}

// enqueueNotice renders and enqueues a notice in tx.
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
	textInterrupted = "The previous request was interrupted when the helper stopped. It was not re-run; resend it if you still need it."
	textNotStarted  = "The previous request did not run: the helper stopped before Codex accepted it. Resend it if you still need it."
	textRestarted   = "Helper restarted: the new process is running and has taken over."
	textHeld        = "Request held: this chat is paused, so new requests do not run. It will not run later by itself; resend it once the chat is resumed."
	textDuplicate   = "Duplicate request ignored (same text within 3 minutes). Send `helper retry` to run it again."
	textActive      = "Codex did not run this request: the session is open in another Codex window that is writing to it. Close it there (or wait until it is idle) and resend."
	textUnconfirmed = "A reply could not be confirmed as delivered, so it was not sent again to avoid a duplicate. Ask again if it is missing."
	// textUnconfirmedRetried: an answer was sent a second time and that copy
	// could not be confirmed either.
	textUnconfirmedRetried = "A reply could not be confirmed as delivered, even after sending it a second time. Ask again if it is missing."
)
