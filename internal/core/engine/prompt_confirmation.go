package engine

import (
	"context"
	"errors"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// appendConfirmation uses the durable outbox, not the cosmetic edit queue.
// The caller records completion without an external post id: the original
// message must remain a user message for own-post recognition.
func (e *Engine) appendConfirmation(ctx context.Context, r store.Outbox, chat string, p channel.Part) error {
	ann, ok := e.annotator()
	if !ok {
		return channel.ErrNotEditable
	}
	if err := e.waitRead(ctx, editReadWait); err != nil {
		if errors.Is(err, errEditLater) && r.NextAttemptAt == 0 {
			// No earlier attempt: a read throttle need not hold later POSTs.
			// Retries must read back a PATCH that may already have landed.
			return channel.ErrNotEditable
		}
		return err
	}
	return ann.Annotate(ctx, chat, r.AppendTo, channel.Annotation{Confirmation: &p, ConfirmationKey: r.DeliveryKey, Label: e.confirmationLabel(r.ConvID), At: fromMS(r.CreatedAt).Local()})
}

// Membership is already known before admission. Do not add owner labels in
// shared chats; media messages defer their label to transcript annotation.
func (e *Engine) confirmationLabel(conv int64) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	cs := e.convs[conv]
	if conv == e.inboxID || cs == nil || !cs.readOnce || cs.group {
		return ""
	}
	return labelUser
}
