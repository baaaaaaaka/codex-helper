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
	label, err := e.confirmationLabel(r.ConvID)
	if err != nil {
		return err
	}
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
	return ann.Annotate(ctx, chat, r.AppendTo, channel.Annotation{Confirmation: &p, ConfirmationKey: r.DeliveryKey, Label: label, At: fromMS(r.CreatedAt).Local()})
}

// Recovered receipts wait for the first membership check. Do not add owner
// labels in shared chats; media labels belong to transcript annotation.
func (e *Engine) confirmationLabel(conv int64) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	cs := e.convs[conv]
	if cs != nil && !cs.readOnce {
		return "", errEditLater
	}
	if conv == e.inboxID || cs == nil || cs.group {
		return "", nil
	}
	return labelUser, nil
}
