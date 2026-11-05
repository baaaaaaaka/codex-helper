package engine

import (
	"context"
	"errors"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

// Registrar publishes this machine's card in the cross-machine registry
// (F-XM-1): requesters (`cxp delegate resolve`) find the machine, whether
// it accepts work and the inbox to post requests to from that card. The
// Teams implementation is internal/core/delegate.
type Registrar interface {
	Publish(ctx context.Context, card RegistryCard) error
}

// RegistryCard is what the engine reports in the card.
type RegistryCard struct {
	// InboxChat and InboxTitle identify the delegation inbox chat.
	InboxChat, InboxTitle string
	// Draining is set while the helper stops or drains for an update; the
	// card then says it does not accept new work.
	Draining bool
}

const (
	// registryStopPublish bounds the draining card published on shutdown.
	registryStopPublish = 10 * time.Second
	// registryMaxBackoff caps the retry delay of a failing publish.
	registryMaxBackoff = 30 * time.Minute
)

// registryLoop publishes the card every Timing.Registry once the inbox chat
// exists, and a draining card when the engine stops.
func (e *Engine) registryLoop(ctx context.Context) {
	var backoff time.Duration
	t := time.NewTimer(0)
	defer t.Stop()
	published := false
	for {
		select {
		case <-ctx.Done():
			if published {
				card, _ := e.registryCard(true)
				sctx, cancel := context.WithTimeout(context.Background(), registryStopPublish)
				if err := e.cfg.Registrar.Publish(sctx, card); err != nil {
					e.logf("registry: draining card: %v", err)
				}
				cancel()
			}
			return
		case <-t.C:
		case <-e.registryWake:
			// The drain state changed, or the inbox chat was created:
			// publish now, unless a failed publish waits out its backoff.
			if backoff > 0 {
				continue
			}
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
		}
		card, ok := e.registryCard(e.draining())
		if !ok {
			// The sender creates the inbox chat (requestChat) and wakes
			// this loop; the timer is a backstop.
			t.Reset(e.t.Control)
			continue
		}
		err := e.cfg.Registrar.Publish(ctx, card)
		if err == nil {
			published, backoff = true, 0
			t.Reset(e.t.Registry)
			continue
		}
		if ctx.Err() != nil {
			continue
		}
		e.logf("registry: publish card: %v", err)
		var th *channel.Throttled
		switch {
		case errors.As(err, &th) && th.RetryAfter > 0:
			backoff = max(backoff, th.RetryAfter)
			t.Reset(th.RetryAfter)
		default:
			backoff = min(max(2*backoff, e.t.Registry), registryMaxBackoff)
			t.Reset(backoff)
		}
	}
}

// registryCard returns the card once the inbox chat exists.
func (e *Engine) registryCard(draining bool) (RegistryCard, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	cs := e.convs[e.inboxID]
	if cs == nil || cs.chat == "" {
		return RegistryCard{}, false
	}
	return RegistryCard{InboxChat: cs.chat, InboxTitle: cs.title, Draining: draining}, true
}

// draining reports whether new turn starts are held for an update.
func (e *Engine) draining() bool { return e.drainFlag.Load() }
