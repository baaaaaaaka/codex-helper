package engine

import (
	"context"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

// Refresh after startup, membership events, and at most every five minutes
// during ordinary polling. Historical senders cannot override this snapshot.
// Failed checks preserve the old state and follow normal read backoff.
func (e *Engine) currentParticipants(ctx context.Context, id int64, chat string, w *channel.Window) error {
	reader, ok := e.cfg.Channel.(channel.ParticipantReader)
	if !ok {
		return nil
	}
	e.mu.Lock()
	cs := e.convs[id]
	refresh := !cs.membersKnown || e.now().Sub(cs.membersChecked) >= 5*time.Minute
	for _, m := range w.Msgs {
		if _, seen := cs.seen[m.ID]; !seen && (m.Kind == channel.KindSystem ||
			!cs.membersOther && m.Kind == channel.KindMessage && !m.ByOwner && m.From != "" && !strings.HasPrefix(m.From, "app:")) {
			refresh = true
		}
	}
	group := cs.membersOther
	e.mu.Unlock()
	if refresh {
		if err := e.waitRead(ctx, time.Minute); err != nil {
			return err
		}
		var err error
		group, err = reader.OtherParticipants(ctx, chat)
		if err != nil {
			return err
		}
		e.mu.Lock()
		cs.membersKnown = true
		cs.membersOther = group
		cs.membersChecked = e.now()
		e.mu.Unlock()
		w.Requests++
	}
	w.OtherParticipants = &group
	return nil
}
