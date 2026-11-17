package engine

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
)

func (e *Engine) nativeWait(conv, turn int64, rs *runState, id string, duration time.Duration, started bool) {
	if id == "" || duration <= 0 || duration > 12*time.Hour {
		return
	}
	rs.silenceMu.Lock()
	defer rs.silenceMu.Unlock()
	e.mu.Lock()
	if cs := e.convs[conv]; cs == nil || cs.running != rs || rs.turnID != turn || rs.cancelled || rs.switching {
		e.mu.Unlock()
		return
	}
	if !started {
		ended := rs.waitID == id && !rs.waitUntil.IsZero()
		if rs.waitID == "" {
			// A completion can arrive before its start response. Remember it
			// without inventing a start or an end message.
			rs.waitID = id
		}
		announced := rs.waitAnnounced
		if ended {
			rs.waitUntil = time.Time{}
			rs.lastEvent.Store(ms(e.now()))
		}
		e.mu.Unlock()
		if ended && announced {
			e.waitNotice(conv, fmt.Sprintf("turn:%d:wait:%s:end", turn, id), "🕒 Wait ended", "Continuing the task.")
		}
		return
	}
	if rs.waitID == id {
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()
	// The durable outbox also rejects an older wait replayed after a newer
	// wait. No growing in-memory set of item IDs is needed.
	added, err := e.waitNotice(conv, fmt.Sprintf("turn:%d:wait:%s:start", turn, id), "🕒 Wait started", fmt.Sprintf("Waiting for up to %s. New input can end the wait early.", duration))
	if err == nil && !added {
		return // an older start already exists in the outbox
	}
	e.mu.Lock()
	if cs := e.convs[conv]; cs != nil && cs.running == rs && !rs.cancelled && !rs.switching {
		rs.waitID, rs.waitUntil = id, e.now().Add(duration)
		rs.waitAnnounced = err == nil && added
		rs.lastEvent.Store(ms(e.now()))
	}
	e.mu.Unlock()
}

// Called under the per-run silence lock so wait events retain their order.
// Enqueue's indexed delivery key deduplicates all phases, including replay.
func (e *Engine) waitNotice(conv int64, key, label, text string) (bool, error) {
	if conv == e.inboxID {
		return true, nil // the inbox carries protocol records only
	}
	msgs, err := e.render(context.Background(), channel.RichDoc{Label: label, Markdown: text}, store.OutNotice, key)
	if err != nil {
		e.logf("wait event in %d: %v", conv, err)
		return false, err
	}
	added := 0
	err = e.update(func(tx *store.Tx) error {
		var err error
		added, err = tx.Enqueue(conv, msgs)
		return err
	})
	if err != nil {
		e.logf("wait event in %d: %v", conv, err)
		return false, err
	}
	if added > 0 {
		e.wakeSender()
	}
	return added > 0, nil
}

// scheduledDoc presents the native scheduled input as a single execution
// event. Later messages retain their normal labels and completion footer.
// Reuse the input's delivery identity and checkpoint for replay deduplication.
func scheduledDoc(doc channel.RichDoc, o transcript.Output) channel.RichDoc {
	if o.Schedule == nil || !(o.User || o.Kind == transcript.OutputPrompt) {
		return doc
	}
	doc.Label = "🕒 Scheduled task started"
	if o.Schedule.ID != "" {
		doc.Label += " (" + o.Schedule.ID + ")"
		doc.Markdown = o.Schedule.Instructions
		if strings.TrimSpace(doc.Markdown) == "" {
			doc.Markdown = "_No instructions were included in this scheduled input._"
		}
	}
	return doc
}
