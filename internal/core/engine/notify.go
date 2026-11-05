package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Owner notifications of finals (review L8). The turn's end records a
// pending notification in the store (meta engine_notify), so a crash does
// not lose it; it is sent only once every part of the final has left the
// outbox, so the card or mention never arrives before the answer.
//
// The Workflow card is never retried: a failed post may still have been
// delivered. On failure, or when the helper stopped during the post, the
// owner gets one control-chat @mention instead (its delivery key dedupes).

const metaNotify = "engine_notify"

type pendingNotify struct {
	Turn int64 `json:"turn"`
	Conv int64 `json:"conv"`
	// Key identifies a notification that is not a Teams-run turn's final
	// and is the delivery-key prefix of the posts it waits for: a live
	// mirrored final ("mirror:<thread>:<record>:") or the notice of a live
	// discovered session ("ready:<label>:", Kind notifyLocal).
	Key       string `json:"key,omitempty"`
	Kind      string `json:"kind,omitempty"`
	Text      string `json:"text,omitempty"`
	Request   string `json:"request,omitempty"`   // the prompt, summarised (requestSummary)
	Attempted bool   `json:"attempted,omitempty"` // the card post was started
}

// notifyLocal is the Kind of a live discovered session's notification
// (v0.1.20 local_session_started); "" is a final (turn_completed).
const notifyLocal = "local"

// id identifies n in the pending list.
func (n pendingNotify) id() string {
	if n.Turn != 0 {
		return fmt.Sprintf("turn:%d", n.Turn)
	}
	return n.Key
}

// waitPrefix is the delivery-key prefix of the posts n waits for.
func (n pendingNotify) waitPrefix() string {
	if n.Turn != 0 {
		return fmt.Sprintf("turn:%d:", n.Turn)
	}
	return n.Key
}

// undelivered reports whether a post n announces was dropped unconfirmed:
// a part of a turn's answer (its text or files), or any post of a keyed
// notification.
func (n pendingNotify) undelivered(tx *store.Tx) (bool, error) {
	if n.Turn != 0 {
		return tx.DroppedUnder(n.waitPrefix(), store.OutFinal)
	}
	return tx.DroppedUnder(n.waitPrefix())
}

func readNotify(tx *store.Tx) ([]pendingNotify, error) {
	v, ok, err := tx.Meta(metaNotify)
	if err != nil || !ok || v == "" {
		return nil, err
	}
	var list []pendingNotify
	if err := json.Unmarshal([]byte(v), &list); err != nil {
		return nil, nil // unreadable: nothing can be done with it
	}
	return list, nil
}

func writeNotify(tx *store.Tx, list []pendingNotify) error {
	if len(list) == 0 {
		return tx.SetMeta(metaNotify, "")
	}
	b, err := json.Marshal(list)
	if err != nil {
		return err
	}
	return tx.SetMeta(metaNotify, string(b))
}

// addNotify records the notification of a finished turn in tx.
func addNotify(tx *store.Tx, n pendingNotify) error {
	list, err := readNotify(tx)
	if err != nil {
		return err
	}
	for _, p := range list {
		if p.id() == n.id() {
			return nil
		}
	}
	return writeNotify(tx, append(list, n))
}

// updateNotify applies fn to the entry id (removing it when fn returns
// false).
func (e *Engine) updateNotify(id string, fn func(*pendingNotify) bool) error {
	return e.update(func(tx *store.Tx) error {
		list, err := readNotify(tx)
		if err != nil {
			return err
		}
		out := list[:0]
		for _, p := range list {
			if p.id() == id && !fn(&p) {
				continue
			}
			out = append(out, p)
		}
		return writeNotify(tx, out)
	})
}

// notifyLoop sends due notifications. It is woken by a turn's end and by
// sender progress; the maintenance cadence is a safety net.
func (e *Engine) notifyLoop(ctx context.Context) {
	safety := time.NewTicker(e.t.Maintenance)
	defer safety.Stop()
	for {
		e.pumpNotify(ctx)
		select {
		case <-ctx.Done():
			return
		case <-e.notifyWake:
		case <-safety.C:
		}
		// Coalesce bursts of sender wakes.
		select {
		case <-ctx.Done():
			return
		case <-time.After(e.t.Tick):
		}
	}
}

// pumpNotify sends the due notifications, one per final (owner ruling
// 2026-11-05, Q3): the Workflow card when one is configured, else a
// control-chat @mention of its own. Finals are never merged into one
// notification.
func (e *Engine) pumpNotify(ctx context.Context) {
	var cards, mentions []pendingNotify
	pending := 0
	// Outside the transaction: Available reads the Workflow setting files
	// and may apply a pending change (a store write of its own).
	card := e.cfg.Notifier != nil && e.cfg.Notifier.Available()
	err := e.update(func(tx *store.Tx) error {
		cards, mentions, pending = cards[:0], mentions[:0], 0
		list, err := readNotify(tx)
		if err != nil {
			return err
		}
		keep := list[:0]
		for _, n := range list {
			rows, err := tx.PendingOutbox(n.Conv)
			if err != nil {
				return err
			}
			prefix := n.waitPrefix()
			waiting := false
			for _, r := range rows {
				if strings.HasPrefix(r.DeliveryKey, prefix) {
					waiting = true
					break
				}
			}
			if !waiting {
				// An answer that was never confirmed delivered is not
				// announced: the chat carries the "could not be
				// confirmed" notice instead (R-5b).
				gone, err := n.undelivered(tx)
				if err != nil {
					return err
				}
				if gone {
					continue
				}
			}
			keep = append(keep, n)
			switch {
			case waiting:
			case card && !n.Attempted:
				cards = append(cards, n)
			default:
				mentions = append(mentions, n)
			}
		}
		pending = len(keep)
		if len(keep) < len(list) {
			return writeNotify(tx, keep)
		}
		return nil
	})
	if err != nil {
		e.logf("owner notifications: %v", err)
		return
	}
	e.notifyN.Store(int64(pending))
	for _, n := range cards {
		if ctx.Err() != nil {
			return
		}
		if !e.sendCard(ctx, n) {
			mentions = append(mentions, n) // failed: the owner is mentioned instead
		}
	}
	if len(mentions) == 0 || ctx.Err() != nil {
		return
	}
	e.sendMentions(mentions)
}

// sendCard posts the Workflow card of n once (never retried: a failed post
// may still have been delivered). It reports whether the card was posted
// or nothing more is to be done for n.
func (e *Engine) sendCard(ctx context.Context, n pendingNotify) bool {
	e.mu.Lock()
	cs := e.convs[n.Conv]
	var label, title, url string
	if cs != nil {
		label, title, url = cs.label, cs.title, cs.url
	}
	e.mu.Unlock()
	if cs == nil {
		_ = e.updateNotify(n.id(), func(*pendingNotify) bool { return false })
		return true
	}
	ev, events := e.cfg.Notifier.(EventNotifier)
	if n.Kind == notifyLocal && !events {
		return false // no card for it: the owner is mentioned instead
	}
	if err := e.updateNotify(n.id(), func(p *pendingNotify) bool { p.Attempted = true; return true }); err != nil {
		e.logf("notify %s: %v", label, err)
		return true
	}
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	var err error
	if n.Kind == notifyLocal {
		err = ev.NotifyEvent(cctx, "💬 New local Codex chat detected: "+firstNonEmpty(title, label),
			"Open this chat to watch the answer and continue from the full local context.", url)
	} else {
		err = e.cfg.Notifier.NotifyFinal(cctx, label, title, n.Text, url)
	}
	cancel()
	if ctx.Err() != nil {
		return true // the next start mentions the owner instead
	}
	if err == nil {
		if err := e.updateNotify(n.id(), func(*pendingNotify) bool { return false }); err != nil {
			e.logf("notify %s: %v", label, err)
		}
		return true
	}
	e.logf("notify %s: %v; mentioning the owner in the control chat instead", label, err)
	return false
}

// sendMentions enqueues one control-chat @mention per entry of set and
// removes them from the pending list in the same transaction.
func (e *Engine) sendMentions(set []pendingNotify) {
	var (
		ids  []string
		msgs []store.OutMsg
	)
	for _, n := range set {
		e.mu.Lock()
		cs := e.convs[n.Conv]
		e.mu.Unlock()
		ids = append(ids, n.id())
		if cs == nil {
			continue
		}
		var (
			m   []store.OutMsg
			err error
		)
		if n.Kind == notifyLocal {
			m, err = e.localMention(cs)
		} else {
			m, err = e.finalMention(n, cs)
		}
		if err != nil {
			e.logf("owner notification: %v", err)
			return
		}
		msgs = append(msgs, m...)
	}
	err := e.update(func(tx *store.Tx) error {
		if len(msgs) > 0 {
			if _, err := tx.Enqueue(e.controlID, msgs); err != nil {
				return err
			}
		}
		list, err := readNotify(tx)
		if err != nil {
			return err
		}
		done := map[string]bool{}
		for _, id := range ids {
			done[id] = true
		}
		out := list[:0]
		for _, p := range list {
			if !done[p.id()] {
				out = append(out, p)
			}
		}
		return writeNotify(tx, out)
	})
	if err != nil {
		e.logf("owner notification: %v", err)
		return
	}
	e.wakeSender()
}
