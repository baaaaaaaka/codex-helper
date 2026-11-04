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
	Turn      int64  `json:"turn"`
	Conv      int64  `json:"conv"`
	Text      string `json:"text,omitempty"`
	Attempted bool   `json:"attempted,omitempty"` // the card post was started
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
		if p.Turn == n.Turn {
			return nil
		}
	}
	return writeNotify(tx, append(list, n))
}

// updateNotify applies fn to the entry of turn (removing it when fn
// returns false).
func (e *Engine) updateNotify(turn int64, fn func(*pendingNotify) bool) error {
	return e.update(func(tx *store.Tx) error {
		list, err := readNotify(tx)
		if err != nil {
			return err
		}
		out := list[:0]
		for _, p := range list {
			if p.Turn == turn && !fn(&p) {
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

func (e *Engine) pumpNotify(ctx context.Context) {
	var due []pendingNotify
	pending := 0
	err := e.update(func(tx *store.Tx) error {
		due, pending = due[:0], 0
		list, err := readNotify(tx)
		if err != nil {
			return err
		}
		pending = len(list)
		for _, n := range list {
			rows, err := tx.PendingOutbox(n.Conv)
			if err != nil {
				return err
			}
			prefix := fmt.Sprintf("turn:%d:", n.Turn)
			waiting := false
			for _, r := range rows {
				if strings.HasPrefix(r.DeliveryKey, prefix) {
					waiting = true
					break
				}
			}
			if !waiting {
				due = append(due, n)
			}
		}
		return nil
	})
	if err != nil {
		e.logf("owner notifications: %v", err)
		return
	}
	e.notifyN.Store(int64(pending))
	for _, n := range due {
		if ctx.Err() != nil {
			return
		}
		e.sendNotify(ctx, n)
	}
}

func (e *Engine) sendNotify(ctx context.Context, n pendingNotify) {
	e.mu.Lock()
	cs := e.convs[n.Conv]
	var label, title, url string
	if cs != nil {
		label, title, url = cs.label, cs.title, cs.url
	}
	e.mu.Unlock()
	if cs == nil {
		_ = e.updateNotify(n.Turn, func(*pendingNotify) bool { return false })
		return
	}
	card := !n.Attempted && e.cfg.Notifier != nil && e.cfg.Notifier.Available()
	if card {
		if err := e.updateNotify(n.Turn, func(p *pendingNotify) bool { p.Attempted = true; return true }); err != nil {
			e.logf("notify %s: %v", label, err)
			return
		}
		cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := e.cfg.Notifier.NotifyFinal(cctx, label, title, n.Text, url)
		cancel()
		if ctx.Err() != nil {
			return // the next start mentions the owner instead
		}
		if err == nil {
			if err := e.updateNotify(n.Turn, func(*pendingNotify) bool { return false }); err != nil {
				e.logf("notify %s: %v", label, err)
			}
			return
		}
		e.logf("notify %s: %v; mentioning the owner in the control chat instead", label, err)
	}
	msgs, err := e.mentionNotice(n.Turn, cs)
	if err == nil {
		err = e.update(func(tx *store.Tx) error {
			if len(msgs) > 0 {
				if _, err := tx.Enqueue(e.controlID, msgs); err != nil {
					return err
				}
			}
			list, err := readNotify(tx)
			if err != nil {
				return err
			}
			out := list[:0]
			for _, p := range list {
				if p.Turn != n.Turn {
					out = append(out, p)
				}
			}
			return writeNotify(tx, out)
		})
	}
	if err != nil {
		e.logf("notify %s: %v", label, err)
		return
	}
	e.wakeSender()
}
