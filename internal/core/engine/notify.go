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
// sender progress; the maintenance cadence is a safety net, and a timer
// ends a mention coalescing window.
func (e *Engine) notifyLoop(ctx context.Context) {
	safety := time.NewTicker(e.t.Maintenance)
	defer safety.Stop()
	window := time.NewTimer(time.Hour)
	window.Stop()
	defer window.Stop()
	for {
		if next := e.pumpNotify(ctx); !next.IsZero() {
			window.Reset(max(next.Sub(e.now()), e.t.Tick))
		}
		select {
		case <-ctx.Done():
			return
		case <-e.notifyWake:
		case <-safety.C:
		case <-window.C:
		}
		// Coalesce bursts of sender wakes.
		select {
		case <-ctx.Done():
			return
		case <-time.After(e.t.Tick):
		}
	}
}

// mentionSetMax bounds the finals one coalesced @mention names.
const mentionSetMax = 20

// pumpNotify sends the due notifications: a Workflow card per final, or
// control-chat @mentions coalesced per window (benchmark F2): the first
// mention goes at once; finals delivered within MentionWindow after it
// (three windows while the account's writes are paced) wait and are named
// together in one mention ("Codex finished in A, B and C"), each exactly
// once. It returns when the window ends ("" when nothing waits for it).
func (e *Engine) pumpNotify(ctx context.Context) time.Time {
	var cards, mentions []pendingNotify
	pending := 0
	err := e.update(func(tx *store.Tx) error {
		cards, mentions, pending = cards[:0], mentions[:0], 0
		list, err := readNotify(tx)
		if err != nil {
			return err
		}
		pending = len(list)
		card := e.cfg.Notifier != nil && e.cfg.Notifier.Available()
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
			switch {
			case waiting:
			case card && !n.Attempted:
				cards = append(cards, n)
			default:
				mentions = append(mentions, n)
			}
		}
		return nil
	})
	if err != nil {
		e.logf("owner notifications: %v", err)
		return time.Time{}
	}
	e.notifyN.Store(int64(pending))
	for _, n := range cards {
		if ctx.Err() != nil {
			return time.Time{}
		}
		if !e.sendCard(ctx, n) {
			mentions = append(mentions, n) // failed: the owner is mentioned instead
		}
	}
	if len(mentions) == 0 || ctx.Err() != nil {
		return time.Time{}
	}
	window := e.t.MentionWindow
	if e.writeTokens.limited() {
		window *= 3
	}
	if next := e.lastMention.Add(window); !e.lastMention.IsZero() && e.now().Before(next) {
		return next
	}
	if len(mentions) > mentionSetMax {
		mentions = mentions[:mentionSetMax]
	}
	if e.sendMentions(mentions) {
		e.lastMention = e.now()
	}
	if pending > len(mentions)+len(cards) {
		return e.lastMention.Add(window) // more are waiting
	}
	return time.Time{}
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
		_ = e.updateNotify(n.Turn, func(*pendingNotify) bool { return false })
		return true
	}
	if err := e.updateNotify(n.Turn, func(p *pendingNotify) bool { p.Attempted = true; return true }); err != nil {
		e.logf("notify %s: %v", label, err)
		return true
	}
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err := e.cfg.Notifier.NotifyFinal(cctx, label, title, n.Text, url)
	cancel()
	if ctx.Err() != nil {
		return true // the next start mentions the owner instead
	}
	if err == nil {
		if err := e.updateNotify(n.Turn, func(*pendingNotify) bool { return false }); err != nil {
			e.logf("notify %s: %v", label, err)
		}
		return true
	}
	e.logf("notify %s: %v; mentioning the owner in the control chat instead", label, err)
	return false
}

// sendMentions enqueues one control-chat @mention naming the finals of set
// and removes them from the pending list in the same transaction.
func (e *Engine) sendMentions(set []pendingNotify) bool {
	var (
		turns []int64
		convs []*convState
	)
	e.mu.Lock()
	for _, n := range set {
		turns = append(turns, n.Turn)
		if cs := e.convs[n.Conv]; cs != nil {
			convs = append(convs, cs)
		}
	}
	e.mu.Unlock()
	var msgs []store.OutMsg
	var err error
	if len(convs) > 0 {
		msgs, err = e.mentionNotice(turns, convs)
	}
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
			done := map[int64]bool{}
			for _, t := range turns {
				done[t] = true
			}
			out := list[:0]
			for _, p := range list {
				if !done[p.Turn] {
					out = append(out, p)
				}
			}
			return writeNotify(tx, out)
		})
	}
	if err != nil {
		e.logf("owner notification: %v", err)
		return false
	}
	e.wakeSender()
	return len(msgs) > 0
}
