package engine

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Owner notifications of finals (review L8). The turn's end records a
// pending notification row in the store, so a crash does
// not lose it; it is sent only once every part of the final has left the
// outbox, so the card or mention never arrives before the answer.
//
// The Workflow card is never retried: a failed post may still have been
// delivered. On failure, or when the helper stopped during the post, the
// owner gets one control-chat @mention instead (its delivery key dedupes).

type pendingNotify store.PendingNotification

// notifyLocal is the Kind of a live discovered session's notification
// (v0.1.20 local_session_started); "" is a final (turn_completed).
const notifyLocal = "local"
const notifyReady = "ready"
const notifyService = "service"
const notifyAttention = "attention"

const notifyScheduled = "scheduled"
const notifyWait = "wait"

const notifyQuestion = "question"

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

// unconfirmed reports the unresolved delivery state of output n announces:
// a part of a turn's answer (its text or files), or any post of a keyed
// notification.
func (n pendingNotify) unconfirmed(tx *store.Tx) (string, error) {
	// Questions still need their existing attention notification; questionNotice
	// changes its wording when the body itself could not be confirmed.
	if n.Kind == notifyQuestion {
		return "", nil
	}
	if n.Turn != 0 {
		return tx.UnconfirmedUnder(n.waitPrefix(), store.OutFinal)
	}
	return tx.UnconfirmedUnder(n.waitPrefix())
}

// addNotify registers one row in the same transaction as its final.
func (e *Engine) addNotify(tx *store.Tx, n pendingNotify) error {
	return tx.AddNotification(store.PendingNotification(n))
}

func (e *Engine) deleteNotify(id string) error {
	return e.update(func(tx *store.Tx) error { return tx.DeleteNotification(id) })
}

// notifyLoop sends due notifications. It is woken by a turn's end and by
// sender progress; the maintenance cadence is a safety net.
func (e *Engine) notifyLoop(ctx context.Context) {
	safety := time.NewTicker(e.t.Maintenance)
	defer safety.Stop()
	e.pumpNotify(ctx) // startup recovery, including events lost in a crash
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.notifyWake:
		case <-safety.C:
			e.wakeNotify()
		}
		// Coalesce bursts of sender wakes.
		select {
		case <-ctx.Done():
			return
		case <-time.After(e.t.Tick):
		}
		e.notifyMu.Lock()
		scopes := e.notifyDirty
		e.notifyDirty = nil
		e.notifyMu.Unlock()
		card := e.cfg.Notifier != nil && e.cfg.Notifier.Available()
		for _, scope := range scopes {
			e.pumpNotifyAvailable(ctx, scope, card)
		}
	}
}

// pumpNotify sends the due notifications, one per final (owner ruling
// 2026-11-05, Q3): the Workflow card when one is configured, else a
// control-chat @mention of its own. Finals are never merged into one
// notification.
func (e *Engine) pumpNotify(ctx context.Context) {
	e.pumpNotifyScope(ctx, store.NotificationScope{})
}

func (e *Engine) pumpNotifyScope(ctx context.Context, scope store.NotificationScope) {
	// Outside the transaction: Available can apply a pending Workflow setting.
	card := e.cfg.Notifier != nil && e.cfg.Notifier.Available()
	e.pumpNotifyAvailable(ctx, scope, card)
}

func (e *Engine) pumpNotifyAvailable(ctx context.Context, scope store.NotificationScope, card bool) {
	var cards, mentions, answered []pendingNotify
	var removed []string
	more := false
	err := e.st.Read(func(tx *store.Tx) error {
		var after int64
		bytes := 0
		for {
			list, last, err := tx.NotificationPageFor(scope, after, 128, !card)
			if err != nil {
				return err
			}
			if len(list) == 0 {
				return nil
			}
			after = last
			for _, saved := range list {
				n := pendingNotify(saved)
				if len(cards)+len(mentions)+len(removed) >= 128 || bytes >= 1<<20 {
					more = true
					return nil
				}
				if (n.Kind == notifyReady || n.Kind == notifyService || n.Kind == notifyAttention) && !card && !n.Attempted {
					removed = append(removed, n.id())
					continue
				}
				var questionGroup store.QuestionGroup
				questionDone := false
				if n.Kind == notifyQuestion {
					var ok bool
					questionGroup, ok, err = tx.QuestionGroup(n.Text)
					if err != nil {
						return err
					}
					if !ok {
						removed = append(removed, n.id())
						continue
					}
					questionDone = questionGroup.State != "open" || questionsReady(questionGroup)
				}
				active, err := tx.ActiveUnder(n.waitPrefix())
				if err != nil {
					return err
				}
				// A notification waits for the automatic title of its chat
				// (rename.go): it names the chat as the owner sees it.
				waiting := e.retitling(n.Conv)
				if n.Kind == notifyQuestion {
					waiting = false
					for _, q := range questionGroup.Questions {
						hit, err := tx.ActiveUnder(questionPrefix(q.ID) + ":")
						if err != nil {
							return err
						}
						waiting = waiting || hit
					}
				}
				waiting = waiting || active

				if !waiting {
					if n.Kind == notifyQuestion && questionDone {
						for _, q := range questionGroup.Questions {
							sent, err := tx.SentUnder(questionPrefix(q.ID) + ":")
							if err != nil {
								return err
							}
							if sent {
								answered = append(answered, n)
								break
							}
						}
						removed = append(removed, n.id())
						continue // resolved before notification: mark unread, no chat mention
					}
					// Only this notification waits for its own unconfirmed
					// answer. Other notifications, sends and tasks continue.
					state, err := n.unconfirmed(tx)
					if err != nil {
						return err
					}
					if state == store.OutDropped {
						removed = append(removed, n.id())
						continue
					}
					if state == store.OutUnknown {
						continue
					}
					if n.Kind == "" || n.Kind == notifyScheduled || n.Kind == notifyWait || n.Kind == notifyLocal || n.Kind == notifyQuestion {
						answered = append(answered, n)
					} // questions mark the work chat unread, but do not retitle it
				}
				if !waiting {
					bytes += len(n.Text) + len(n.Request) + len(n.Key)
				}
				switch {
				case waiting:
				case card && !n.Attempted:
					cards = append(cards, n)
				default:
					mentions = append(mentions, n)
				}
			}
		}
	})
	if err != nil {
		e.logf("owner notifications: %v", err)
		return
	}
	if len(removed) > 0 {
		if err := e.update(func(tx *store.Tx) error {
			for _, id := range removed {
				if err := tx.DeleteNotification(id); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			e.logf("owner notifications: %v", err)
			return
		}
	}
	if more {
		e.wakeNotify(scope)
	}
	// After the notifications named their chats (rename.go).
	defer func() { e.afterAnswers(e.notified(answered)) }()
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
		if n.Kind == notifyService {
			if control := e.convs[e.controlID]; control != nil {
				title, url = control.title, control.url
			}
		}
	}
	e.mu.Unlock()
	if cs == nil {
		_ = e.deleteNotify(n.id())
		return true
	}
	var claimed bool
	if err := e.update(func(tx *store.Tx) (err error) { claimed, err = tx.AttemptNotification(n.id()); return }); err != nil {
		e.logf("notify %s: %v", label, err)
		return true
	}
	if !claimed {
		return true
	}
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	var err error
	if n.Kind == notifyQuestion {
		var g store.QuestionGroup
		err = e.st.Read(func(tx *store.Tx) (err error) { g, _, err = tx.QuestionGroup(n.Text); return })
		if err == nil && (g.State != "open" || questionsReady(g)) {
			cancel()
			_ = e.deleteNotify(n.id())
			return true
		}
		var heading, text string
		if err == nil {
			heading, text, err = e.questionNotice(g, firstNonEmpty(title, label))
		}
		if err == nil {
			err = e.cfg.Notifier.Notify(cctx, channel.OwnerNotice{Heading: heading, ChatTitle: firstNonEmpty(title, label), Hint: text, URL: url})
		}
	} else {
		heading, hint := resultNotice(n.Kind)
		notice := channel.OwnerNotice{Heading: heading, ChatTitle: firstNonEmpty(title, label), Request: n.Request, Hint: hint, Button: "Open answer", URL: url}
		switch n.Kind {
		case notifyScheduled:
			notice.Request, _, _ = strings.Cut(n.Text, "\n")
			if notice.Request == notice.Heading {
				notice.Request = ""
			}
		case notifyService:
			notice.Heading, notice.Hint, notice.Request, notice.Button = n.Request, n.Text, "", "Open Control"
		case notifyAttention:
			notice.Heading, notice.Hint, notice.Button = "⚠️ Codex needs attention", n.Text, "Open chat"
		case notifyLocal:
			notice.Heading = "💬 New local Codex chat detected"
			notice.Hint = "Open this chat to watch the answer and continue from the full local context."
			notice.Button = "Open chat"
		case notifyReady:
			notice.Heading = "💬 Codex chat ready"
			notice.Hint = "Open this chat to start or continue the task."
			notice.Button = "Open chat"
		}
		err = e.cfg.Notifier.Notify(cctx, notice)
	}
	cancel()
	if ctx.Err() != nil {
		return true // the next start mentions the owner instead
	}
	if err == nil {
		if err := e.deleteNotify(n.id()); err != nil {
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
		title, url := "", ""
		if cs != nil {
			title, url = cs.title, cs.url
		}
		e.mu.Unlock()
		ids = append(ids, n.id())
		if cs == nil {
			continue
		}
		var (
			m   []store.OutMsg
			err error
		)
		if n.Kind == notifyQuestion {
			m, err = e.questionMention(n, cs)
		} else if n.Kind == notifyService {
			m, err = e.render(context.Background(), channel.RichDoc{Label: n.Request, Markdown: n.Text, Mention: true}, store.OutNotice, "notify:"+n.Key)
		} else if n.Kind == notifyAttention {
			m, err = e.render(context.Background(), channel.RichDoc{Label: "⚠️ Codex needs attention", Markdown: n.Text + "\n\n" + url, Mention: true}, store.OutNotice, fmt.Sprintf("notify:attention:%d", n.Turn))
		} else if n.Kind == notifyReady {
			m, err = e.render(context.Background(), channel.RichDoc{Label: "💬 Codex chat ready", Markdown: title + "\n\n" + url, Mention: true}, store.OutNotice, "notify:"+n.Key)
		} else if n.Kind == notifyLocal {
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
		for _, id := range ids {
			if err := tx.DeleteNotification(id); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		e.logf("owner notification: %v", err)
		return
	}
	e.wakeSender()
}

// addServiceNotify accompanies an already durable lifecycle confirmation.
// With no webhook, that existing confirmation is sufficient.
func (e *Engine) addServiceNotify(tx *store.Tx, conv int64, key, text string) error {
	if e.cfg.Notifier == nil {
		return nil
	}
	heading, hint, _ := strings.Cut(text, "\n\n")
	return e.addNotify(tx, pendingNotify{Conv: conv, Key: key + ":", Kind: notifyService, Request: heading, Text: hint})
}
