package engine

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

const (
	questionMarkerPrefix = "❓Pending agent question"
	questionMarkerMeta   = "engine_question_marker:"
)

func questionMarkerKey(conv int64) string {
	return questionMarkerMeta + fmt.Sprint(conv)
}

func (e *Engine) questionIsPending(conv int64) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.questionPending[conv]
}

func isQuestionMarkerMessage(r store.Outbox) bool {
	if r.Part != r.Parts {
		return false
	}
	return r.Kind == store.OutFinal || r.Kind == store.OutProgress ||
		r.Kind == store.OutMirror && isAgentMirrorBody(r.Body) ||
		r.Kind == store.OutNotice && strings.HasPrefix(r.DeliveryKey, "question:")
}

// OutMirror also carries local user prompts. Its rendered label distinguishes
// those prompts from mirrored Codex output.
func isAgentMirrorBody(body string) bool {
	return strings.Contains(body, "<strong>🤖 ✅ Codex answer:") ||
		strings.Contains(body, "<strong>🤖 ⏳ Codex status:")
}

// queueQuestionMarker keeps one first-line marker on the latest sent Codex
// message while the conversation has an unanswered question. The persisted
// id lets startup repair a marker edit interrupted by a crash.
func (e *Engine) queueQuestionMarker(conv int64) {
	if !e.ch.Caps().EditMessages {
		return
	}
	if _, ok := e.cfg.Channel.(channel.MessagePrefixEditor); !ok {
		return
	}
	editor := e.ch.(channel.MessagePrefixEditor)
	e.queueEditLast(conv, chatEdit{key: "question-marker", what: "pending-question marker", run: func(ctx context.Context, chat string) error {
		var pending bool
		var old string
		var latest store.Outbox
		var hasLatest bool
		if err := e.update(func(tx *store.Tx) (err error) {
			pending, err = tx.HasPendingQuestion(conv)
			if err != nil {
				return err
			}
			old, _, err = tx.Meta(questionMarkerKey(conv))
			if err != nil {
				return err
			}
			latest, hasLatest, err = tx.LatestQuestionMarkerMessage(conv)
			return err
		}); err != nil {
			return err
		}
		desired := ""
		if pending && hasLatest {
			desired = latest.ExtMsgID
		}
		if old == desired {
			if desired == "" {
				return nil
			}
			if err := e.waitRead(ctx, editReadWait); err != nil {
				return err
			}
			return editor.SetMessagePrefix(ctx, chat, desired, questionMarkerPrefix, true)
		}
		if old != "" {
			if err := e.waitRead(ctx, editReadWait); err != nil {
				return err
			}
			if err := editor.SetMessagePrefix(ctx, chat, old, questionMarkerPrefix, false); err != nil && !errors.Is(err, channel.ErrNotFound) {
				return err
			}
		}
		if err := e.update(func(tx *store.Tx) error {
			if desired == "" {
				return tx.DeleteMeta(questionMarkerKey(conv))
			}
			return tx.SetMeta(questionMarkerKey(conv), desired)
		}); err != nil {
			return err
		}
		if desired == "" {
			return nil
		}
		if old != "" {
			if err := e.waitWrite(ctx); err != nil {
				return err
			}
		}
		if err := e.waitRead(ctx, editReadWait); err != nil {
			return err
		}
		return editor.SetMessagePrefix(ctx, chat, desired, questionMarkerPrefix, true)
	}})
	e.wakeSender()
}

func (e *Engine) resumeQuestionMarkers() {
	var pending []int64
	var markers map[string]string
	if err := e.update(func(tx *store.Tx) (err error) {
		pending, err = tx.PendingQuestionConversations()
		if err != nil {
			return err
		}
		markers, err = tx.MetaPrefix(questionMarkerMeta)
		return err
	}); err != nil {
		e.logf("recover pending-question markers: %v", err)
		return
	}
	convs := make(map[int64]bool, len(pending)+len(markers))
	pendingSet := make(map[int64]bool, len(pending))
	for _, conv := range pending {
		convs[conv] = true
		pendingSet[conv] = true
	}
	e.mu.Lock()
	e.questionPending = pendingSet
	e.mu.Unlock()
	for key, id := range markers {
		if id == "" {
			continue
		}
		conv, err := strconv.ParseInt(strings.TrimPrefix(key, questionMarkerMeta), 10, 64)
		if err == nil && conv > 0 {
			convs[conv] = true
		}
	}
	for conv := range convs {
		e.queueQuestionMarker(conv)
	}
}
