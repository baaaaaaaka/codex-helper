package engine

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

const (
	questionMarkerPrefix       = "❓Pending agent question"
	questionMarkerMeta         = "engine_question_marker:"
	questionMarkerPreviousMeta = "engine_question_marker_previous:"
)

func questionMarkerKey(conv int64) string {
	return questionMarkerMeta + fmt.Sprint(conv)
}

func questionMarkerPreviousKey(conv int64) string {
	return questionMarkerPreviousMeta + fmt.Sprint(conv)
}

func (e *Engine) questionIsPending(conv int64) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.questionPending[conv]
}

// The sender delivers command replies and files as well as agent output.
// Protocol-only conversations are excluded by queueQuestionMarker itself.
func isQuestionMarkerMessage(r store.Outbox) bool { return r.Kind != store.OutTitle }

// Keep the current and previous target IDs across a handoff. The original
// current-ID key retains its single-ID format for existing installations.
func (e *Engine) queueQuestionMarker(conv int64) {
	if conv == e.inboxID || !e.ch.Caps().EditMessages {
		return
	}
	if _, ok := e.cfg.Channel.(channel.QuestionDecorator); !ok {
		return
	}
	editor := e.ch.(channel.QuestionDecorator)
	e.queueEditLast(conv, chatEdit{key: "question-marker", what: "pending questions", run: func(ctx context.Context, chat string) (result error) {
		// Directory delivery will schedule the next refresh through the normal sender.
		defer func() {
			if errors.Is(result, errQuestionDirectoryQueued) {
				result = nil
			}
		}()

		var saved, previous string
		var links []store.QuestionLink
		var latest store.Outbox
		var hasLatest bool
		if err := e.st.Read(func(tx *store.Tx) (err error) {
			links, err = tx.PendingQuestionLinks(conv)
			if err != nil {
				return err
			}
			saved, _, err = tx.Meta(questionMarkerKey(conv))
			if err != nil {
				return err
			}
			previous, _, err = tx.Meta(questionMarkerPreviousKey(conv))
			if err != nil {
				return err
			}
			latest, hasLatest, err = tx.LatestQuestionMarkerMessage(conv)
			return err
		}); err != nil {
			return err
		}
		questions := pendingQuestionItems(links)
		directoryKey := questionDirectoryKey(questions)
		directoryRows, err := e.st.OutboxByKeyPrefix(directoryKey + ":")
		if err != nil {
			return err
		}
		if len(directoryRows) > 0 {
			pages := make([]channel.PendingQuestion, 0, len(directoryRows))
			for _, row := range directoryRows {
				if row.State != store.OutSent || row.ExtMsgID == "" {
					return nil
				}
				pages = append(pages, channel.PendingQuestion{ID: fmt.Sprintf("Questions · page %d/%d", row.Part, row.Parts), MessageID: row.ExtMsgID})
			}
			questions = pages
		}
		desired := ""
		if len(questions) > 0 && hasLatest {
			desired = latest.ExtMsgID
		}
		current := saved
		writes := 0
		apply := func(id string, qs []channel.PendingQuestion) error {
			if writes > 0 {
				if err := e.waitWrite(ctx); err != nil {
					return err
				}
			}
			if err := e.waitRead(ctx, editReadWait); err != nil {
				return err
			}
			writes++
			count := 0
			if len(qs) > 0 {
				count = len(links)
			}
			err := editor.SetPendingQuestions(ctx, chat, id, channel.PendingQuestions{Count: count, Items: qs})
			if errors.Is(err, channel.ErrQuestionDecorationTooLarge) && len(directoryRows) == 0 {
				// A large group is published one question at a time. Wait until every
				// message has its ID before generating one complete directory.
				for _, q := range questions {
					if q.MessageID == "" {
						if err := e.waitRead(ctx, editReadWait); err != nil {
							return err
						}
						return editor.SetPendingQuestions(ctx, chat, id, channel.PendingQuestions{Count: len(links), Items: []channel.PendingQuestion{{ID: "Questions are being delivered"}}})
					}
				}

				if err := e.queueQuestionDirectory(ctx, chat, conv, directoryKey, questions); err != nil {
					return err
				}
				return errQuestionDirectoryQueued
			}
			if len(qs) == 0 && (errors.Is(err, channel.ErrNotFound) || errors.Is(err, channel.ErrNotEditable)) {
				return nil
			}
			return err
		}

		save := func(current, previous string) error {
			return e.update(func(tx *store.Tx) error {
				for key, value := range map[string]string{questionMarkerKey(conv): current, questionMarkerPreviousKey(conv): previous} {
					var err error
					if value == "" {
						err = tx.DeleteMeta(key)
					} else {
						err = tx.SetMeta(key, value)
					}
					if err != nil {
						return err
					}
				}
				return nil
			})
		}

		// A newer message may have arrived after an interrupted handoff. Retire
		// the superseded staged target while retaining its predecessor for recovery.
		if previous != "" && current != desired {
			if err := apply(current, nil); err != nil {
				return err
			}
			if err := save(previous, ""); err != nil {
				return err
			}
			current, previous = previous, ""
		}
		// Finish a prior interrupted handoff before staging another one.
		if previous != "" {
			if current != "" {
				if err := apply(current, questions); err != nil {
					return err
				}
			}
			if err := apply(previous, nil); err != nil {
				return err
			}
			if err := save(current, ""); err != nil {
				return err
			}
		}
		if current == desired {
			if desired == "" {
				return nil
			}
			return apply(desired, questions)
		}
		if desired != "" {
			if err := save(desired, current); err != nil {
				return err
			}
			if err := apply(desired, questions); err != nil {
				return err
			}
		}
		if current != "" {
			if err := apply(current, nil); err != nil {
				return err
			}
		}
		if current == "" {
			return nil
		} // the first target was persisted before editing
		return save(desired, "")
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
		if err != nil {
			return err
		}
		previous, err := tx.MetaPrefix(questionMarkerPreviousMeta)
		if markers == nil {
			markers = map[string]string{}
		}
		for key, id := range previous {
			markers[questionMarkerMeta+strings.TrimPrefix(key, questionMarkerPreviousMeta)] = id
		}
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

var errQuestionDirectoryQueued = errors.New("pending question directory queued")

func questionDirectoryKey(questions []channel.PendingQuestion) string {
	body, _ := json.Marshal(questions)
	return fmt.Sprintf("question-directory:%x", sha256.Sum256(body))
}

func (e *Engine) queueQuestionDirectory(ctx context.Context, chat string, conv int64, key string, questions []channel.PendingQuestion) error {
	parts, err := e.ch.(channel.QuestionDecorator).RenderQuestionDirectory(ctx, chat, questions)
	if err != nil {
		return err
	}
	msgs := make([]store.OutMsg, 0, len(parts))
	for _, p := range parts {
		msgs = append(msgs, store.OutMsg{DeliveryKey: fmt.Sprintf("%s:%d", key, p.Index), Kind: store.OutNotice, Part: p.Index, Parts: p.Count, Body: p.Body, BodyHash: hashBytes(p.Hash)})
	}
	if err := e.update(func(tx *store.Tx) error { _, err := tx.Enqueue(conv, msgs); return err }); err != nil {
		return err
	}
	e.wakeSender()
	return nil
}

func pendingQuestionItems(links []store.QuestionLink) []channel.PendingQuestion {
	items := make([]channel.PendingQuestion, 0, len(links))
	for _, q := range links {
		items = append(items, channel.PendingQuestion{ID: fmt.Sprintf("Q%d", q.ID), Title: q.Title, MessageID: q.MessageID})
	}
	return items
}
