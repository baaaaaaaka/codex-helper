package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

const questionReplyStart = "<send_user_message_question_reply>"
const questionReplyEnd = "</send_user_message_question_reply>"

func questionPrefix(id int64) string { return fmt.Sprintf("question:%d:body", id) }

func questionDoc(g store.QuestionGroup, q store.Question) channel.RichDoc {
	label := fmt.Sprintf("❓ Q%d · Question from Codex", q.ID)
	status := "⏸️ Codex is waiting for your answer."
	if g.Async {
		status = "▶️ Reply when ready. Codex does not wait for your answer."
	} else if !g.Blocking {
		status = "⏳ Reply requested. Unanswered questions are skipped after 2 minutes."
		if g.Deadline == 0 {
			status = "⏳ Reply requested. Your other answers are recorded."
		}
	}
	done := ""
	switch {
	case g.State == "expired":
		done = "Expired"
	case g.State == "uncertain":
		done = "Delivery unconfirmed"
	case q.Skipped:
		done = "Skipped"
	case g.State == "submitting":
		done = "Checking delivery"
	case q.Submitted:
		done = "Answer submitted"
	case g.State == "resolved":
		done = "Resolved"
	case q.Answer != nil:
		done = "Answer recorded"
	}
	if done != "" {
		text := q.Title + "\n\n" + done
		if q.Answer != nil && !q.Skipped {
			caption := "Answer"
			if !q.Submitted {
				caption = "Recorded answer"
			}
			text += "\n\n**" + caption + ":** " + limitRunes(*q.Answer, 256)
		}
		if done == "Answer recorded" && !g.Async {
			remaining := 0
			for _, p := range g.Questions {
				if p.Answer == nil && !p.Skipped {
					remaining++
				}
			}
			text = q.Title + "\n\n" + fmt.Sprintf("Answer recorded · %d question(s) remaining", remaining) + "\n\n**Answer:** " + limitRunes(*q.Answer, 256)
		}
		return channel.RichDoc{Label: fmt.Sprintf("✓ Q%d · %s", q.ID, done), Markdown: text, Math: true}
	}
	var body strings.Builder
	body.WriteString(status + "\n\n" + q.Title)
	for i, o := range q.Options {
		fmt.Fprintf(&body, "\n\n**%s. %s**", optionLetter(i), o.Label)
		if o.Description != "" {
			body.WriteString("\n" + o.Description)
		}
	}
	if q.Secret {
		body.WriteString("\n\n🔒 Secret input is not supported in Teams. Use a supported secure input method; do not post secrets here.")
	} else {
		body.WriteString("\n\n↩️ **Answer:** Quote this message and reply with ")
		if len(q.Options) > 0 {
			var labels []string
			for i := range q.Options {
				labels = append(labels, "**"+optionLetter(i)+"**")
			}
			body.WriteString(strings.Join(labels, ", ") + ", or your own answer.")
		} else {
			body.WriteString("your answer.")
		}
		fmt.Fprintf(&body, "\nOr use `helper answer Q%d <answer>`.", q.ID)
	}
	fmt.Fprintf(&body, "\n\n⏭️ **Skip question:** `helper answer Q%d --skip`", q.ID)
	return channel.RichDoc{Label: label, Markdown: body.String(), Math: true}
}

func optionLetter(i int) string {
	if i < 26 {
		return string(rune('A' + i))
	}
	return "A" + string(rune('A'+i-26))
}

func (e *Engine) questionEvent(ev QuestionEvent) error {
	e.questionMu.Lock()
	defer e.questionMu.Unlock()
	if ev.Group.Key != "" {
		g := ev.Group
		e.mu.Lock()
		conv := e.byThread[g.ThreadID]
		e.mu.Unlock()
		if conv == 0 {
			return errors.New("this thread is not bound to a Teams chat")
		}
		g.ConvID = conv
		if g.Async {
			g.Deadline = ms(e.now().Add(7 * 24 * time.Hour))
		} else if !g.Blocking {
			g.Deadline = ms(e.now().Add(2 * time.Minute))
		}
		var stored store.QuestionGroup
		err := e.update(func(tx *store.Tx) error {
			if _, err := tx.AddQuestionGroup(g); err != nil {
				return err
			}
			var err error
			stored, _, err = tx.QuestionGroup(g.Key)
			return err
		})
		if err != nil {
			return err
		}
		e.queueQuestionPublish(stored)
		e.refreshQuestionWaiting()
		return nil
	}
	var changed []store.QuestionGroup
	err := e.update(func(tx *store.Tx) error {
		var groups []store.QuestionGroup
		var err error
		if ev.Resolved != "" {
			var g store.QuestionGroup
			var ok bool
			g, ok, err = tx.QuestionGroup(ev.Resolved)
			if ok && (g.State == "open" || g.State == "submitting") {
				groups = append(groups, g)
			}
		} else if ev.ThreadID != "" {
			groups, err = tx.ThreadQuestionGroups(ev.ThreadID)
		}
		if err != nil {
			return err
		}
		for _, g := range groups {
			state := ""
			if ev.Resolved == g.Key {
				state = "resolved"
			}
			if ev.Finished && ev.ThreadID == g.ThreadID && (ev.TurnID == "" || g.TurnID == ev.TurnID) && (!g.Async || ev.Interrupted) {
				state = "expired"
			}
			if len(ev.Answered) > 0 && ev.ThreadID == g.ThreadID && g.Async {
				matched := false
				for _, q := range g.Questions {
					for _, id := range ev.Answered {
						if q.NativeID == id {
							matched = true
							if err := tx.MarkQuestionSubmitted(q.ID); err != nil {
								return err
							}
						}
						if q.NativeID == id && q.Answer == nil {
							if err := tx.RecordQuestionAnswer(q.ID, 0, "Answered in another Codex client", false); err != nil {
								return err
							}
						}
					}
				}
				if !matched {
					continue
				}
				g, _, err = tx.QuestionGroup(g.Key)
				if err != nil {
					return err
				}
				if questionsDelivered(g) {
					state = "resolved"
				}
				changed = append(changed, g)
			}
			if state != "" {
				if err := tx.SetQuestionState(g.Key, state); err != nil {
					return err
				}
				g.State = state
				changed = append(changed, g)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, g := range changed {
		e.editQuestions(g)
	}
	e.refreshQuestionWaiting()
	return nil
}

// Rendering and network edits stay outside write transactions. Enqueue is
// idempotent, so a crash after registration is repaired by maintenance.
// Use the existing bounded sender workers: math rendering must not stall the
// native app-server reader or delay steer/response notifications.
func (e *Engine) queueQuestionPublish(g store.QuestionGroup) {
	if g.State != "open" {
		return
	}
	missing := false
	for _, q := range g.Questions {
		yes, err := e.st.OutboxHasKeyPrefix(questionPrefix(q.ID) + ":")
		if err != nil {
			e.logf("check question publication: %v", err)
			return
		}
		if !yes {
			missing = true
			break
		}
	}
	if !missing {
		return
	}
	e.queueEdit(g.ConvID, chatEdit{key: "question-publish:" + g.Key, what: "question publication", run: func(ctx context.Context, _ string) error {
		var current store.QuestionGroup
		if err := e.update(func(tx *store.Tx) (err error) { current, _, err = tx.QuestionGroup(g.Key); return }); err != nil {
			return err
		}
		return e.publishQuestions(ctx, current)
	}})
	e.wakeSender()
}

func (e *Engine) publishQuestions(ctx context.Context, g store.QuestionGroup) error {
	if g.State != "open" {
		return nil
	}
	var posts []store.OutMsg
	for _, q := range g.Questions {
		// Recovery only renders missing messages. Repeated maintenance must
		// not repeat math rendering or allocate the same question bodies.
		published, err := e.st.OutboxHasKeyPrefix(questionPrefix(q.ID) + ":")
		if err != nil {
			return err
		}
		if published {
			continue
		}
		msgs, err := e.render(ctx, questionDoc(g, q), store.OutNotice, questionPrefix(q.ID))
		if err != nil {
			return err
		}
		posts = append(posts, msgs...)
	}
	if len(posts) == 0 {
		return nil
	}
	var current store.QuestionGroup
	var added int
	err := e.update(func(tx *store.Tx) error {
		var err error
		current, _, err = tx.QuestionGroup(g.Key)
		if err != nil || current.State != "open" {
			return err // resolved while rendering: do not publish stale actions
		}
		n, err := tx.Enqueue(g.ConvID, posts)
		if err != nil {
			return err
		}
		if n > 0 {
			added = n
			return e.addNotify(tx, pendingNotify{Conv: g.ConvID, Key: "questions:" + g.Key, Kind: notifyQuestion, Text: g.Key})
		}
		return nil
	})
	if err == nil {
		if added > 0 && hasQuestionAnswer(current) {
			e.editQuestions(current) // a draft changed while its first body rendered
		}
		e.wakeSender()
		wake(e.notifyWake)
	}
	return err
}

func questionsReady(g store.QuestionGroup) bool {
	for _, q := range g.Questions {
		if q.Answer == nil && !q.Skipped {
			return false
		}
	}
	return true
}

func questionsDelivered(g store.QuestionGroup) bool {
	for _, q := range g.Questions {
		if !q.Submitted && !q.Skipped {
			return false
		}
	}
	return true
}

func (e *Engine) editQuestions(g store.QuestionGroup) {
	if !e.ch.Caps().EditMessages {
		return
	}
	for _, question := range g.Questions {
		qid, key, conv := question.ID, g.Key, g.ConvID
		published, err := e.st.OutboxHasKeyPrefix(questionPrefix(qid) + ":")
		if err != nil || !published {
			continue // publication checks the latest state before enqueueing
		}
		e.queueEdit(conv, chatEdit{key: fmt.Sprintf("question:%d", qid), what: "question status", run: func(ctx context.Context, chat string) error {
			var current store.QuestionGroup
			if err := e.update(func(tx *store.Tx) (err error) { current, _, err = tx.QuestionGroup(key); return }); err != nil {
				return err
			}
			if current.Key == "" {
				return nil
			}
			var q store.Question
			for _, v := range current.Questions {
				if v.ID == qid {
					q = v
				}
			}
			rows, err := e.st.OutboxByKeyPrefix(questionPrefix(qid) + ":")
			if err != nil {
				return err
			}
			if len(rows) == 0 {
				return errEditLater
			}
			for _, row := range rows {
				if row.State == store.OutDropped {
					return nil
				}
				if row.State != store.OutSent || row.ExtMsgID == "" {
					return errEditLater
				}
			}
			posts, err := e.render(ctx, questionDoc(current, q), store.OutNotice, "question-edit")
			if err != nil {
				return err
			}
			for i, row := range rows {
				if i > 0 {
					if err := e.waitWrite(ctx); err != nil {
						return err
					}
				}
				p := posts[0]
				if i < len(posts) {
					p = posts[i]
				}
				if err := e.ch.Edit(ctx, chat, row.ExtMsgID, channel.Part{Body: p.Body, Size: len(p.Body)}, row.AttemptToken); err != nil {
					return err
				}
			}
			return nil
		}})
	}
	e.wakeSender()
}

func (e *Engine) refreshQuestionWaiting() {
	var convs []int64
	_ = e.update(func(tx *store.Tx) (err error) { convs, err = tx.WaitingQuestionConversations(); return })
	waiting := map[int64]bool{}
	for _, conv := range convs {
		waiting[conv] = true
	}
	e.mu.Lock()
	e.questionWaiting = waiting
	e.mu.Unlock()
}

func (e *Engine) cmdAnswer(ctx context.Context, j cmdJob, arg string) {
	fields := strings.Fields(arg)
	if len(fields) < 2 || !strings.HasPrefix(strings.ToUpper(fields[0]), "Q") {
		e.reply(j, "Usage: `helper answer Q<number> <answer>` or `helper answer Q<number> --skip`.", nil)
		return
	}
	id, err := strconv.ParseInt(fields[0][1:], 10, 64)
	if err != nil || id <= 0 {
		e.reply(j, "Invalid question number.", nil)
		return
	}
	answer := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(arg), fields[0]))
	skip := answer == "--skip"
	if strings.HasPrefix(answer, "-- ") {
		answer = strings.TrimPrefix(answer, "-- ")
	}
	e.questionMu.Lock()
	var g store.QuestionGroup
	var sending bool
	var problem string
	err = e.update(func(tx *store.Tx) error {
		var ok bool
		var err error
		g, ok, err = tx.QuestionByID(j.conv, id)
		if err != nil {
			return err
		}
		if !ok {
			problem = "Question not found in this chat."
			return nil
		}
		if g.State != "open" {
			problem = "This question is already resolved, expired, or being submitted."
			return nil
		}
		c, _, err := tx.Conversation(j.conv)
		if err != nil {
			return err
		}
		if c.State != store.ConvActive && c.State != store.ConvParked {
			problem = "This chat is not active. The answer was not sent."
			return nil
		}
		th, _, err := tx.Thread(j.conv)
		if err != nil {
			return err
		}
		if th.ThreadID != g.ThreadID {
			problem = "This question belongs to a different Codex session."
			return tx.SetQuestionState(g.Key, "expired")
		}
		for _, q := range g.Questions {
			if q.ID == id {
				if q.InboundID == j.inbound {
					return tx.SetInbound(j.inbound, store.InDone, 0)
				}
				if g.Async && (q.Answer != nil || q.Skipped) {
					problem = "This question has already been answered or skipped."
					return nil
				}
				if q.Secret && !skip {
					problem = "Secret input is not supported in Teams. Do not post secrets here."
					return nil
				}
				if !skip {
					for i, o := range q.Options {
						if answer == optionLetter(i) {
							answer = o.Label
							break
						}
					}
				}
				if err := tx.RecordQuestionAnswer(id, j.inbound, answer, skip); err != nil {
					return err
				}
				if !g.Async && !g.Blocking {
					if _, err := tx.SetQuestionDeadline(g.Key, 0); err != nil {
						return err
					}
				}
				if g.Async && !skip {
					raw, _ := json.Marshal([]map[string]string{{"questionItemId": q.NativeID, "question": q.Title, "answer": answer}})
					if err := tx.QueueQuestionReply(j.conv, j.inbound, questionReplyStart+"\n"+string(raw)+"\n"+questionReplyEnd); err != nil {
						return err
					}
				} else if err := tx.SetInbound(j.inbound, store.InDone, 0); err != nil {
					return err
				}
			}
		}
		g, _, err = tx.QuestionGroup(g.Key)
		if err != nil {
			return err
		}
		if questionsReady(g) {
			state := "submitting"
			if g.Async {
				if !questionsDelivered(g) {
					return nil
				}
				state = "resolved"
			} else {
				sending = true
			}
			if err := tx.SetQuestionState(g.Key, state); err != nil {
				return err
			}
			g.State = state
		}
		return nil
	})
	e.questionMu.Unlock()
	if err != nil {
		e.logf("answer question: %v", err)
		return
	}
	if problem != "" {
		if g.Key != "" {
			e.editQuestions(g)
			e.refreshQuestionWaiting()
		}
		e.reply(j, problem, nil)
		return
	}
	e.editQuestions(g)
	e.refreshQuestionWaiting()
	wake(e.notifyWake)
	if g.Async {
		e.wakeRunner(j.conv)
		return
	}
	if sending {
		e.submitQuestionGroup(ctx, g)
	}
}

func (e *Engine) submitQuestionGroup(ctx context.Context, g store.QuestionGroup) {
	qa, ok := e.ag.(QuestionAgent)
	if !ok {
		return
	}
	answers := map[string][]string{}
	for _, q := range g.Questions {
		if q.Answer != nil && !q.Skipped {
			answers[q.NativeID] = []string{*q.Answer}
		}
	}
	err := qa.AnswerQuestions(ctx, g.Key, answers)
	state := "resolved"
	if errors.Is(err, ErrQuestionExpired) {
		state = "expired"
	} else if err != nil {
		state = "uncertain"
	}
	_ = e.update(func(tx *store.Tx) error {
		if err == nil {
			for i := range g.Questions {
				q := &g.Questions[i]
				if q.Answer != nil && !q.Skipped {
					q.Submitted = true
					if x := tx.MarkQuestionSubmitted(q.ID); x != nil {
						return x
					}
				}
			}
		}
		return tx.SetQuestionState(g.Key, state)
	})
	g.State = state
	e.editQuestions(g)
	e.refreshQuestionWaiting()
}

func (e *Engine) maintainQuestions(ctx context.Context) {
	qa, ok := e.ag.(QuestionAgent)
	if !ok {
		return
	}
	var groups []store.QuestionGroup
	if err := e.update(func(tx *store.Tx) (err error) { groups, err = tx.OpenQuestionGroups(); return }); err != nil {
		return
	}
	for _, g := range groups {
		state := ""
		if g.State == "submitting" {
			if !qa.QuestionPending(g.Key) {
				_ = e.update(func(tx *store.Tx) error {
					_, err := tx.TransitionQuestion(g.Key, "submitting", "uncertain")
					return err
				})
				g.State = "uncertain"
				e.editQuestions(g)
			}
			continue
		} // uncertain writes are never replayed
		if !g.Async && !qa.QuestionPending(g.Key) {
			state = "expired"
		}
		e.mu.Lock()
		cs := e.convs[g.ConvID]
		if cs == nil || cs.threadID != g.ThreadID || (cs.state != store.ConvActive && cs.state != store.ConvParked) {
			state = "expired"
		}
		e.mu.Unlock()
		anyAnswer := false
		for _, q := range g.Questions {
			anyAnswer = anyAnswer || q.Answer != nil || q.Skipped
		}
		if g.Deadline > 0 && ms(e.now()) >= g.Deadline {
			if g.Async {
				state = "expired"
			} else if !anyAnswer && state == "" {
				claimed := false
				_ = e.update(func(tx *store.Tx) error {
					cur, ok, err := tx.QuestionGroup(g.Key)
					if err != nil || !ok {
						return err
					}
					if cur.State == "open" && !hasQuestionAnswer(cur) {
						claimed = true
						return tx.SetQuestionState(g.Key, "submitting")
					}
					return nil
				})
				if claimed {
					e.submitQuestionGroup(ctx, g)
				}
				continue
			}
		}
		if state != "" {
			_ = e.update(func(tx *store.Tx) error { _, err := tx.TransitionQuestion(g.Key, "open", state); return err })
			g.State = state
			e.editQuestions(g)
		} else {
			e.queueQuestionPublish(g)
		}
	}
	_ = e.update(func(tx *store.Tx) error { return tx.PruneQuestions(ms(e.now().Add(-30 * 24 * time.Hour))) })
	e.refreshQuestionWaiting()
}

func (e *Engine) rejectQuestionReply(tx *store.Tx, tr store.Turn) error {
	text := "This question is no longer available in this Codex session. The answer was not sent."
	msgs, err := e.notice(fmt.Sprintf("turn:%d:question-expired", tr.ID), text)
	if err != nil {
		return err
	}
	return tx.FinishTurn(tr.ID, store.TurnNotStarted, text, msgs)
}

func hasQuestionAnswer(g store.QuestionGroup) bool {
	for _, q := range g.Questions {
		if q.Answer != nil || q.Skipped {
			return true
		}
	}
	return false
}

func questionNotification(g store.QuestionGroup, title string) (string, string) {
	heading := "❓ Codex has a question"
	if len(g.Questions) > 1 {
		heading = fmt.Sprintf("❓ Codex has %d questions", len(g.Questions))
	}
	status := "Waiting for your answer"
	if g.Async {
		status = "Reply when ready"
	} else if !g.Blocking {
		status = "Reply requested"
	}
	return heading, title + " · " + status
}

func (e *Engine) questionNotice(g store.QuestionGroup, title string) (heading, text string, err error) {
	heading, text = questionNotification(g, title)
	err = e.update(func(tx *store.Tx) error {
		for _, q := range g.Questions {
			gone, err := tx.DroppedUnder(questionPrefix(q.ID) + ":")
			if err != nil {
				return err
			}
			if gone {
				heading = "⚠️ Codex question delivery unconfirmed"
				text = title + "\n\nOpen the chat to check the question, or answer in Codex TUI."
				break
			}
		}
		return nil
	})
	return
}

func (e *Engine) questionMention(n pendingNotify, cs *convState) ([]store.OutMsg, error) {
	var g store.QuestionGroup
	if err := e.update(func(tx *store.Tx) (err error) { g, _, err = tx.QuestionGroup(n.Text); return }); err != nil {
		return nil, err
	}
	if g.State != "open" || questionsReady(g) {
		return nil, nil
	}
	e.mu.Lock()
	title, url := firstNonEmpty(cs.title, cs.label), cs.url
	e.mu.Unlock()
	heading, text, err := e.questionNotice(g, title)
	if err != nil {
		return nil, err
	}
	if url != "" {
		text += "\n\n[Open chat](" + url + ")"
	}
	return e.render(context.Background(), channel.RichDoc{Label: heading, Markdown: text, Mention: true}, store.OutNotice, "notify:question:"+g.Key)
}
