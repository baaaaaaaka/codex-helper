package turns

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

const pendingQuestionMarker = "❓Pending agent question"

func hasPendingQuestionMarker(m fakegraph.Message) bool {
	return strings.HasPrefix(strings.TrimSpace(m.PlainText()), pendingQuestionMarker+"\n")
}

func scriptedQuestions(async bool, n int) *fakecodex.QuestionScript {
	qs := &fakecodex.QuestionScript{Async: async, Blocking: true}
	for i := 0; i < n; i++ {
		qs.Questions = append(qs.Questions, fakecodex.QuestionSpec{ID: fmt.Sprintf("environment-%d", i), Question: fmt.Sprintf("选择测试环境？ (%d)", i), Options: []fakecodex.QuestionOption{{Label: "STAGING", Description: "Safe test data"}, {Label: "PRODUCTION", Description: "Live data"}}})
	}
	return qs
}

func TestQuestionsSyncQuoteMultiAndNotification(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "QUESTIONS-SYNC", harness.Options{Golden: harness.GoldenOptions{Skip: "Question messages change through asynchronous edits; assertions verify delivery, content, native replies, and state transitions directly."}, Title: "Native RPC questions: quote answer, multi-question aggregation, one owner notification, no new turn", Budget: 100 * time.Second})
	defer s.Finish()
	token := s.Token("question")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(token) {
			return harness.CodexBehavior{Questions: scriptedQuestions(false, 2)}
		}
		return harness.CodexBehavior{}
	})
	ctl := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, ctl, "questions-sync")
	steps.MustPost(s, work.ID, "Ask questions "+token)
	s.Require("questions visible before turn completion", steps.AnswerTimeout, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, "Question from Codex")) == 2, "waiting for two question messages"
	})
	s.Require("only the latest agent question carries the marker", steps.CommandTimeout, func() (bool, string) {
		latest := s.SUTMessagesContaining(work.ID, "Question from Codex")
		if len(latest) < 2 {
			return false, fmt.Sprintf("%d question messages", len(latest))
		}
		marked := 0
		markedID := ""
		for _, m := range s.SUTMessages(work.ID) {
			if hasPendingQuestionMarker(m) {
				marked++
				markedID = m.ID
			}
		}
		latestID := latest[len(latest)-1].ID
		return marked == 1 && markedID == latestID, fmt.Sprintf("%d marked message(s), latest=%s marked=%s", marked, latestID, markedID)
	})
	questions := s.SUTMessagesContaining(work.ID, "Question from Codex")
	s.Require("owner notified once for the group", steps.AnswerTimeout, func() (bool, string) {
		return len(s.SUTMessagesContaining(ctl.ID, "Codex has 2 questions")) == 1, "waiting for owner mention"
	})
	s.Require("question marks its work chat unread once", steps.AnswerTimeout, func() (bool, string) {
		requests := s.Graph.Requests(fakegraph.RequestFilter{Method: "POST", Route: "POST /chats/{chat}/markChatUnreadForUser", ChatID: work.ID, OnlyOK: true})
		return len(requests) == 1, fmt.Sprintf("%d unread marks", len(requests))
	})
	_, err := s.Graph.PostWithQuotesAsUser(work.ID, "B", fakegraph.QuoteSpec{MessageID: questions[1].ID})
	if err != nil {
		t.Fatal(err)
	}
	s.Require("partial answer recorded without finishing", steps.CommandTimeout, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, "Answer recorded")) > 0, "waiting for answer record"
	})
	s.Require("status edit keeps marker on the latest pending question", steps.CommandTimeout, func() (bool, string) {
		updated := messageByID(s.Graph.Messages(work.ID), questions[1].ID)
		return hasPendingQuestionMarker(updated) && strings.Contains(updated.PlainText(), "Answer recorded"), "Q2: " + strings.Join(strings.Fields(updated.PlainText()), " ")
	})
	steps.MustPost(s, work.ID, "helper answer Q1 --skip")
	s.Require("answer reaches original RPC", steps.AnswerTimeout, func() (bool, string) {
		msgs := s.SUTMessagesContaining(work.ID, "QUESTION_RESULT")
		return len(msgs) == 1 && strings.Contains(msgs[0].PlainText(), "PRODUCTION"), "waiting for original tool response"
	})
	s.Require("resolved question clears the marker", steps.AnswerTimeout, func() (bool, string) {
		for _, m := range s.SUTMessages(work.ID) {
			if hasPendingQuestionMarker(m) {
				return false, "marker still present on " + m.ID
			}
		}
		return true, "no marked message"
	})
	calls := s.Codex.CallsWithToken(token)
	s.Check("one execution for the whole interaction", len(calls) == 1, "one native turn", fmt.Sprint(len(calls)))
	steps.MustPost(s, work.ID, "helper answer Q1 A")
	s.Require("late answer cannot reopen the request", steps.CommandTimeout, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, "already resolved")) > 0, "waiting for closed-question response"
	})
	s.Check("only one question notification", len(s.SUTMessagesContaining(ctl.ID, "Codex has 2 questions")) == 1, "one group notification", "notification count checked")
	marks := s.Graph.Requests(fakegraph.RequestFilter{Method: "POST", Route: "POST /chats/{chat}/markChatUnreadForUser", ChatID: work.ID, OnlyOK: true})
	var firstMark struct {
		ReadAt string `json:"lastMessageReadDateTime"`
	}
	if len(marks) > 0 {
		_ = json.Unmarshal([]byte(marks[0].Body), &firstMark)
	}
	s.Check("question unread mark is not repeated", len(marks) == 2 && firstMark.ReadAt == "", "one zero-threshold question mark plus final-answer mark", fmt.Sprintf("%d unread request(s), first readAt %q", len(marks), firstMark.ReadAt))
}

func TestAsyncQuestionMarkerMovesToNextAgentMessage(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "QUESTIONS-MARKER-MOVE", harness.Options{Golden: harness.GoldenOptions{Skip: "The marker is an in-place message edit; explicit assertions verify the single current target."}, Title: "An async question marker moves from the question to the next agent answer", Budget: 90 * time.Second})
	defer s.Finish()
	token := s.Token("question")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(token) {
			return harness.CodexBehavior{Questions: scriptedQuestions(true, 1), Answer: "continued after question", Delay: 2 * time.Second}
		}
		return harness.CodexBehavior{}
	})
	ctl := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, ctl, "question-marker-move")
	steps.MustPost(s, work.ID, "Ask "+token)
	s.Require("question initially carries the marker", steps.AnswerTimeout, func() (bool, string) {
		questions := s.SUTMessagesContaining(work.ID, "Question from Codex")
		return len(questions) == 1 && hasPendingQuestionMarker(questions[0]), "waiting for marked question"
	})
	s.Require("Codex sends its next agent message", steps.AnswerTimeout, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, "continued after question")) == 1, "waiting for the final answer"
	})
	s.Require("only the latest agent message remains marked", steps.CommandTimeout, func() (bool, string) {
		questions := s.SUTMessagesContaining(work.ID, "Question from Codex")
		answers := s.SUTMessagesContaining(work.ID, "continued after question")
		var marked []fakegraph.Message
		for _, m := range s.SUTMessages(work.ID) {
			if hasPendingQuestionMarker(m) {
				marked = append(marked, m)
			}
		}
		questionMarked, answerMarked := false, false
		if len(questions) > 0 {
			questionMarked = hasPendingQuestionMarker(questions[0])
		}
		if len(answers) > 0 {
			answerMarked = hasPendingQuestionMarker(answers[0])
		}
		ok := len(questions) == 1 && len(answers) == 1 && len(marked) == 1 &&
			marked[0].ID == answers[0].ID && !questionMarked
		return ok, fmt.Sprintf("%d marked message(s), question=%v answer=%v", len(marked), questionMarked, answerMarked)
	})
}

func messageByID(messages []fakegraph.Message, id string) fakegraph.Message {
	for _, m := range messages {
		if m.ID == id {
			return m
		}
	}
	return fakegraph.Message{}
}

func TestQuestionsAsyncSteerQueueAndLateReply(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "QUESTIONS-ASYNC", harness.Options{Golden: harness.GoldenOptions{Skip: "Question messages change through asynchronous edits; assertions verify delivery, content, native replies, and state transitions directly."}, Title: "Async question is immediate, native reply steers, queue stays explicit, and late replies start a follow-up", Budget: 140 * time.Second})
	defer s.Finish()
	first, second, queued := s.Token("first"), s.Token("second"), s.Token("queued")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(first) {
			return harness.CodexBehavior{Questions: scriptedQuestions(true, 1), Block: true}
		}
		if c.HasToken(second) {
			return harness.CodexBehavior{Questions: scriptedQuestions(true, 1)}
		}
		return harness.CodexBehavior{}
	})
	ctl := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, ctl, "questions-async")
	steps.MustPost(s, work.ID, "Start "+first)
	s.Require("async question shown while Codex runs", steps.AnswerTimeout, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, "Question from Codex")) == 1, "waiting for question"
	})
	call := s.Codex.CallsWithToken(first)[0]
	tui := attachTUI(s, call.ThreadID)
	steps.MustPost(s, work.ID, "helper queue "+queued)
	steps.MustPost(s, work.ID, "helper answer Q1 B")
	s.Require("native question reply steers without a Teams envelope", steps.AnswerTimeout, func() (bool, string) {
		raw := tui.call("thread/turns/list", map[string]any{"threadId": call.ThreadID, "limit": 5, "sortDirection": "desc"})
		return strings.Contains(string(raw), "questionItemId") && strings.Contains(string(raw), "PRODUCTION"), "waiting for native reply input"
	})
	s.Check("queued task has not started", len(s.Codex.CallsWithToken(queued)) == 0, "zero queued executions", "checked while first task active")
	s.Codex.Release(call.Seq)
	steps.WaitAnswered(s, work.ID, queued, steps.AnswerTimeout)
	steps.MustPost(s, work.ID, "Next "+second)
	steps.WaitAnswered(s, work.ID, second, steps.AnswerTimeout)
	steps.MustPost(s, work.ID, "helper answer Q2 A")
	s.Require("late answer becomes native follow-up", steps.AnswerTimeout, func() (bool, string) {
		for _, c := range s.Codex.Calls() {
			if strings.HasPrefix(strings.TrimSpace(c.Prompt), "<send_user_message_question_reply>") && strings.Contains(c.Prompt, "STAGING") {
				return true, ""
			}
		}
		return false, "waiting for late-answer execution"
	})
	s.Check("async question is not emitted as a final answer", len(s.SUTMessagesContaining(work.ID, "ASYNC_QUESTION_TEXT")) == 0, "no duplicate async final", "checked")
}

func TestQuestionsSharedTUIAnswerWins(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "QUESTIONS-TUI", harness.Options{Golden: harness.GoldenOptions{Skip: "Question messages change through asynchronous edits; assertions verify delivery, content, native replies, and state transitions directly."}, Title: "An attached TUI can answer a Teams-visible RPC; replay and Teams late reply do not resubmit", Budget: 100 * time.Second})
	defer s.Finish()
	token := s.Token("question")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(token) {
			return harness.CodexBehavior{Questions: scriptedQuestions(false, 1)}
		}
		return harness.CodexBehavior{}
	})
	ctl := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, ctl, "questions-tui")
	steps.MustPost(s, work.ID, "Ask "+token)
	s.Require("question posted", steps.AnswerTimeout, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, "Question from Codex")) == 1, "waiting"
	})
	call := s.Codex.CallsWithToken(token)[0]
	tui := attachTUI(s, call.ThreadID)
	// Resuming the native connection replays the pending request. A second
	// resume forces a replay after attachTUI's call loop has drained responses.
	_ = tui.c.WriteJSON(map[string]any{"id": 999, "method": "thread/resume", "params": map[string]any{"threadId": call.ThreadID, "excludeTurns": true}})
	_ = tui.c.SetReadDeadline(time.Now().Add(10 * time.Second))
	var requestID json.RawMessage
	for len(requestID) == 0 {
		_, raw, err := tui.c.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		var m struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.Unmarshal(raw, &m)
		if m.Method == "item/tool/requestUserInput" {
			requestID = m.ID
		}
	}
	if err := tui.c.WriteJSON(map[string]any{"id": requestID, "result": map[string]any{"answers": map[string]any{"environment-0": map[string]any{"answers": []string{"TUI_CHOICE"}}}}}); err != nil {
		t.Fatal(err)
	}
	s.Require("native answer resolves the shared request", steps.AnswerTimeout, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, "TUI_CHOICE")) > 0, "waiting for answer"
	})
	steps.MustPost(s, work.ID, "helper answer Q1 B")
	s.Require("Teams sees request already handled", steps.CommandTimeout, func() (bool, string) { return len(s.SUTMessagesContaining(work.ID, "already resolved")) > 0, "waiting" })
	s.Check("one question after replay", len(s.SUTMessagesContaining(work.ID, "Question from Codex")) == 0, "original question updated to resolved", "no reopened question")
}
