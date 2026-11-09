package turns

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

func TestQuestionsCancelExpiresRPCAndNextTaskWorks(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "QUESTIONS-CANCEL", harness.Options{Golden: harness.GoldenOptions{Skip: "Question edits are asynchronous; explicit assertions check the native protocol, routing, and terminal state."}, Title: "Cancel resolves a waiting question without making a new task from a late answer", Budget: 120 * time.Second})
	defer s.Finish()
	runQuestionCancellation(s, false)
}

func TestQuestionsAsyncCancelRejectsLateAnswer(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "QUESTIONS-ASYNC-CANCEL", harness.Options{Golden: harness.GoldenOptions{Skip: "Question edits are asynchronous; explicit assertions check the native protocol, routing, and terminal state."}, Title: "Cancel expires an async question without making a new task from a late answer", Budget: 120 * time.Second})
	defer s.Finish()
	runQuestionCancellation(s, true)
}

func runQuestionCancellation(s *harness.Scenario, async bool) {
	ask, next := s.Token("ask"), s.Token("next")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(ask) {
			return harness.CodexBehavior{Questions: scriptedQuestions(async, 1), Block: async}
		}
		return harness.CodexBehavior{}
	})
	ctl := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, ctl, "questions-cancel")
	steps.MustPost(s, work.ID, "Ask "+ask)
	s.Require("waiting question visible", steps.AnswerTimeout, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, "Question from Codex")) == 1, "waiting for RPC question"
	})
	steps.MustPost(s, work.ID, "helper cancel running")
	s.Require("question closed on cancel", steps.AnswerTimeout, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, "Resolved"))+len(s.SUTMessagesContaining(work.ID, "Expired")) > 0, "waiting for question closure"
	})
	steps.MustPost(s, work.ID, "helper answer Q1 A")
	s.Require("late reply refused", steps.CommandTimeout, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, "already resolved")) > 0, "waiting for refusal"
	})
	steps.MustPost(s, work.ID, "Next "+next)
	steps.WaitAnswered(s, work.ID, next, steps.AnswerTimeout)
	s.Check("no task from late answer", len(s.Codex.Calls()) == 2, "original and next task", fmt.Sprint(len(s.Codex.Calls())))
}

func TestQuestionsRestartExpiresOrphanRPC(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "QUESTIONS-RESTART", harness.Options{Golden: harness.GoldenOptions{Skip: "Question edits are asynchronous; explicit assertions check the native protocol, routing, and terminal state."}, Title: "A helper restart does not replay an orphan RPC or reuse its question number", Budget: 120 * time.Second})
	defer s.Finish()
	token := s.Token("ask")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(token) {
			return harness.CodexBehavior{Questions: scriptedQuestions(false, 1)}
		}
		return harness.CodexBehavior{}
	})
	ctl := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, ctl, "questions-restart")
	steps.MustPost(s, work.ID, "Ask "+token)
	s.Require("first question delivered", steps.AnswerTimeout, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, "Question from Codex")) == 1, "waiting"
	})
	s.Require("pending question is marked", steps.AnswerTimeout, func() (bool, string) {
		questions := s.SUTMessagesContaining(work.ID, "Question from Codex")
		return len(questions) == 1 && hasPendingQuestionMarker(questions[0]), "waiting for first-line marker"
	})
	s.StopSUT()
	s.StartSUT()
	s.Require("orphan question cleanup clears marker after restart", steps.AnswerTimeout, func() (bool, string) {
		for _, m := range s.SUTMessages(work.ID) {
			if hasPendingQuestionMarker(m) {
				return false, "marker remains on " + m.ID
			}
		}
		return true, "pending marker removed"
	})
	steps.MustPost(s, work.ID, "helper answer Q1 B")
	s.Require("orphan reply refused", steps.AnswerTimeout, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, "already resolved")) > 0, "waiting for recovered closed question"
	})
	s.Check("old question not replayed", len(s.Codex.CallsWithToken(token)) == 1, "one original native call", fmt.Sprint(len(s.Codex.CallsWithToken(token))))
}

func TestQuestionsAsyncSkipAndQuotedQueueKeepTheirMeaning(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "QUESTIONS-ROUTING", harness.Options{Golden: harness.GoldenOptions{Skip: "Question edits are asynchronous; explicit assertions check the native protocol, routing, and terminal state."}, Title: "An async question does not capture ordinary input, explicit queue, or a local skip as an answer", Budget: 120 * time.Second})
	defer s.Finish()
	ask, ordinary, queued := s.Token("ask"), s.Token("ordinary"), s.Token("queued")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(ask) {
			return harness.CodexBehavior{Questions: scriptedQuestions(true, 1), Block: true}
		}
		return harness.CodexBehavior{}
	})
	ctl := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, ctl, "questions-routing")
	steps.MustPost(s, work.ID, "Ask "+ask)
	s.Require("async question delivered", steps.AnswerTimeout, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, "Question from Codex")) == 1, "waiting"
	})
	q := s.SUTMessagesContaining(work.ID, "Question from Codex")[0]
	call := s.Codex.CallsWithToken(ask)[0]
	tui := attachTUI(s, call.ThreadID)
	_, err := s.Graph.PostWithQuotesAsUser(work.ID, "helper queue "+queued, fakegraph.QuoteSpec{MessageID: q.ID})
	if err != nil {
		t.Fatal(err)
	}
	steps.MustPost(s, work.ID, "Additional instructions "+ordinary)
	s.Require("ordinary input steers without becoming a question reply", steps.AnswerTimeout, func() (bool, string) {
		raw := string(tui.call("thread/turns/list", map[string]any{"threadId": call.ThreadID, "limit": 5}))
		return strings.Contains(raw, ordinary) && !strings.Contains(raw, "questionItemId"), "waiting for ordinary steer"
	})
	steps.MustPost(s, work.ID, "helper answer Q1 --skip")
	s.Require("local skip visible", steps.CommandTimeout, func() (bool, string) { return len(s.SUTMessagesContaining(work.ID, "Skipped")) > 0, "waiting" })
	s.Check("queue still waits", len(s.Codex.Calls()) == 1, "one running native task", fmt.Sprint(len(s.Codex.Calls())))
	s.Codex.Release(call.Seq)
	steps.WaitAnswered(s, work.ID, queued, steps.AnswerTimeout)
	s.Check("skip did not add a turn", len(s.Codex.Calls()) == 2, "original and explicitly queued task", fmt.Sprint(len(s.Codex.Calls())))
}
