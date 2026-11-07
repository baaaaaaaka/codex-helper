package commands

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// CMD-GOAL (owner request 2026-11-07): `helper goal` shows, sets, resumes
// and pauses the Codex goal of a Work chat's thread; while the goal is
// active every Codex status and answer carries a "🎯 Active goal:" line,
// and every answer a "🔧 Helper: model - …, effort - …, tier - …" line,
// both before the "finished responding" footer; `helper cancel` pauses
// an active goal (Codex's interrupt does not), also when nothing of the
// helper's runs; the Control chat refuses `helper goal`.
func TestGoalCommands(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CMD-GOAL", harness.Options{
		Guarantees: []string{"cmd-goal"},
		Title:      "`helper goal [<objective>|active|pause]`; goal and model lines on new Codex messages; `helper cancel` pauses the goal",
		Budget:     180 * time.Second,
	})
	defer s.Finish()
	run := s.Token("goalrun")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(run) {
			return harness.CodexBehavior{Block: true, Commentary: "Working through the goal " + run, Answer: "LATE-ANSWER-SHOULD-NOT-APPEAR " + run}
		}
		return harness.CodexBehavior{}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "goal")

	expect(s, work.ID, "helper goal", "This chat has no Codex thread yet.")
	expect(s, control.ID, "helper goal", "Wrong chat")
	c, ok := askNotified(s, control.ID, work.ID, "before")
	before := answerText(s, work.ID, c)
	s.Check("without a goal an answer has the model line and no goal line",
		ok && strings.Contains(before, "🔧 Helper: model - gpt-5-codex, effort - default, tier - standard 🔧 Helper: ✅ Codex finished responding.") &&
			!strings.Contains(before, "Active goal"),
		"model line before the footer, no goal line", clip(before, 400))

	expect(s, work.ID, "helper goal", "No goal is set for this chat.", "helper goal <objective>")
	expect(s, work.ID, "helper goal active", "No goal is set for this chat.")
	objective := "Ship the quarterly report " + s.Token("objective")
	expect(s, work.ID, "helper goal "+objective, "Goal set.", "Goal status: active", "Goal: "+objective)
	expect(s, work.ID, "helper goal", "Goal status: active", "Goal: "+objective)
	expect(s, work.ID, "helper goal active", "The goal is already active.", "Goal status: active")

	c, ok = askNotified(s, control.ID, work.ID, "withgoal")
	with := answerText(s, work.ID, c)
	s.Check("with an active goal an answer has the goal line, then the model line, then the footer",
		ok && strings.Contains(with, "🎯 Active goal: "+objective+" 🔧 Helper: model - gpt-5-codex, effort - default, tier - standard 🔧 Helper: ✅ Codex finished responding."),
		"goal line, model line, footer", clip(with, 400))

	steps.MustPost(s, work.ID, "Long job "+run)
	s.WaitCodexCalls(run, 1, steps.AnswerTimeout)
	st, seen := s.WaitSUTMessage(work.ID, "Working through the goal "+run, steps.AnswerTimeout)
	status := harness.NormalizeSpace(st.PlainText())
	s.Check("a Codex status carries the goal line and no model line",
		seen && strings.Contains(status, "🎯 Active goal: "+objective) && !strings.Contains(status, "model - "),
		"goal line only", clip(status, 300))
	stop := expect(s, work.ID, "helper cancel", "The goal is paused too")
	_, cancelled := s.WaitSUTMessage(work.ID, "Codex request canceled.", 30*time.Second)
	s.Codex.ReleaseAll()
	s.Check("`helper cancel` stops the running turn and pauses the goal", cancelled && strings.Contains(stop, "helper goal active resumes it"),
		"turn cancelled, goal paused", clip(stop, 300))
	expect(s, work.ID, "helper goal", "Goal status: paused", "Goal: "+objective)

	c, ok = askNotified(s, control.ID, work.ID, "paused")
	paused := answerText(s, work.ID, c)
	s.Check("a paused goal has no goal line; the next message does not resume it",
		ok && !strings.Contains(paused, "Active goal") && strings.Contains(paused, "🔧 Helper: model - gpt-5-codex"),
		"model line, no goal line", clip(paused, 400))
	expect(s, work.ID, "helper goal", "Goal status: paused")

	expect(s, work.ID, "helper goal active", "Goal active again.", "Goal status: active", "Goal: "+objective)
	expect(s, work.ID, "helper cancel", "the active goal is paused")
	expect(s, work.ID, "helper goal", "Goal status: paused")
	expect(s, work.ID, "helper goal resume", "Goal active again.")
	expect(s, work.ID, "helper goal pause", "Goal paused.", "Goal status: paused", "Goal: "+objective)
	s.Settle(2*time.Second, 10*time.Second)
	s.Check("the cancelled turn's answer never reaches the chat", len(s.SUTMessagesContaining(work.ID, "LATE-ANSWER")) == 0, "none",
		fmt.Sprint(len(s.SUTMessagesContaining(work.ID, "LATE-ANSWER"))))
}

// answerText is the plain text of the chat's post of c's answer ("" when
// there is none).
func answerText(s *harness.Scenario, chatID string, c harness.CodexCall) string {
	if c.Answer == "" {
		return ""
	}
	msgs := s.SUTMessagesContaining(chatID, c.Answer)
	if len(msgs) == 0 {
		return ""
	}
	return harness.NormalizeSpace(msgs[len(msgs)-1].PlainText())
}
