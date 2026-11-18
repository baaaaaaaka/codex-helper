package turns

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

func TestNativeGoalSilenceWithToolActivity(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "NATIVE-GOAL-QUIET", harness.Options{
		Title:  "a goal-triggered turn with internal tool activity posts one quiet notice",
		Budget: 60 * time.Second,
		Seam: harness.Seam{Knobs: map[string]string{
			"NativeQuietHint": "2s", "Maintenance": "200ms",
		}},
		Golden: harness.GoldenOptions{Skip: "Direct assertions verify the live status, notice count, and answer."},
	})
	defer s.Finish()

	seed, goal := s.Token("seed"), s.Token("goal")
	polls := make([]string, 24)
	for i := range polls {
		polls[i] = "sleep 0.25"
	}
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(goal) {
			return harness.CodexBehavior{Block: true, Exec: polls}
		}
		return harness.CodexBehavior{}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "quiet-goal")
	steps.MustPost(s, work.ID, "Seed "+seed)
	if _, ok := steps.WaitAnswered(s, work.ID, seed, steps.AnswerTimeout); !ok {
		t.Fatal("seed request did not answer")
	}
	tui := attachTUI(s, s.Codex.CallsWithToken(seed)[0].ThreadID)
	steps.MustPost(s, work.ID, "helper goal Monitor "+goal)
	if _, ok := s.WaitSUTMessage(work.ID, "Goal set.", steps.CommandTimeout); !ok {
		t.Fatal("goal command did not activate")
	}
	tui.call("fakecodex/continueGoal", map[string]any{"threadId": tui.thread})
	s.WaitCodexCalls(goal, 1, steps.AnswerTimeout)
	call := s.Codex.CallsWithToken(goal)[0]

	steps.MustPost(s, work.ID, "helper status")
	if _, ok := s.WaitSUTMessage(work.ID, "Native turn: running.", steps.CommandTimeout); !ok {
		t.Error("helper status did not show the native turn")
	}
	const notice = "Codex is still running. No new progress yet."
	s.Require("Teams reports silence during internal polling", 5*time.Second, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, notice)) == 1, "quiet notice missing"
	})
	s.Settle(3*time.Second, 4*time.Second)
	count := len(s.SUTMessagesContaining(work.ID, notice))
	s.Check("the active native turn gets only one quiet notice", count == 1, "one notice", fmt.Sprint(count))
	s.Codex.Release(call.Seq)
	if _, ok := steps.WaitAnswered(s, work.ID, goal, steps.AnswerTimeout); !ok {
		t.Fatal("native goal answer was not mirrored")
	}
	steps.MustPost(s, work.ID, "helper status")
	status, idle := s.WaitSUTMessage(work.ID, "Native turn: none observed.", steps.CommandTimeout)
	reported := status.PlainText()
	s.Check("a completed turn leaves the goal active and has no running native turn",
		idle && strings.Contains(reported, "Goal status: active."), "active goal with no running turn", reported)
}
