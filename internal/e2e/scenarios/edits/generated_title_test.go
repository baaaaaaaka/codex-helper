package edits

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

func TestGeneratedTitle(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "EDIT-GENERATED-TITLE", harness.Options{Title: "new work chat gets one isolated title; persisted name and manual rename survive restart", Budget: 120 * time.Second})
	defer s.Finish()
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	s.Codex.SetTitleScript(func(r fakecodex.TitleRequest) fakecodex.TitleResult {
		<-release
		return fakecodex.TitleResult{Output: `{"title":"登录修复"}`}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "generated-title")
	tok, _, _ := ask(s, work.ID, "Fix the login flow and add regression coverage", "first")
	s.WaitSUTMessage(control.ID, "Codex finished", steps.CommandTimeout)
	// The prompt title update is asynchronous with the finished notice. Wait
	// for it before releasing generated-title work so the transcript has a
	// deterministic title history on slow runners.
	s.Require("first-prompt title applied before generated title", steps.CommandTimeout, func() (bool, string) {
		got := topic(s, work.ID)
		return strings.Contains(got, "Fix the login flow"), fmt.Sprintf("title %q", got)
	})
	once.Do(func() { close(release) })
	want := "💬 " + harness.MachineLabel + " - 登录修复"
	s.Check("generated title applied", waitTopic(s, work.ID, want, steps.CommandTimeout), want, topic(s, work.ID))
	calls := s.Codex.TitleCalls()
	s.Check("exactly one cosmetic request", len(calls) == 1, "1", fmt.Sprint(len(calls)))
	if len(calls) == 1 {
		encoded := fmt.Sprint(calls[0].Turn)
		s.Check("wrapper instructions excluded", !strings.Contains(encoded, "Teams helper safety:") && strings.Contains(encoded, "Fix the login"), "unwrapped first task", encoded)
	}
	thread := ""
	if calls := s.Codex.CallsWithToken(tok); len(calls) > 0 {
		thread = calls[0].ThreadID
	}
	th, err := (fakecodex.Store{Dir: s.Env.CodexHome + "/e2e-fakecodex"}).Load(thread)
	s.Check("saved to original Codex thread", err == nil && th.Name == "登录修复", "登录修复", fmt.Sprintf("%+v %v", th, err))
	s.StopSUT()
	s.StartSUT()
	second, _, _ := ask(s, work.ID, "Continue the tests", "second")
	s.Check("restart and existing thread do not generate again", len(s.Codex.TitleCalls()) == 1, "1", fmt.Sprint(len(s.Codex.TitleCalls())))
	// Wait for this completion notice before renaming; the first turn's
	// notice is still present and would let the rename race this snapshot.
	s.WaitSUTMessage(control.ID, "Continue the tests "+second, steps.CommandTimeout)
	command(s, work.ID, "helper rename My choice", "Work chat renamed.")
	ask(s, work.ID, "One more task", "third")
	s.Check("manual title wins", topic(s, work.ID) == "💬 "+harness.MachineLabel+" - My choice", "manual title", topic(s, work.ID))
}

func TestGeneratedTitleDoesNotDelayAnswers(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "EDIT-TITLE-HELD", harness.Options{
		Title:  "held title never delays answers or manual rename; cancellation is cleaned up",
		Budget: 100 * time.Second,
		Golden: harness.GoldenOptions{Masks: []harness.Mask{{
			Pattern: regexp.MustCompile(`(<strong>Chat:</strong> )[^<]*(<br>)`),
			Replace: `${1}<chat title>${2}`,
			Reason:  "the completion notice snapshots the chat title while the asynchronous title update is pending",
		}}},
	})
	defer s.Finish()
	s.Codex.SetTitleScript(func(fakecodex.TitleRequest) fakecodex.TitleResult { return fakecodex.TitleResult{Hold: true} })
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "held-title")
	ask(s, work.ID, "A task with a held cosmetic request", "first")
	s.Require("title request started", 5*time.Second, func() (bool, string) { return len(s.Codex.TitleCalls()) == 1, "not started" })
	s.Require("first-prompt title applied", steps.CommandTimeout, func() (bool, string) {
		got := topic(s, work.ID)
		return strings.Contains(got, "A task with a held cosmetic request"), fmt.Sprintf("title %q", got)
	})
	s.WaitSUTMessage(control.ID, "Codex finished", steps.CommandTimeout)
	_, ok := command(s, work.ID, "helper rename Owner title", "Work chat renamed.")
	s.Check("manual rename cancels held title", ok && topic(s, work.ID) == "💬 "+harness.MachineLabel+" - Owner title", "manual title applied", topic(s, work.ID))
	ask(s, work.ID, "Another task", "second")
	s.Check("no title retry", len(s.Codex.TitleCalls()) == 1, "1", fmt.Sprint(len(s.Codex.TitleCalls())))
}

func TestGeneratedTitleNotRecoveredAfterRestart(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "EDIT-TITLE-RESTART", harness.Options{Title: "restart cancels held naming without replaying a naming task", Budget: 100 * time.Second})
	defer s.Finish()
	s.Codex.SetTitleScript(func(fakecodex.TitleRequest) fakecodex.TitleResult { return fakecodex.TitleResult{Hold: true} })
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "title-restart")
	ask(s, work.ID, "First task before restart", "first")
	s.Require("title request started", 5*time.Second, func() (bool, string) { return len(s.Codex.TitleCalls()) == 1, "not started" })
	s.StopSUT()
	s.StartSUT()
	ask(s, work.ID, "Next task after restart", "second")
	s.Check("restart does not recover cosmetic work", len(s.Codex.TitleCalls()) == 1, "1", fmt.Sprint(len(s.Codex.TitleCalls())))
}
