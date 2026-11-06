package sharedhome

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// v0.1.20's `cxp teams pause [reason]` and `cxp teams resume` are hidden
// aliases of `cxp teams stop` and `cxp teams start` (F-TRN-17 / R-40,
// coordinator ruling): the same stop everywhere, the same output; neither
// is listed in the help.
func TestPauseAndResumeAreStopAndStart(t *testing.T) {
	harness.NeedsCLI(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "PAUSE-ALIAS", harness.Options{
		Guarantees: []string{"F-TRN-17", "R-40"},
		Title:      "`cxp teams pause [reason]` / `resume` are hidden aliases of `stop` / `start`: the helper stops everywhere and the prompt sent meanwhile is answered once after `resume`",
		Budget:     150 * time.Second,
		Seam:       stopSeam("host-a"),
		Invariants: harness.Invariants{Except: []harness.Exception{
			{Invariant: harness.InvCleanStop, Match: `cxp-1 exited on its own with "exit status 1"`,
				Reason: "the point of the scenario: a foreground helper exits when it is paused (stopped on all machines)"},
		}},
	})
	defer s.Finish()
	// The help itself is not part of the golden (it changes with every
	// command added): only that it lists neither alias.
	help := s.RunCLIAs(20*time.Second, harness.CLIAs{Poll: true}, "teams", "--help")
	s.Check("`cxp teams --help` lists neither `pause` nor `resume`",
		help.Code == 0 && strings.Contains(help.Stdout, "stop") && !strings.Contains(help.Stdout, "pause") && !strings.Contains(help.Stdout, "resume"),
		"exit 0, stop listed, no pause/resume", fmt.Sprintf("exit %d", help.Code))

	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "pause")
	first := s.Token("first")
	steps.MustPost(s, work.ID, "Before the pause "+first)
	_, answered := steps.WaitAnswered(s, work.ID, first, steps.AnswerTimeout)
	s.Check("the helper answers before the pause", answered, "answered", fmt.Sprint(answered))

	out, code := s.RunCLI(120*time.Second, "teams", "pause", "lunch", "break")
	stopped := s.Poll(5*time.Second, func() bool { return !s.SUT.Running() })
	s.Check("`cxp teams pause` stops the running helper like `cxp teams stop`",
		code == 0 && stopped && strings.Contains(out, "Stopping the Teams helper on all machines") &&
			strings.Contains(out, ") stopped.") && strings.Contains(out, "To start it again: cxp teams start"),
		"exit 0, owner exited, stop output", fmt.Sprintf("exit %d, owner exited=%v; output %s", code, stopped, clipTail(out, 400)))

	held := s.Token("held")
	steps.MustPost(s, work.ID, "While paused "+held)
	line := statusFirstLine(s)
	s.Check("`cxp teams status` shows the stop", stoppedLineOK(line, "`cxp teams stop`", "host-a"),
		"Stopped on all machines by `cxp teams stop` (host-a, <time>). Start again with `cxp teams start`.", line)
	again, code := s.RunCLI(30*time.Second, "teams", "pause")
	s.Check("`cxp teams pause` again changes nothing", code == 0 && strings.Contains(again, "Already stopped; nothing changed."),
		"exit 0, already stopped", fmt.Sprintf("exit %d; %s", code, clipTail(again, 300)))

	resumed, code := s.RunCLI(30*time.Second, "teams", "resume")
	s.Check("`cxp teams resume` removes the stop like `cxp teams start`", code == 0 &&
		strings.Contains(resumed, "Started the Teams helper again on all machines"),
		"exit 0, started", fmt.Sprintf("exit %d; %s", code, clipTail(resumed, 400)))
	again, code = s.RunCLI(30*time.Second, "teams", "resume")
	s.Check("`cxp teams resume` again changes nothing", code == 0 && strings.Contains(again, "was not stopped"),
		"exit 0, not stopped", fmt.Sprintf("exit %d; %s", code, clipTail(again, 300)))

	s.StartSUT()
	_, heldAnswered := steps.WaitAnswered(s, work.ID, held, steps.AnswerTimeout)
	s.Check("after `cxp teams resume` the prompt sent while paused is answered", heldAnswered, "answered", fmt.Sprint(heldAnswered))
	exactlyOnce(s, work.ID, held)
}
