package commands

import (
	"fmt"
	"html"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// A model switch while a turn runs leaves that turn alone and applies from
// the next one, on the same Codex thread, also after a crash (R-31).
//
// History: model and effort switches that changed the running turn under
// Codex, or that the queued prompt never saw (SPEC R-31: "the running
// turn completes on the old model, and the next turn's turn/start.model
// is gpt-x"). CMD-MODEL only switches an idle chat.
func TestModelSwitchWhileBusy(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "MODEL-SWITCH-BUSY", harness.Options{
		Guarantees: []string{"R-31"},
		Title:      "`model switch gpt-5-mini` during a running turn: that turn keeps gpt-5-codex, the queued prompt and later ones (also after SIGKILL) run on gpt-5-mini, same thread",
		Budget:     120 * time.Second,
	})
	defer s.Finish()
	running := s.Token("running")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(running) {
			return harness.CodexBehavior{Block: true}
		}
		return harness.CodexBehavior{}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "modelbusy")

	steps.MustPost(s, work.ID, "Long task "+running)
	s.Require("the long turn reached Codex", steps.AnswerTimeout, func() (bool, string) {
		return len(s.Codex.CallsWithToken(running)) == 1, "not yet"
	})
	reply, ok := command(s, work.ID, "helper model switch gpt-5-mini", "Model profile switched")
	norm := html.UnescapeString(harness.NormalizeSpace(reply))
	s.Check("the switch is accepted and says the running turn keeps its model", ok && strings.Contains(norm, "The running turn keeps its model"),
		"\"Model profile switched … The running turn keeps its model.\"", clip(norm, 300))

	queued := s.Token("queued")
	steps.MustPost(s, work.ID, "Next task "+queued)
	s.Poll(4*time.Second, func() bool { return len(s.Codex.CallsWithToken(queued)) > 0 })
	early := len(s.Codex.CallsWithToken(queued))
	s.Codex.Release(s.Codex.CallsWithToken(running)[0].Seq)
	steps.WaitAnswered(s, work.ID, running, steps.AnswerTimeout)
	steps.WaitAnswered(s, work.ID, queued, steps.AnswerTimeout)

	s.CrashSUT()
	s.StartSUT()
	after := s.Token("aftercrash")
	steps.MustPost(s, work.ID, "After the crash "+after)
	steps.WaitAnswered(s, work.ID, after, steps.AnswerTimeout)
	s.Settle(2*time.Second, 8*time.Second)

	one := func(tok string) harness.CodexCall {
		if c := s.Codex.CallsWithToken(tok); len(c) == 1 {
			return c[0]
		}
		return harness.CodexCall{}
	}
	r, q, a := one(running), one(queued), one(after)
	s.Check("the queued prompt waits for the running turn", early == 0, "not started while the turn runs", fmt.Sprintf("%d calls", early))
	// The chat had no model selection when the turn started: turn/start
	// leaves the model to Codex's account default.
	s.Check("the running turn is neither restarted nor moved to the new model", len(s.Codex.CallsWithToken(running)) == 1 && (r.Model == "" || r.Model == "gpt-5-codex"),
		"1 Codex call on the account default", fmt.Sprintf("%d calls, model=%q", len(s.Codex.CallsWithToken(running)), r.Model))
	s.Check("the next turn runs on the new model and its effort, on the same thread",
		q.Model == "gpt-5-mini" && q.Effort == "low" && q.ThreadID != "" && q.ThreadID == r.ThreadID,
		"gpt-5-mini, low, same thread", fmt.Sprintf("model=%q effort=%q thread %q -> %q", q.Model, q.Effort, r.ThreadID, q.ThreadID))
	s.Check("the switch survives a crash", a.Model == "gpt-5-mini" && a.ThreadID == r.ThreadID,
		"gpt-5-mini, same thread", fmt.Sprintf("model=%q thread %q", a.Model, a.ThreadID))
	n := len(steps.AnswerPosts(s, work.ID, running)) + len(steps.AnswerPosts(s, work.ID, queued)) + len(steps.AnswerPosts(s, work.ID, after))
	s.Check("each prompt is answered once", n == 3, "3 answers", fmt.Sprint(n), s.Transcript(work.ID, 12)...)
}
