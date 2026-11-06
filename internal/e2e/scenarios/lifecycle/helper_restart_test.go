package lifecycle

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Owner decision: `helper restart` (confirmed: `helper restart now`) in the
// control chat restarts the helper and posts exactly one confirmation, and
// only after the new process owns the scope: the confirmation comes from a
// new SUT instance, after that instance's first Graph request, and the old
// instance is silent from then on. Work continues afterwards.
//
// The SUT has to restart itself (as under its service supervisor); the
// harness does not restart it. See the M3 report for the adapter gap on
// the old core.
func TestHelperRestartConfirmation(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "RESTART", harness.Options{
		Guarantees: []string{"helper-restart-one-confirmation"},
		Title:      "`helper restart now`: one confirmation, posted by the new process after it owns the scope",
		Budget:     120 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	chat, _ := steps.NewWorkChat(s, control, "restart")
	warm := s.Token("warm")
	steps.MustPost(s, chat.ID, "First task "+warm)
	steps.WaitAnswered(s, chat.ID, warm, steps.AnswerTimeout)
	s.Settle(2*time.Second, 10*time.Second)

	oldInst := s.SUT.Instance()
	cmdSeq := s.Graph.LastSeq()
	steps.MustPost(s, control.ID, "helper restart now")
	confirmations := func() []fakegraph.Message {
		var out []fakegraph.Message
		for _, m := range s.SUTMessages(control.ID) {
			if m.RequestSeq > cmdSeq && m.Instance != "" && m.Instance != oldInst && strings.Contains(m.PlainText(), "Helper restart completed") {
				out = append(out, m)
			}
		}
		return out
	}
	s.WaitFor("a restart confirmation from a new instance", 60*time.Second, func() (bool, string) {
		return len(confirmations()) > 0, "instance " + s.SUT.Instance()
	})
	s.Settle(3*time.Second, 15*time.Second)
	conf := confirmations()

	var replies []string
	for _, m := range s.SUTMessages(control.ID) {
		if m.RequestSeq > cmdSeq {
			replies = append(replies, fmt.Sprintf("[%s] %s", m.Instance, clip(harness.NormalizeSpace(m.PlainText()), 110)))
		}
	}
	newInst := s.SUT.Instance()
	s.Check("restart: a new helper process takes over", newInst != oldInst && s.SUT.Running(), "a new running instance", fmt.Sprintf("old=%s now=%s running=%v", oldInst, newInst, s.SUT.Running()), replies...)
	s.Check("restart: exactly one confirmation, from the new process", len(conf) == 1, "1", fmt.Sprint(len(conf)), replies...)
	if len(conf) > 0 {
		c := conf[0]
		var firstNew int64
		if rs := s.Graph.Requests(fakegraph.RequestFilter{Instance: c.Instance, Since: cmdSeq}); len(rs) > 0 {
			firstNew = rs[0].Seq
		}
		oldAfter := len(s.Graph.Requests(fakegraph.RequestFilter{Instance: oldInst, Since: c.RequestSeq}))
		s.Check("restart: the confirmation follows the new process's takeover; the old one is gone", firstNew > 0 && firstNew < c.RequestSeq && oldAfter == 0,
			"new instance active before the confirmation, 0 old-instance requests after it", fmt.Sprintf("first new request #%d, confirmation #%d, old-instance requests after it %d", firstNew, c.RequestSeq, oldAfter))
	}
	next := s.Token("after")
	steps.MustPost(s, chat.ID, "After the restart "+next)
	_, ok := steps.WaitAnswered(s, chat.ID, next, 45*time.Second)
	s.Check("restart: work continues afterwards", ok && len(s.SUTMessagesContaining(chat.ID, "answer for "+next)) == 1, "answered once", fmt.Sprint(ok), s.Transcript(chat.ID, 8)...)
}
