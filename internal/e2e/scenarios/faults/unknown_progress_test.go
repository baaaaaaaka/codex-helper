package faults

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

func TestUnknownProgressAllowsStatusCancelAndNextTask(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "UNKNOWN-PROGRESS", harness.Options{Title: "missing progress cannot block status, cancel, or the next task", Budget: 100 * time.Second, Golden: harness.GoldenOptions{Skip: "Explicit assertions cover command delivery, native cancellation, and the single ambiguous POST."}})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "unknown-progress")
	tok := s.Token("blockedprogress")
	note := "Unconfirmed progress " + tok
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(tok) {
			return harness.CodexBehavior{Commentary: note, Block: true}
		}
		return harness.CodexBehavior{}
	})
	fault := s.Graph.AddFault(fakegraph.Fault{Name: "progress-not-accepted", Kind: fakegraph.FaultDropBeforeAccept, Method: "POST", ChatID: work.ID, Times: 1, Match: func(r fakegraph.Request) bool { return strings.Contains(r.Body, note) }})
	steps.MustPost(s, work.ID, "Work until canceled "+tok)
	s.Require("progress POST fails ambiguously", 30*time.Second, func() (bool, string) { return fault.Fired() == 1, "fault not exercised" })
	steps.MustPost(s, work.ID, "helper status")
	s.Require("status delivered with unknown still absent", 30*time.Second, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, "1 unconfirmed")) > 0, "status pending"
	})
	steps.MustPost(s, work.ID, "helper cancel all")
	s.Require("cancel confirmation delivered and native turn canceled", 30*time.Second, func() (bool, string) {
		calls := s.Codex.CallsWithToken(tok)
		return len(s.SUTMessagesContaining(work.ID, "cancel all requested")) == 1 && len(calls) == 1 && calls[0].Outcome == "cancelled", "cancel pending"
	})
	next := s.Token("nextrequest")
	steps.MustPost(s, work.ID, "Next request "+next)
	_, ok := steps.WaitAnswered(s, work.ID, next, steps.AnswerTimeout)
	s.Check("same chat remains usable", ok && len(s.Codex.CallsWithToken(next)) == 1, "one new answer", fmt.Sprint(ok))
	posts := s.Graph.Requests(fakegraph.RequestFilter{Method: "POST", ChatID: work.ID})
	attempts := 0
	for _, p := range posts {
		if strings.Contains(p.Body, note) {
			attempts++
		}
	}
	s.Check("original progress was attempted once and remains absent", attempts == 1 && len(s.SUTMessagesContaining(work.ID, note)) == 0, "one POST, zero delivered copies", fmt.Sprint(attempts))
}

// Inputs are admitted before reads fail. This proves an unavailable read-back
// cannot hold an already queued task; it does not require reading new commands
// from a service that is deliberately refusing all message reads.
func TestUnknownAcceptedFinalAllowsQueuedTaskWhileReadsFail(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "UNKNOWN-READBACK", harness.Options{Title: "accepted final with lost response does not require read-back before the next task", Budget: 100 * time.Second, Golden: harness.GoldenOptions{Skip: "Explicit assertions check the request barrier, two executions, and the accepted final's POST count."}})
	defer s.Finish()
	first, next := s.Token("firstunknown"), s.Token("queuednext")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		return harness.CodexBehavior{Block: c.HasToken(first)}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "unknown-readback")
	steps.MustPost(s, work.ID, "First request "+first)
	s.WaitCodexCalls(first, 1, steps.AnswerTimeout)
	steps.MustPost(s, work.ID, "helper queue Following request "+next)
	s.Require("second request admitted", steps.CommandTimeout, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, "Queued.")) > 0, "queued acknowledgment pending"
	})
	reads := s.Graph.AddFault(fakegraph.Fault{Name: "message-reads-unavailable", Kind: fakegraph.FaultStatus, Status: 503, Method: "GET", Route: "GET /chats/{chat}/messages", ChatID: work.ID})
	lost := s.Graph.AddFault(fakegraph.Fault{Name: "accepted-final-response-lost", Kind: fakegraph.FaultAcceptedLost, Method: "POST", ChatID: work.ID, Match: steps.AnswerPostMatcher(first), Times: 1})
	s.Codex.Release(s.Codex.CallsWithToken(first)[0].Seq)
	_, ok := steps.WaitAnswered(s, work.ID, next, steps.AnswerTimeout)
	s.Check("queued task answers while reconciliation is unavailable", ok && lost.Fired() == 1 && len(s.Codex.CallsWithToken(next)) == 1, "one next answer during failed reads", fmt.Sprint(ok))
	s.Require("failed read path exercised", 20*time.Second, func() (bool, string) { return reads.Fired() > 0, "no failed read yet" })
	reads.Disarm()
	steps.MustPost(s, work.ID, "helper status")
	s.Require("same chat usable after reads return", steps.CommandTimeout, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, "idle, 0 queued")) > 0, "status pending"
	})
	s.Settle(2*time.Second, 6*time.Second)
	s.Check("accepted final was never posted again", len(steps.AnswerPosts(s, work.ID, first)) == 1, "one POST", fmt.Sprint(len(steps.AnswerPosts(s, work.ID, first))))
}
