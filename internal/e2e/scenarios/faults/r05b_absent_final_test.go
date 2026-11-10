package faults

import (
	"fmt"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Graph listing absence is not proof that a POST was rejected. Keep the
// content recoverable and report it in control, without blind retries.
func TestR05b_UnconfirmedFinalRetained(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "R-5b", harness.Options{Guarantees: []string{"R-5", "R-1", "R-2"}, Title: "an unconfirmed final is retained across restart without blind resend; the same chat and other chats continue", Budget: 100 * time.Second})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "unconfirmed")
	tok := s.Token("absent")
	fault := s.Graph.AddFault(fakegraph.Fault{Name: "final-reset-before-accept", Kind: fakegraph.FaultDropBeforeAccept, Method: "POST", ChatID: work.ID, Match: steps.AnswerPostMatcher(tok), Times: 1})
	steps.MustPost(s, work.ID, "Answer me "+tok)
	s.Require("control reports unconfirmed delivery", 45*time.Second, func() (bool, string) {
		return len(s.SUTMessagesContaining(control.ID, "Delivery unconfirmed")) == 1, "no warning"
	})
	s.Check("the ambiguous send was attempted once", fault.Fired() == 1 && len(steps.AnswerPosts(s, work.ID, tok)) == 1, "1 POST", fmt.Sprint(len(steps.AnswerPosts(s, work.ID, tok))))
	s.StopSUT()
	s.StartSUT()
	steps.MustPost(s, work.ID, "helper status")
	s.Require("same chat reports retained unknown", 30*time.Second, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, "1 unconfirmed")) == 1, "status pending"
	})
	following := s.Token("following")
	steps.MustPost(s, work.ID, "Next request "+following)
	_, ok := steps.WaitAnswered(s, work.ID, following, steps.AnswerTimeout)
	s.Check("same chat executes and answers once", ok && len(s.Codex.CallsWithToken(following)) == 1, "one answer, one execution", fmt.Sprint(ok))
	// Completion publication and the next control command are asynchronous.
	// Wait for this request's notice so their transcript order is deliberate.
	s.Require("following completion notice arrives", 30*time.Second, func() (bool, string) {
		return len(s.SUTMessagesContaining(control.ID, following)) == 1, "completion notice pending"
	})
	other, _ := steps.NewWorkChat(s, control, "other-delivery")
	next := s.Token("unrelated")
	steps.MustPost(s, other.ID, "Answer independently "+next)
	s.Require("other chats can answer", 30*time.Second, func() (bool, string) { return len(steps.AnswerPosts(s, other.ID, next)) > 0, "other answer pending" })
	s.Settle(3*time.Second, 8*time.Second)
	s.Check("restart does not resend ambiguous content", len(steps.AnswerPosts(s, work.ID, tok)) == 1, "1 POST", fmt.Sprint(len(steps.AnswerPosts(s, work.ID, tok))))
	s.Check("missing original remains absent", len(s.SUTMessagesContaining(work.ID, s.Codex.CallsWithToken(tok)[0].Answer)) == 0, "no original delivered", "check transcript", s.Transcript(work.ID, 8)...)
	s.Check("original task never reruns", len(s.Codex.CallsWithToken(tok)) == 1, "one execution", fmt.Sprint(len(s.Codex.CallsWithToken(tok))))
	s.Check("no duplicate warning", len(s.SUTMessagesContaining(control.ID, "Delivery unconfirmed")) == 1, "1 warning", fmt.Sprint(len(s.SUTMessagesContaining(control.ID, "Delivery unconfirmed"))))
}
