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

// R-5b (owner decision "read back; resend the final if it is absent";
// STORE-DESIGN §3.7): a final-answer POST that fails ambiguously while Graph
// never stores the message is read back, and once the read-backs agree it is
// absent, it is sent again exactly once.
//
// Part 1: the first POST is lost before Graph accepts it. Expected: the
// answer ends up in the chat exactly once, from exactly two POSTs, with a
// read-back of the chat between them and no "not confirmed" notice.
//
// Part 2: the resend is lost the same way. Expected: no third POST, one
// visible notice, and the chat keeps working.
func TestR05b_AbsentFinalResentOnce(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "R-5b", harness.Options{
		Guarantees: []string{"R-5", "R-1", "R-2"},
		Title:      "final POST never stored by Graph is read back and resent exactly once",
		Budget:     150 * time.Second,
	})
	defer s.Finish()

	control := steps.ReadyControlChat(s)

	// Part 1: absent once → resent once.
	work, _ := steps.NewWorkChat(s, control, "r05b-once")
	tok := s.Token("absent")
	drop := s.Graph.AddFault(fakegraph.Fault{
		Name: "final-reset-before-accept", Kind: fakegraph.FaultDropBeforeAccept,
		Method: "POST", ChatID: work.ID, Match: steps.AnswerPostMatcher(tok), Times: 1,
	})
	steps.MustPost(s, work.ID, "Answer me "+tok)
	s.WaitCodexCalls(tok, 1, steps.AnswerTimeout)
	s.WaitFor("lossy final POST attempted", steps.AnswerTimeout, func() (bool, string) {
		return drop.Fired() > 0, "fault not fired"
	})
	answer := ""
	s.WaitFor("absent final resent and visible", 60*time.Second, func() (bool, string) {
		calls := s.Codex.CallsWithToken(tok)
		if len(calls) == 0 || calls[0].Answer == "" {
			return false, "no answer yet"
		}
		answer = calls[0].Answer
		n := len(s.SUTMessagesContaining(work.ID, answer))
		return n > 0, fmt.Sprintf("%d copies, %d POSTs", n, len(steps.AnswerPosts(s, work.ID, tok)))
	})
	s.Settle(5*time.Second, 20*time.Second)
	posts := steps.AnswerPosts(s, work.ID, tok)
	var ev []string
	for _, p := range posts {
		ev = append(ev, p.String())
	}
	readBetween := 0
	if len(posts) >= 2 {
		for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Method: "GET", ChatID: work.ID}) {
			if r.Seq > posts[0].Seq && r.Seq < posts[1].Seq && strings.Contains(r.Route, "messages") {
				readBetween++
			}
		}
	}
	transcript := s.Transcript(work.ID, 30)
	copies := len(s.SUTMessagesContaining(work.ID, answer))
	s.Check("R-5b absent final is POSTed exactly twice (one resend)", len(posts) == 2, "2 POST attempts", fmt.Sprintf("%d POST attempts", len(posts)), ev...)
	s.Check("R-5b the resend follows a read-back of the chat", readBetween > 0, "≥1 message read between the POSTs", fmt.Sprintf("%d reads", readBetween), ev...)
	s.Check("R-2 exactly one copy of the answer in the chat", copies == 1, "1 copy", fmt.Sprintf("%d copies", copies), transcript...)
	notices := len(s.SUTMessagesContaining(work.ID, "could not be confirmed"))
	s.Check("R-5b no 'not confirmed' notice once the resend landed", notices == 0, "0 notices", fmt.Sprintf("%d notices", notices), transcript...)
	s.Check("R-3 Codex ran once for the prompt", len(s.Codex.CallsWithToken(tok)) == 1, "1", fmt.Sprint(len(s.Codex.CallsWithToken(tok))))

	// Part 2: absent twice → dropped with one visible notice, no 3rd POST.
	work2, _ := steps.NewWorkChat(s, control, "r05b-twice")
	tok2 := s.Token("absent2")
	drop2 := s.Graph.AddFault(fakegraph.Fault{
		Name: "final-reset-before-accept-twice", Kind: fakegraph.FaultDropBeforeAccept,
		Method: "POST", ChatID: work2.ID, Match: steps.AnswerPostMatcher(tok2), Times: 2,
	})
	steps.MustPost(s, work2.ID, "Answer me twice "+tok2)
	s.WaitCodexCalls(tok2, 1, steps.AnswerTimeout)
	s.WaitFor("both lossy POSTs attempted", 60*time.Second, func() (bool, string) {
		return drop2.Fired() >= 2, fmt.Sprintf("fault fired %d times", drop2.Fired())
	})
	s.WaitFor("visible notice after the second absence", 60*time.Second, func() (bool, string) {
		n := len(s.SUTMessagesContaining(work2.ID, "could not be confirmed"))
		return n > 0, fmt.Sprintf("%d notices", n)
	})
	tok3 := s.Token("after")
	steps.MustPost(s, work2.ID, "Next question "+tok3)
	s.WaitFor("next prompt answered", 45*time.Second, func() (bool, string) {
		for _, c := range s.Codex.CallsWithToken(tok3) {
			if c.Answer != "" && len(s.SUTMessagesContaining(work2.ID, c.Answer)) > 0 {
				return true, ""
			}
		}
		return false, fmt.Sprintf("%d calls for next prompt", len(s.Codex.CallsWithToken(tok3)))
	})
	s.Settle(3*time.Second, 15*time.Second)
	posts2 := steps.AnswerPosts(s, work2.ID, tok2)
	var ev2 []string
	for _, p := range posts2 {
		ev2 = append(ev2, p.String())
	}
	transcript2 := s.Transcript(work2.ID, 30)
	s.Check("R-5b a resend that is absent again is not sent a third time", len(posts2) == 2, "2 POST attempts", fmt.Sprintf("%d POST attempts", len(posts2)), ev2...)
	n2 := len(s.SUTMessagesContaining(work2.ID, "could not be confirmed"))
	s.Check("R-1 exactly one visible notice for the lost answer", n2 == 1, "1 notice", fmt.Sprintf("%d notices", n2), transcript2...)
	var visible []string
	for _, m := range s.SUTMessages(work2.ID) {
		visible = append(visible, m.PlainText())
	}
	leaks := steps.InternalLeaks(s, strings.Join(visible, "\n"))
	s.Check("R-33 notice carries no internal ids", len(leaks) == 0, "no leaks", fmt.Sprint(leaks), leaks...)
}
