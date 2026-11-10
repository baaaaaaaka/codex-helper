package restart

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// R-4 / R-29 (with R-5b): work accepted before a stop, and messages posted
// while the SUT is stopped, are processed after restart exactly once.
//
//  1. Prompt A starts and blocks in Codex; prompt B is queued behind it.
//  2. The SUT is stopped (graceful).
//  3. While stopped the user posts prompt C.
//  4. The SUT restarts; later it is restarted once more.
//
// Expected: B and C each run exactly once (B before C); A is not re-run
// automatically (R-5b) and the chat shows that A was interrupted; nothing is
// replayed by the second restart.
func TestR04_R29_RestartPreservesWork(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "R-4-29", harness.Options{
		Guarantees: []string{"R-4", "R-29", "R-5", "R-3", "F-TRN-5"},
		Title:      "queued and down-time messages survive a restart exactly once; running turn not re-run",
		Budget:     120 * time.Second,
	})
	defer s.Finish()
	tokA, tokB, tokC := s.Token("running"), s.Token("queued"), s.Token("whiledown")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(tokA) {
			return harness.CodexBehavior{Block: true}
		}
		return harness.CodexBehavior{}
	})

	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "r04")

	steps.MustPost(s, work.ID, "Long task "+tokA)
	s.WaitCodexCalls(tokA, 1, steps.AnswerTimeout)
	steps.MustPost(s, work.ID, "helper queue Follow-up "+tokB)
	s.WaitFor("B acknowledged as queued", steps.AnswerTimeout, func() (bool, string) {
		n := len(steps.PromptConfirmations(s, work.ID, "Queued."))
		return n > 0, fmt.Sprintf("%d queued notices", n)
	})
	beforeStop := len(s.SUTMessages(work.ID))

	s.StopSUT()
	s.Note("stopped after %d SUT messages in work chat", beforeStop)
	// Both messages must request independent tasks. Ordinary input may steer
	// B if it is still running when the restart admits C.
	steps.MustPost(s, work.ID, "helper queue Sent while helper is down "+tokC)
	time.Sleep(500 * time.Millisecond)

	restartAt := time.Now()
	s.StartSUT()
	s.WaitFor("B and C answered after restart", 60*time.Second, func() (bool, string) {
		state := []string{}
		ok := true
		for _, tok := range []string{tokB, tokC} {
			answered := false
			for _, c := range s.Codex.CallsWithToken(tok) {
				if c.Answer != "" && len(s.SUTMessagesContaining(work.ID, c.Answer)) > 0 {
					answered = true
				}
			}
			state = append(state, fmt.Sprintf("%s=%v", tok, answered))
			ok = ok && answered
		}
		return ok, strings.Join(state, " ")
	})
	s.Settle(3*time.Second, 15*time.Second)

	// Second restart must not replay anything.
	s.StopSUT()
	s.StartSUT()
	if _, ok := s.WaitPolled(work.ID, s.Graph.LastSeq(), 30*time.Second); ok {
		s.Settle(4*time.Second, 15*time.Second)
	}

	callsA, callsB, callsC := s.Codex.CallsWithToken(tokA), s.Codex.CallsWithToken(tokB), s.Codex.CallsWithToken(tokC)
	transcript := s.Transcript(work.ID, 40)
	s.Check("R-29 queued prompt B runs exactly once", len(callsB) == 1, "1 call", fmt.Sprint(len(callsB)), transcript...)
	s.Check("R-4 down-time prompt C runs exactly once", len(callsC) == 1, "1 call", fmt.Sprint(len(callsC)))
	s.Check("R-5b running prompt A is not re-run automatically", len(callsA) == 1, "1 call (the original)", fmt.Sprint(len(callsA)))
	if len(callsB) == 1 && len(callsC) == 1 {
		s.Check("R-6 B runs before C", callsB[0].Seq < callsC[0].Seq, "seq(B) < seq(C)", fmt.Sprintf("%d,%d", callsB[0].Seq, callsC[0].Seq))
		for _, c := range []harness.CodexCall{callsB[0], callsC[0]} {
			n := len(s.SUTMessagesContaining(work.ID, c.Answer))
			s.Check("R-2 answer posted once: "+c.Tokens[0], n == 1, "1", fmt.Sprint(n))
		}
	}
	interrupted := 0
	var notices []string
	for _, m := range s.SUTMessages(work.ID) {
		if m.Created.Before(restartAt.Add(-time.Second)) {
			continue
		}
		if steps.IsStoppedTurnNotice(m.PlainText()) {
			interrupted++
			notices = append(notices, harness.NormalizeSpace(m.PlainText()))
		}
	}
	s.Check("R-33/F-TRN-5 interrupted turn A is reported (once)", interrupted == 1, "1 interruption notice after restart",
		fmt.Sprintf("%d: %q", interrupted, notices), transcript...)
	if len(callsA) > 0 {
		s.Note("A outcome in fake Codex: %s", callsA[0].Outcome)
	}
}
