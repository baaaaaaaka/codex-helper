package lifecycle

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Self-echo breaker (CLEANUP-PLAN §8.3, decisions: pause auto-start only,
// keep reading and holding messages, survive a restart, notify once, never
// interrupt the running turn, lift with `helper unpause`; R-7, R-11, R-22).
//
// The helper's own lifecycle messages come back three times within a few
// seconds without their provenance marker (as if another helper on the same
// account, or a client that strips the HTML comment, re-posted them) while a
// turn is running. Separately, three fast genuine prompts in another chat
// must not trip it.
func TestSelfEchoBreaker(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "BREAKER", harness.Options{
		Guarantees: []string{"F-WRK-14", "R-7", "R-11", "R-22", "R-4"},
		Title:      "3 marker-less helper echoes in 60 s: pause auto-start only, running turn survives, one notice, survives restart, `helper unpause` lifts it",
		Budget:     150 * time.Second,
		Golden:     harness.GoldenOptions{FinishedNotices: "the s002 burst finishes concurrently with the s001 pause notice in the control chat"},
	})
	defer s.Finish()
	running := s.Token("running")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(running) {
			return harness.CodexBehavior{Block: true, Answer: "Final of the running turn " + running}
		}
		return harness.CodexBehavior{}
	})
	control := steps.ReadyControlChat(s)
	work, workDir := steps.NewWorkChat(s, control, "breaker")
	burst, _ := steps.NewWorkChat(s, control, "burst")

	// No false positive: three genuine prompts sent back to back.
	var burstToks []string
	for i := 0; i < 3; i++ {
		tok := s.Token(fmt.Sprintf("burst%d", i))
		burstToks = append(burstToks, tok)
		steps.MustPost(s, burst.ID, fmt.Sprintf("helper queue Quick question %d %s", i, tok))
	}

	steps.MustPost(s, work.ID, "Long task "+running)
	s.Require("running turn reached Codex", steps.AnswerTimeout, func() (bool, string) {
		return len(s.Codex.CallsWithToken(running)) == 1, "not yet"
	})
	if _, ok := s.WaitSUTMessage(work.ID, "Request accepted", 15*time.Second); !ok {
		return
	}
	s.Settle(2*time.Second, 6*time.Second)

	// Echo the helper's three most recent messages in this chat, with the
	// provenance marker stripped. A helper that posted fewer than three
	// helper messages here has its most recent ones echoed again (the
	// trigger is three echoes, not three distinct texts).
	marker := regexp.MustCompile(`<!--.*?-->`)
	var echoed []string
	var helperMsgs []fakegraph.Message
	sut := s.SUTMessages(work.ID)
	for i := len(sut) - 1; i >= 0 && len(helperMsgs) < 3; i-- {
		if strings.Contains(sut[i].Content, "Helper") {
			helperMsgs = append(helperMsgs, sut[i])
		}
	}
	for i := 0; len(helperMsgs) > 0 && i < 3; i++ {
		m := helperMsgs[i%len(helperMsgs)]
		body := marker.ReplaceAllString(m.Content, "")
		if _, err := s.Graph.PostAsOtherHelper(work.ID, body); err != nil {
			t.Fatal(err)
		}
		echoed = append(echoed, clip(harness.NormalizeSpace(m.PlainText()), 60))
	}
	s.Note("echoed %d helper messages: %q", len(echoed), echoed)
	echoSeq := s.Graph.LastSeq()
	tripped := s.Poll(20*time.Second, func() bool { return len(breakerNotices(s, control.ID, 0))+len(breakerNotices(s, work.ID, 0)) > 0 })
	s.Settle(2*time.Second, 8*time.Second)
	runningOutcome := s.Codex.CallsWithToken(running)[0].Outcome

	// While paused: a new prompt is read and held, not run.
	heldTok := s.Token("held")
	heldSeq := s.Graph.LastSeq()
	steps.MustPost(s, work.ID, "Prompt while paused "+heldTok)
	s.Poll(10*time.Second, func() bool { return false })
	readsWhilePaused := len(chatReads(s, work.ID, heldSeq))
	heldRanWhilePaused := len(s.Codex.CallsWithToken(heldTok))

	// The running turn finishes; its final must still be delivered.
	s.Codex.Release(s.Codex.CallsWithToken(running)[0].Seq)
	finalDelivered := s.Poll(15*time.Second, func() bool {
		return len(s.SUTMessagesContaining(work.ID, "Final of the running turn "+running)) > 0
	})

	// The pause survives a restart.
	s.StopSUT()
	s.StartSUT()
	afterRestart := s.Token("afterrestart")
	steps.MustPost(s, work.ID, "Prompt after restart "+afterRestart)
	s.Poll(10*time.Second, func() bool { return len(s.Codex.CallsWithToken(afterRestart)) > 0 })
	ranAfterRestart := len(s.Codex.CallsWithToken(afterRestart))

	// `helper unpause` lifts it: a new prompt runs; held ones stay held.
	unpauseSeq := s.Graph.LastSeq()
	steps.MustPost(s, work.ID, "helper unpause")
	s.Poll(8*time.Second, func() bool { return len(sutAfter(s, work.ID, unpauseSeq)) > 0 })
	afterUnpause := s.Token("afterunpause")
	steps.MustPost(s, work.ID, "Prompt after unpause "+afterUnpause)
	unpaused := s.Poll(15*time.Second, func() bool { return len(s.SUTMessagesContaining(work.ID, "answer for "+afterUnpause)) > 0 })
	s.Settle(2*time.Second, 6*time.Second)

	tr := s.Transcript(work.ID, 25)
	echoTurns := 0
	for _, c := range s.Codex.Calls() {
		if c.Cwd == workDir && len(c.Tokens) == 0 {
			echoTurns++
		}
	}
	s.Check("R-7 echoed helper messages start no turn and no command", echoTurns == 0, "0 turns without a user token", fmt.Sprint(echoTurns), tr...)
	s.Check("the breaker trips on 3 echoes within 60 s", tripped, "a breaker notice", fmt.Sprintf("tripped=%v (echo posted at seq %d)", tripped, echoSeq))
	s.Check("the breaker does not interrupt the running turn", runningOutcome == "running" && finalDelivered,
		"running turn continues and its final is delivered", fmt.Sprintf("outcome right after the trip=%s, final delivered=%v", runningOutcome, finalDelivered))
	nc, nw := len(breakerNotices(s, control.ID, 0)), len(breakerNotices(s, work.ID, 0))
	s.Check("R-22 one breaker notice in the work chat and one in the control chat", nc == 1 && nw == 1, "control=1 work=1",
		fmt.Sprintf("control=%d %q; work=%d %q", nc, breakerNotices(s, control.ID, 0), nw, breakerNotices(s, work.ID, 0)), s.Transcript(control.ID, 10)...)
	s.Check("while paused the chat is still read", readsWhilePaused > 0, ">0 reads of the work chat in 10 s", fmt.Sprint(readsWhilePaused))
	s.Check("while paused a new prompt is held, not run", heldRanWhilePaused == 0, "0 turns", fmt.Sprint(heldRanWhilePaused))
	s.Check("the pause survives a restart", ranAfterRestart == 0, "0 turns after restart", fmt.Sprint(ranAfterRestart))
	s.Check("`helper unpause` lifts the breaker", unpaused, "next prompt answered", fmt.Sprintf("answered=%v; replies to unpause: %q", unpaused, sutAfter(s, work.ID, unpauseSeq)))
	heldAfter := len(s.Codex.CallsWithToken(heldTok)) + len(s.Codex.CallsWithToken(afterRestart))
	s.Check("held prompts are not run automatically after unpause", heldAfter == 0, "0 turns for held prompts", fmt.Sprint(heldAfter))

	burstRan, burstNotices := 0, len(breakerNotices(s, burst.ID, 0))
	for _, tok := range burstToks {
		if len(s.Codex.CallsWithToken(tok)) == 1 {
			burstRan++
		}
	}
	s.Check("three fast genuine prompts do not trip the breaker", burstRan == 3 && burstNotices == 0, "3 turns, 0 breaker notices",
		fmt.Sprintf("%d turns, %d notices", burstRan, burstNotices), s.Transcript(burst.ID, 15)...)
}

// breakerNotices returns SUT messages in chatID after seq that report the
// chat as quarantined or paused.
func breakerNotices(s *harness.Scenario, chatID string, since int64) []string {
	var out []string
	for _, m := range s.SUTMessages(chatID) {
		if m.RequestSeq <= since {
			continue
		}
		// The control chat's and the work chat's notices both open with it.
		if strings.Contains(m.PlainText(), "Paused auto-start") {
			out = append(out, clip(harness.NormalizeSpace(m.PlainText()), 140))
		}
	}
	return out
}

func sutAfter(s *harness.Scenario, chatID string, since int64) []string {
	var out []string
	for _, m := range s.SUTMessages(chatID) {
		if m.RequestSeq > since {
			out = append(out, clip(harness.NormalizeSpace(m.PlainText()), 120))
		}
	}
	return out
}

func clip(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}
