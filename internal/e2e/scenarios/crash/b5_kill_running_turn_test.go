package crash

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// B-5 (CLEANUP-PLAN §8.1), R-5b, R-32, R-33: kill -9 of the helper while
// turns are in three different states, then restart. Real Codex drops a
// turn when its client dies (FIDELITY C4), and so does the fake by default:
//
//	chat X: turn running and already streaming progress (commentary)
//	                                  -> interrupted: exactly one
//	                                     interruption notice at restart
//	                                     recovery, before the next turn
//	chat Y: turn running, no output yet
//	                                  -> the same
//	chat Z: helper dies while turn/start is in flight; Codex never records
//	        the turn                  -> one "not executed" notice, no re-run
//
// Then one new prompt per chat must be answered and continue the same Codex
// thread. The scenario also records whether the helper gives Codex a Teams
// turn key (turn/start clientUserMessageId, or a Teams id in the input)
// that would let it match turns after a restart.
//
// (Until M3 chat X used the fake's SurviveHelper knob, a Codex that
// finishes the turn after its client died; real Codex never does, so no
// scenario depends on it.)
func TestB5_KillDuringRunningTurn(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "B-5", harness.Options{
		Guarantees: []string{"B-5", "R-5b", "R-32", "R-33", "R-1", "R-2"},
		Title:      "kill -9 with turns running (with and without progress) / not started; restart reports each once and keeps the thread",
		Budget:     150 * time.Second,
	})
	defer s.Finish()
	tokX, tokY, tokZ := s.Token("streaming"), s.Token("lost"), s.Token("notstarted")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		switch {
		case c.HasToken(tokX):
			return harness.CodexBehavior{Block: true, Commentary: "Looking at the files now."}
		case c.HasToken(tokY):
			return harness.CodexBehavior{Block: true}
		case c.HasToken(tokZ):
			return harness.CodexBehavior{HoldStart: true}
		}
		return harness.CodexBehavior{}
	})

	control := steps.ReadyControlChat(s)
	chatX, _ := steps.NewWorkChat(s, control, "b5-x")
	chatY, _ := steps.NewWorkChat(s, control, "b5-y")
	chatZ, _ := steps.NewWorkChat(s, control, "b5-z")
	steps.MustPost(s, chatX.ID, "Long task "+tokX)
	steps.MustPost(s, chatY.ID, "Long task "+tokY)
	steps.MustPost(s, chatZ.ID, "Long task "+tokZ)
	s.Require("all three turns reached Codex", steps.AnswerTimeout, func() (bool, string) {
		n := 0
		for _, tok := range []string{tokX, tokY, tokZ} {
			if len(s.Codex.CallsWithToken(tok)) > 0 {
				n++
			}
		}
		return n == 3, fmt.Sprintf("%d/3", n)
	})

	s.Settle(2*time.Second, 8*time.Second) // X's progress reaches the chat
	s.CrashSUT()
	killedAt := time.Now()
	s.Codex.Release(s.Codex.CallsWithToken(tokZ)[0].Seq)
	s.Require("Codex lost X and Y and never started Z", 15*time.Second, func() (bool, string) {
		x, y, z := s.Codex.CallsWithToken(tokX)[0], s.Codex.CallsWithToken(tokY)[0], s.Codex.CallsWithToken(tokZ)[0]
		return x.Outcome != "running" && y.Outcome != "running" && z.Outcome != "running", "X=" + x.Outcome + " Y=" + y.Outcome + " Z=" + z.Outcome
	})
	s.StartSUT()

	// R-33 / B-5: each chat reaches a visible terminal state for its turn.
	s.WaitFor("each chat shows the outcome of its killed turn", 45*time.Second, func() (bool, string) {
		x := len(noticesAfter(s, chatX.ID, killedAt)) > 0
		y := len(noticesAfter(s, chatY.ID, killedAt)) > 0
		z := len(noticesAfter(s, chatZ.ID, killedAt)) > 0
		return x && y && z, fmt.Sprintf("x=%v y=%v z=%v", x, y, z)
	})
	s.Settle(3*time.Second, 15*time.Second)

	// Next prompt in every chat.
	next := map[string]string{chatX.ID: s.Token("nextx"), chatY.ID: s.Token("nexty"), chatZ.ID: s.Token("nextz")}
	for chat, tok := range next {
		steps.MustPost(s, chat, "Next request "+tok)
	}
	for chat, tok := range next {
		steps.WaitAnswered(s, chat, tok, 45*time.Second)
	}
	s.Settle(3*time.Second, 15*time.Second)

	// --- verdicts --------------------------------------------------------
	trX, trY, trZ := s.Transcript(chatX.ID, 25), s.Transcript(chatY.ID, 25), s.Transcript(chatZ.ID, 25)
	for _, c := range []struct {
		name, chat string
		tr         []string
	}{{"X", chatX.ID, trX}, {"Y", chatY.ID, trY}} {
		notices := noticesAfter(s, c.chat, killedAt)
		s.Check("B-5/R-33 interrupted turn "+c.name+" gets exactly one notice", len(notices) == 1, "1 notice", fmt.Sprintf("%d: %q", len(notices), notices), c.tr...)
		if callsNext := s.Codex.CallsWithToken(next[c.chat]); len(callsNext) > 0 && len(notices) > 0 {
			first := firstNoticeTime(s, c.chat, killedAt)
			s.Check("B-5 interruption notice in "+c.name+" precedes the next turn", first.Before(callsNext[0].Started), "notice before next turn/start",
				fmt.Sprintf("notice %s, next turn started %s", first.Format("15:04:05.000"), callsNext[0].Started.Format("15:04:05.000")))
		}
	}
	for _, c := range []struct{ name, chat string }{{"X", chatX.ID}, {"Y", chatY.ID}, {"Z", chatZ.ID}} {
		n := len(s.SUTMessagesContaining(c.chat, "answer for "+next[c.chat]))
		s.Check("B-5 the next prompt in "+c.name+" is answered once", n == 1, "1 answer", fmt.Sprint(n))
	}

	noticesZ := noticesAfter(s, chatZ.ID, killedAt)
	s.Check("B-5 turn that never started gets one notice", len(noticesZ) == 1, "1 notice (not executed, resend)", fmt.Sprintf("%d: %q", len(noticesZ), noticesZ), trZ...)
	for name, tok := range map[string]string{"X": tokX, "Y": tokY, "Z": tokZ} {
		n := len(s.Codex.CallsWithToken(tok))
		s.Check("R-5b turn "+name+" is not re-run automatically", n == 1, "1 Codex call", fmt.Sprint(n))
	}

	for _, c := range []struct {
		name, chat, orig string
	}{{"X", chatX.ID, tokX}, {"Y", chatY.ID, tokY}, {"Z", chatZ.ID, tokZ}} {
		orig, nxt := s.Codex.CallsWithToken(c.orig), s.Codex.CallsWithToken(next[c.chat])
		if len(orig) == 0 || len(nxt) == 0 {
			s.Check("R-32 chat "+c.name+" continues the same Codex thread", false, "next turn on the same thread", "next prompt never reached Codex")
			continue
		}
		s.Check("R-32 chat "+c.name+" continues the same Codex thread", orig[0].ThreadID == nxt[0].ThreadID,
			"thread "+orig[0].ThreadID, "thread "+nxt[0].ThreadID+" (new thread announced: "+fmt.Sprint(announcedNewThread(s, c.chat, killedAt))+")")
	}

	keyed := 0
	var sample harness.CodexCall
	for _, c := range s.Codex.Calls() {
		if c.Kind != "work" {
			continue
		}
		sample = c
		if c.ClientID != "" {
			keyed++
		}
	}
	work := 0
	for _, c := range s.Codex.Calls() {
		if c.Kind == "work" {
			work++
		}
	}
	s.Check("B-5 every Teams turn carries a Teams turn key to Codex (clientUserMessageId)", work > 0 && keyed == work,
		"clientUserMessageId on every turn/start", fmt.Sprintf("%d of %d turn/start calls carried one; prompt contains a Teams message/turn id: %v", keyed, work, promptHasTeamsID(sample)))
	s.Note("Codex outcomes after kill: X=%s Y=%s Z=%s", s.Codex.CallsWithToken(tokX)[0].Outcome, s.Codex.CallsWithToken(tokY)[0].Outcome, s.Codex.CallsWithToken(tokZ)[0].Outcome)
}

// noticesAfter returns SUT messages in chatID created after t that report an
// interrupted/unfinished turn or a restart.
func noticesAfter(s *harness.Scenario, chatID string, t time.Time) []string {
	var out []string
	for _, m := range s.SUTMessages(chatID) {
		if m.Created.Before(t) {
			continue
		}
		if isNotice(m) {
			out = append(out, clip(harness.NormalizeSpace(m.PlainText()), 140))
		}
	}
	return out
}

func isNotice(m fakegraph.Message) bool {
	low := strings.ToLower(m.PlainText())
	for _, k := range []string{"interrupt", "restart", "unconfirmed", "not executed", "did not run", "resend", "needs attention"} {
		if strings.Contains(low, k) {
			return true
		}
	}
	return false
}

func firstNoticeTime(s *harness.Scenario, chatID string, t time.Time) time.Time {
	for _, m := range s.SUTMessages(chatID) {
		if !m.Created.Before(t) && isNotice(m) {
			return m.Created
		}
	}
	return time.Time{}
}

func announcedNewThread(s *harness.Scenario, chatID string, t time.Time) bool {
	for _, m := range s.SUTMessages(chatID) {
		low := strings.ToLower(m.PlainText())
		if !m.Created.Before(t) && (strings.Contains(low, "new conversation") || strings.Contains(low, "new thread") || strings.Contains(low, "new codex thread")) {
			return true
		}
	}
	return false
}

func promptHasTeamsID(c harness.CodexCall) bool {
	return strings.Contains(c.Prompt, "19:") || strings.Contains(c.Prompt, "turn:inbound")
}

func clip(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}
