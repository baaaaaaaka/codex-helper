package turns

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// command posts text and waits for a new helper reply containing sub; it
// returns the reply's plain text (whitespace-normalized).
func command(s *harness.Scenario, chat, text, sub string) (string, bool) {
	seq := s.Graph.LastSeq()
	steps.MustPost(s, chat, text)
	var got string
	ok := s.WaitFor(fmt.Sprintf("reply to %q containing %q", text, sub), steps.CommandTimeout, func() (bool, string) {
		for _, m := range s.SUTMessages(chat) {
			if m.RequestSeq > seq && strings.Contains(harness.NormalizeSpace(m.PlainText()), harness.NormalizeSpace(sub)) {
				got = strings.TrimPrefix(harness.NormalizeSpace(m.PlainText()), "🔧 Helper: ")
				return true, ""
			}
		}
		return false, strings.Join(s.Transcript(chat, 4), " | ")
	})
	return got, ok
}

var turnRefRE = regexp.MustCompile("turn-(\\d+)")

// turnNum is the number of the first `turn-N` in text (0 when none).
func turnNum(text string) int {
	m := turnRefRE.FindStringSubmatch(text)
	if m == nil {
		return 0
	}
	n, _ := strconv.Atoi(m[1])
	return n
}

// F-TRN-3: `helper cancel` with every selector (v0.1.20
// cancelTurnCommandForScope). One running and three queued prompts:
//
//	cancel last         -> the newest queued prompt, named; the others listed
//	cancel turn-<id>    -> that queued prompt
//	cancel turn-<id>    -> again: "is canceled and cannot be canceled"
//	cancel turn-999999  -> "turn not found in this session"
//	cancel queued       -> the last queued prompt; nothing else queued
//	cancel queued       -> "no queued turn is available"
//	cancel all          -> the running prompt; its turn ends "Codex request canceled."
//	cancel              -> "Nothing to cancel" + usage
//
// No cancelled prompt ever reaches Codex; each command gets one reply.
func TestCancelCommands(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CANCEL-CMDS", harness.Options{
		Guarantees: []string{"F-TRN-3", "R-20"},
		Title:      "`helper cancel last|queued|running|all|<turn-id>`: named prompts, refusals, one reply each, cancelled prompts never run",
		Budget:     120 * time.Second,
	})
	defer s.Finish()
	run, q1, q2, q3, next := s.Token("run"), s.Token("q1"), s.Token("q2"), s.Token("q3"), s.Token("next")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(run) {
			return harness.CodexBehavior{Block: true, Answer: "LATE-ANSWER-SHOULD-NOT-APPEAR " + run}
		}
		return harness.CodexBehavior{}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "cancel-cmds")

	steps.MustPost(s, work.ID, "Running job "+run)
	s.WaitCodexCalls(run, 1, steps.AnswerTimeout)
	for _, q := range []string{q1, q2, q3} {
		seq := s.Graph.LastSeq()
		steps.MustPost(s, work.ID, "Queued job "+q)
		s.WaitFor("queued ACK for "+q, steps.CommandTimeout, func() (bool, string) {
			for _, m := range s.SUTMessages(work.ID) {
				if m.RequestSeq > seq {
					return true, ""
				}
			}
			return false, "no reply"
		})
	}

	last, okLast := command(s, work.ID, "helper cancel last", "turn canceled:")
	n3 := turnNum(last)
	s.Check("F-TRN-3 `cancel last` cancels the newest queued prompt, names it and lists what is still queued",
		okLast && n3 > 0 && strings.Contains(last, "Canceled prompt: Queued job "+q3) && strings.Contains(last, "Still queued: Queued job "+q1+" Queued job "+q2),
		"names "+q3+", still queued "+q1+", "+q2, last)
	n2 := n3 - 1
	byID, okID := command(s, work.ID, fmt.Sprintf("helper cancel turn-%d", n2), "turn canceled:")
	s.Check("F-TRN-3 `cancel <turn-id>` cancels that queued prompt", okID && strings.Contains(byID, fmt.Sprintf("turn-%d", n2)) && strings.Contains(byID, "Canceled prompt: Queued job "+q2),
		fmt.Sprintf("turn-%d, prompt %s", n2, q2), byID)
	again, okAgain := command(s, work.ID, fmt.Sprintf("!cancel turn-%d", n2), "cannot be canceled")
	s.Check("F-TRN-3 cancelling a cancelled turn is refused with its state", okAgain && again == fmt.Sprintf("turn turn-%d is canceled and cannot be canceled.", n2),
		fmt.Sprintf("turn turn-%d is canceled and cannot be canceled.", n2), again)
	missing, okMissing := command(s, work.ID, "helper cancel turn-999999", "turn not found")
	s.Check("F-TRN-3 an unknown turn id is refused", okMissing && missing == "turn not found in this session: turn-999999", "turn not found in this session: turn-999999", missing)
	queued, okQueued := command(s, work.ID, "helper cancel queued", "turn canceled:")
	s.Check("F-TRN-3 `cancel queued` cancels the remaining queued prompt; nothing else is queued",
		okQueued && strings.Contains(queued, "Canceled prompt: Queued job "+q1) && strings.Contains(queued, "No other prompts are queued."), q1+"; none queued", queued)
	none, okNone := command(s, work.ID, "helper cancel queued", "no queued turn")
	s.Check("F-TRN-3 `cancel queued` with nothing queued says so", okNone && none == "no queued turn is available to cancel in this session.", "no queued turn …", none)
	all, okAll := command(s, work.ID, "helper cancel all", "cancel all requested")
	s.Check("F-TRN-3 `cancel all` names the running prompt",
		okAll && strings.Contains(all, "Running requests cancel requested: Running job "+run) && strings.Contains(all, "No queued prompts remain."), "running "+run, all)
	_, finished := s.WaitSUTMessage(work.ID, "Codex request canceled.", 30*time.Second)
	s.Codex.ReleaseAll()
	s.Settle(2*time.Second, 10*time.Second)
	bare, okBare := command(s, work.ID, "helper cancel", "Nothing to cancel")
	s.Check("F-TRN-3 bare `helper cancel` with nothing to cancel explains and shows the usage",
		okBare && strings.HasPrefix(bare, "Nothing to cancel: no request is running or queued.") && strings.Contains(bare, "helper cancel <turn-id>"), "Nothing to cancel + usage", bare)
	steps.MustPost(s, work.ID, "Next request "+next)
	_, answered := steps.WaitAnswered(s, work.ID, next, steps.AnswerTimeout)
	s.Settle(3*time.Second, 15*time.Second)

	tr := s.Transcript(work.ID, 40)
	s.Check("R-20 the running turn ends once as cancelled, with no answer", finished && len(s.SUTMessagesContaining(work.ID, "Codex request canceled.")) == 1 && len(s.SUTMessagesContaining(work.ID, "LATE-ANSWER")) == 0,
		"1 cancelled notice, no answer", fmt.Sprintf("cancelled x%d, late x%d", len(s.SUTMessagesContaining(work.ID, "Codex request canceled.")), len(s.SUTMessagesContaining(work.ID, "LATE-ANSWER"))), tr...)
	var ran []string
	for _, q := range []string{q1, q2, q3} {
		if n := len(s.Codex.CallsWithToken(q)); n != 0 {
			ran = append(ran, fmt.Sprintf("%s x%d", q, n))
		}
	}
	s.Check("F-TRN-3 no cancelled prompt reaches Codex", len(ran) == 0, "none", strings.Join(ran, ", "), tr...)
	for _, sub := range []string{"Canceled prompt: Queued job " + q3, "Canceled prompt: Queued job " + q2, "Canceled prompt: Queued job " + q1, "cancel all requested"} {
		n := len(s.SUTMessagesContaining(work.ID, sub))
		s.Check("F-TRN-3 one reply per command: "+sub, n == 1, "1", fmt.Sprint(n))
	}
	s.Check("the next message still works", answered && len(s.Codex.CallsWithToken(next)) == 1, "answered once", fmt.Sprint(answered), tr...)
}

// F-TRN-4 / F-TRN-5: `helper retry` (v0.1.20 retryTurnCommandForScope).
//
//	a prompt fails in Codex       -> one failure notice naming `helper retry last`
//	helper retry last             -> "Running `turn-A` again as `turn-B`", answered once
//	helper retry last             -> nothing left to retry
//	helper retry turn-A           -> already run again as `turn-B`
//	helper retry turn-B           -> completed; only failed or interrupted turns
//	helper retry turn-999999      -> not found
//	helper retry                  -> usage
//	a prompt lost with the helper -> one interruption notice; `helper retry last` runs it once
//
// A retry never posts a second final for a prompt that was answered.
func TestRetryCommands(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "RETRY-CMDS", harness.Options{
		Guarantees: []string{"F-TRN-4", "F-TRN-5"},
		Title:      "`helper retry last|<turn-id>`: failed and interrupted prompts run again once; refusals for answered, retried and unknown turns",
		Budget:     150 * time.Second,
	})
	defer s.Finish()
	fail, lost := s.Token("fail"), s.Token("lost")
	var failCalls, lostCalls atomic.Int32
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		switch {
		case c.HasToken(fail):
			if failCalls.Add(1) == 1 {
				return harness.CodexBehavior{Err: errors.New("scripted model failure")}
			}
		case c.HasToken(lost):
			if lostCalls.Add(1) == 1 {
				return harness.CodexBehavior{Block: true}
			}
		}
		return harness.CodexBehavior{}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "retry-cmds")

	steps.MustPost(s, work.ID, "Flaky job "+fail)
	failure, okFail := s.WaitSUTMessage(work.ID, "the request did not finish", steps.AnswerTimeout)
	failText := harness.NormalizeSpace(failure.PlainText())
	s.Check("F-TRN-4 a failed prompt gets one notice naming `helper retry last` and is not re-run by itself",
		okFail && strings.Contains(failText, "helper retry last") && len(s.Codex.CallsWithToken(fail)) == 1, "notice with `helper retry last`; 1 call", failText)

	retried, okRetry := command(s, work.ID, "helper retry last", "again as")
	a, b := 0, 0
	if m := turnRefRE.FindAllStringSubmatch(retried, -1); len(m) >= 2 {
		a, _ = strconv.Atoi(m[0][1])
		b, _ = strconv.Atoi(m[1][1])
	}
	s.Check("F-TRN-4 `retry last` names the failed turn, the new turn and the prompt",
		okRetry && a > 0 && b > a && strings.Contains(retried, "Prompt: Flaky job "+fail), "Running `turn-A` again as `turn-B`. Prompt: …", retried)
	_, answered := steps.WaitAnswered(s, work.ID, fail, steps.AnswerTimeout)
	s.Settle(2*time.Second, 10*time.Second)

	nothing, okNothing := command(s, work.ID, "helper retry last", "no failed or interrupted turn")
	s.Check("F-TRN-4 nothing is left to retry once the retry was answered", okNothing && nothing == "no failed or interrupted turn is available to retry in this session.", "no failed or interrupted turn …", nothing)
	twice, okTwice := command(s, work.ID, fmt.Sprintf("helper retry turn-%d", a), "already run again")
	s.Check("F-TRN-4 a turn already run again is refused, naming its run", okTwice && strings.Contains(twice, fmt.Sprintf("was already run again as turn-%d", b)), fmt.Sprintf("already run again as turn-%d", b), twice)
	done, okDone := command(s, work.ID, fmt.Sprintf("!retry turn-%d", b), "only failed or interrupted")
	s.Check("F-TRN-4 a completed turn is refused with its state", okDone && done == fmt.Sprintf("turn turn-%d is completed; only failed or interrupted turns can be retried.", b), "turn … is completed; …", done)
	missing, okMissing := command(s, work.ID, "helper retry turn-999999", "turn not found")
	s.Check("F-TRN-4 an unknown turn id is refused", okMissing && missing == "turn not found in this session: turn-999999", "turn not found …", missing)
	usage, okUsage := command(s, work.ID, "helper retry", "usage:")
	s.Check("F-TRN-4 bare `helper retry` with no ignored duplicate shows the usage", okUsage && strings.Contains(usage, "helper retry last"), "usage", usage)

	steps.MustPost(s, work.ID, "Long job "+lost)
	s.WaitCodexCalls(lost, 1, steps.AnswerTimeout)
	s.Settle(2*time.Second, 8*time.Second)
	s.CrashSUT()
	s.Require("Codex lost the turn with the helper", 15*time.Second, func() (bool, string) {
		c := s.Codex.CallsWithToken(lost)[0]
		return c.Outcome != "running", c.Outcome
	})
	s.StartSUT()
	notice, okNotice := s.WaitSUTMessage(work.ID, "Codex request interrupted", 45*time.Second)
	noticeText := harness.NormalizeSpace(notice.PlainText())
	s.Check("F-TRN-5 the interrupted prompt gets one notice naming `helper retry last`", okNotice && strings.Contains(noticeText, "helper retry last") && !strings.Contains(noticeText, "resend it."),
		"interruption notice with `helper retry last`", noticeText)
	s.Settle(2*time.Second, 10*time.Second)
	_, okLost := command(s, work.ID, "helper retry last", "Prompt: Long job "+lost)
	_, answeredLost := steps.WaitAnswered(s, work.ID, lost, steps.AnswerTimeout)
	s.Settle(3*time.Second, 15*time.Second)

	tr := s.Transcript(work.ID, 40)
	s.Check("F-TRN-4 the failed prompt is answered exactly once after the retry", answered && len(s.Codex.CallsWithToken(fail)) == 2 && len(steps.AnswerPosts(s, work.ID, fail)) == 1,
		"2 Codex calls, 1 answer", fmt.Sprintf("calls=%d answers=%d", len(s.Codex.CallsWithToken(fail)), len(steps.AnswerPosts(s, work.ID, fail))), tr...)
	s.Check("F-TRN-5 `retry last` after an interruption runs the prompt once more, answered once",
		okLost && answeredLost && len(s.Codex.CallsWithToken(lost)) == 2 && len(steps.AnswerPosts(s, work.ID, lost)) == 1 && len(s.SUTMessagesContaining(work.ID, "Codex request interrupted")) == 1,
		"2 Codex calls, 1 answer, 1 notice", fmt.Sprintf("calls=%d answers=%d notices=%d", len(s.Codex.CallsWithToken(lost)), len(steps.AnswerPosts(s, work.ID, lost)), len(s.SUTMessagesContaining(work.ID, "Codex request interrupted"))), tr...)
}
