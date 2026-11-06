package commands

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// F-WRK-10 / F-CTL-13 / R-41 (rc.6 parity): `helper fork` is accepted
// while a turn runs. The fork is cut at the last completed turn: the fork
// chat gets the history up to it (not the running prompt), then the
// marker; the original chat gets "Fork requested" and then exactly one
// "Fork complete" with the new chat's link, after the marker; the running
// turn finishes in the original chat only, and the fork works on its own
// thread.
func TestForkWhileRunning(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "FORK-RUNNING", harness.Options{
		Guarantees: []string{"F-WRK-10", "F-CTL-13", "R-41"},
		Title:      "`helper fork` while a turn runs: requested, then one completion with the link; the running turn stays in the original",
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
	work, _ := steps.NewWorkChat(s, control, "forkrun")
	first, ok := ask(s, work.ID, "before")
	s.Check("the original session answers", ok && first.ThreadID != "", "answered on a thread", fmt.Sprintf("ok=%v thread=%q", ok, first.ThreadID))
	s.WaitSUTMessage(control.ID, "Codex finished", steps.CommandTimeout)

	steps.MustPost(s, work.ID, "Long job "+running)
	s.WaitCodexCalls(running, 1, steps.AnswerTimeout)
	before := map[string]bool{}
	for _, c := range s.Graph.Chats() {
		before[c.ID] = true
	}
	forkSeq := s.Graph.LastSeq()
	reply, replied := command(s, work.ID, "helper fork", "Fork requested")
	var fork fakegraph.ChatInfo
	created := s.WaitFor("fork chat created", steps.CommandTimeout, func() (bool, string) {
		for _, c := range s.Graph.Chats() {
			if !before[c.ID] && !strings.Contains(c.Topic, harness.ControlChatTopicMarker) {
				fork = c
				return true, ""
			}
		}
		return false, fmt.Sprintf("%d chats", len(s.Graph.Chats()))
	})
	done, doneOK := replyAfter(s, work.ID, forkSeq, "Fork complete", steps.CommandTimeout)
	s.Check("F-WRK-10 `helper fork` is accepted while a turn runs", replied && strings.HasPrefix(strings.TrimPrefix(harness.NormalizeSpace(reply), "🔧 Helper: "), "⏳ Fork requested."),
		"⏳ Fork requested. …", clip(reply, 160))
	s.Check("F-WRK-10 the fork creates exactly one new chat", created, "one new chat", fork.Topic)
	s.Check("F-CTL-13 the original chat gets the completion with the new chat's link", doneOK && created && strings.Contains(done, "https://teams.microsoft.com/l/chat/"+url.QueryEscape(fork.ID)+"/"),
		"✅ Fork complete … <fork url>", clip(done, 200))
	if !created {
		return
	}
	s.WaitSUTMessage(fork.ID, "Forked from", steps.CommandTimeout)
	s.WaitSUTMessage(control.ID, "Work chat created: s002", steps.CommandTimeout)
	if len(first.Tokens) > 0 {
		forkHistoryOnce(s, fork.ID, work.ID, control.ID, "s002", "Please answer "+first.Tokens[0], first.Answer)
	}
	n := len(helperSeqs(s, fork.ID, running))
	s.Check("F-WRK-10 the running turn is not in the fork's history", n == 0, "0", fmt.Sprint(n), s.Transcript(fork.ID, 6)...)
	s.WaitPolled(fork.ID, forkSeq, steps.CommandTimeout)
	inFork, okFork := ask(s, fork.ID, "infork")
	s.Check("R-41 a prompt in the fork runs on a new thread", okFork && inFork.ThreadID != "" && inFork.ThreadID != first.ThreadID,
		"thread != original", fmt.Sprintf("ok=%v fork=%q original=%q", okFork, inFork.ThreadID, first.ThreadID))

	s.Codex.ReleaseAll()
	_, answered := steps.WaitAnswered(s, work.ID, running, steps.AnswerTimeout)
	s.Settle(3*time.Second, 15*time.Second)
	s.Check("R-41 the running turn finishes once in the original chat and not in the fork",
		answered && len(steps.AnswerPosts(s, work.ID, running)) == 1 && len(steps.AnswerPosts(s, fork.ID, running)) == 0 && len(s.Codex.CallsWithToken(running)) == 1,
		"1 answer in the original, 0 in the fork, 1 Codex call",
		fmt.Sprintf("original=%d fork=%d calls=%d", len(steps.AnswerPosts(s, work.ID, running)), len(steps.AnswerPosts(s, fork.ID, running)), len(s.Codex.CallsWithToken(running))),
		s.Transcript(work.ID, 12)...)
	n = len(s.SUTMessagesContaining(work.ID, "Fork complete"))
	s.Check("F-CTL-13 exactly one completion notice", n == 1, "1", fmt.Sprint(n))
	leaks := steps.InternalLeaks(s, reply)
	s.Check("the fork request reply carries no internal ids or paths", len(leaks) == 0, "none", fmt.Sprint(leaks))
}
