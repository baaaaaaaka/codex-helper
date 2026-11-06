package commands

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// replyAfter waits for the first SUT message in chatID after seq that
// contains sub (case-insensitive) and returns its plain text.
func replyAfter(s *harness.Scenario, chatID string, seq int64, sub string, timeout time.Duration) (string, bool) {
	var got string
	ok := s.WaitFor(fmt.Sprintf("reply containing %q", sub), timeout, func() (bool, string) {
		var last string
		for _, m := range s.SUTMessages(chatID) {
			if m.RequestSeq <= seq {
				continue
			}
			last = m.PlainText()
			if strings.Contains(strings.ToLower(last), strings.ToLower(sub)) {
				got = last
				return true, ""
			}
		}
		return false, "last reply: " + clip(last, 160)
	})
	return got, ok
}

// command posts text in chatID and waits for a reply containing sub.
func command(s *harness.Scenario, chatID, text, sub string) (string, bool) {
	seq := s.Graph.LastSeq()
	steps.MustPost(s, chatID, text)
	return replyAfter(s, chatID, seq, sub, steps.CommandTimeout)
}

func clip(s string, n int) string {
	s = harness.NormalizeSpace(s)
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

func ask(s *harness.Scenario, chatID, label string) (harness.CodexCall, bool) {
	token := s.Token(label)
	steps.MustPost(s, chatID, "Please answer "+token)
	_, ok := steps.WaitAnswered(s, chatID, token, steps.AnswerTimeout)
	calls := s.Codex.CallsWithToken(token)
	if len(calls) != 1 {
		return harness.CodexCall{}, false
	}
	return calls[0], ok
}

// askNotified is ask for a Work chat whose owner notice in the Control
// chat must land before the scenario posts there again: the notice is
// asynchronous, so without the wait its place in the Control chat's
// transcript (and in a later control question's history) would race.
func askNotified(s *harness.Scenario, controlID, chatID, label string) (harness.CodexCall, bool) {
	token := s.Token(label)
	steps.MustPost(s, chatID, "Please answer "+token)
	_, ok := steps.WaitAnswered(s, chatID, token, steps.AnswerTimeout)
	if _, seen := s.WaitSUTMessage(controlID, "Please answer "+token, steps.CommandTimeout); !seen {
		ok = false
	}
	calls := s.Codex.CallsWithToken(token)
	if len(calls) != 1 {
		return harness.CodexCall{}, false
	}
	return calls[0], ok
}

// `new` then `helper fork`: the fork gets its own chat and a new Codex
// thread forked from the original; both chats keep working on their own
// threads, and the fork does not replay the original's history.
func TestNewAndFork(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CMD-FORK", harness.Options{
		Guarantees: []string{"F-CTL-4", "cmd-fork"},
		Title:      "`new` creates a session; `helper fork` copies it into a new chat on a forked thread",
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "fork")
	first, ok := ask(s, work.ID, "before")
	s.Check("the original session answers", ok && first.ThreadID != "", "answered on a thread", fmt.Sprintf("ok=%v thread=%q", ok, first.ThreadID))
	// The owner's "Codex finished" notice comes before the fork's
	// announcement in the control chat (a fixed transcript).
	s.WaitSUTMessage(control.ID, "Codex finished", steps.CommandTimeout)

	before := map[string]bool{}
	for _, c := range s.Graph.Chats() {
		before[c.ID] = true
	}
	forkSeq := s.Graph.LastSeq()
	reply, replied := command(s, work.ID, "helper fork", "fork")
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
	s.Check("`helper fork` is answered in the original chat", replied, "a reply mentioning the fork", clip(reply, 120))
	s.Check("`helper fork` creates exactly one new chat", created, "one new chat", fork.Topic)
	if !created {
		return
	}
	intro, introOK := s.WaitSUTMessage(fork.ID, "Forked from", steps.CommandTimeout)
	s.Check("the fork chat says what it is", introOK, "\"Forked from …\"", clip(intro.PlainText(), 120))
	s.WaitPolled(fork.ID, forkSeq, steps.CommandTimeout) // the SUT reads the new chat before the prompt

	inFork, okFork := ask(s, fork.ID, "infork")
	again, okAgain := ask(s, work.ID, "after")
	s.Check("a prompt in the fork runs on a new thread", okFork && inFork.ThreadID != "" && inFork.ThreadID != first.ThreadID,
		"thread != original", fmt.Sprintf("ok=%v fork=%q original=%q", okFork, inFork.ThreadID, first.ThreadID))
	s.Check("the original chat continues on its own thread", okAgain && again.ThreadID == first.ThreadID,
		"same thread as before", fmt.Sprintf("ok=%v now=%q before=%q", okAgain, again.ThreadID, first.ThreadID))
	s.Settle(2*time.Second, 8*time.Second)
	replayed := 0
	if first.Answer != "" {
		replayed = len(s.SUTMessagesContaining(fork.ID, first.Answer))
	}
	s.Check("the fork does not replay the original's history", replayed == 0, "0 copies of the original answer", fmt.Sprint(replayed), s.Transcript(fork.ID, 8)...)
	leaks := steps.InternalLeaks(s, intro.PlainText()+" "+reply)
	s.Check("fork replies carry no internal ids or paths", len(leaks) == 0, "none", fmt.Sprint(leaks))
}

// `model …` and `effort …` are in model_cmds_test.go (gap batch B).

// `helper stats` and control `stats`: token usage computed from the Codex
// rollouts, in v0.1.20's layout (F-STA-1).
func TestStats(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CMD-STATS", harness.Options{
		Guarantees: []string{"cmd-stats"},
		Title:      "`helper stats` / `stats` report token usage from the Codex rollouts",
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "stats")
	_, ok1 := ask(s, work.ID, "one")
	c2, ok2 := ask(s, work.ID, "two")
	s.Check("two requests answered", ok1 && ok2, "2 answers", fmt.Sprintf("%v %v", ok1, ok2))
	// The fake Codex's rollout records 1,280 tokens per turn (1,200 input,
	// 80 output of which 20 reasoning) under gpt-5-codex / medium, without
	// a service tier.
	st, ok := command(s, work.ID, "helper stats", "STATS: Codex tokens")
	s.Check("`helper stats` names the session, its Codex thread and latest request",
		ok && strings.Contains(st, "Session: s001") && strings.Contains(st, "Codex thread: "+c2.ThreadID) && strings.Contains(st, "Latest request: completed"),
		"Session: s001, Codex thread, Latest request: completed", clip(st, 300))
	s.Check("`helper stats` reports the conversation total from the rollout", strings.Contains(st, "2,560") && strings.Contains(st, "2,400") && strings.Contains(st, "reasoning 40"),
		"2,560 total, 2,400 input, reasoning 40", clip(st, 300))
	s.Check("`helper stats` shows model, effort and the unknown tier", strings.Contains(st, "Model: gpt-5-codex") && strings.Contains(st, "Effort: medium") && strings.Contains(st, "Tier: unknown"),
		"Model: gpt-5-codex, Effort: medium, Tier: unknown", clip(st, 300))
	all, ok := command(s, control.ID, "stats", "Scope: all Work chats")
	s.Check("control `stats` sums the Work chats", ok && strings.Contains(all, "2,560") && strings.Contains(all, "s001: total 2,560"), "s001: total 2,560", clip(all, 300))
	leaks := steps.InternalLeaks(s, all)
	s.Check("control stats carry no internal ids or paths", len(leaks) == 0, "none", fmt.Sprint(leaks))
}

// `skills` in the control chat and `helper skills` in a work chat answer
// from the local skill subscriptions; `push` reviews in the chat (batch B,
// SKILLS-PUSH), remove stays in the local CLI.
func TestSkills(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CMD-SKILLS", harness.Options{
		Guarantees: []string{"cmd-skills"},
		Title:      "`skills` lists skill subscriptions; push reviews in the chat; remove points to the local CLI",
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "skills")
	ctl, ok := command(s, control.ID, "skills", "subscription")
	s.Check("`skills` in the control chat lists subscriptions", ok && strings.Contains(strings.ToLower(ctl), "no skill subscriptions"),
		"\"No skill subscriptions\"", clip(ctl, 160))
	wk, ok := command(s, work.ID, "helper skills", "subscription")
	s.Check("`helper skills` in a work chat lists subscriptions", ok && strings.Contains(strings.ToLower(wk), "no skill subscriptions"),
		"\"No skill subscriptions\"", clip(wk, 160))
	push, ok := command(s, work.ID, "helper skills push", "No local skill changes to push.")
	s.Check("`helper skills push` reviews in the chat (nothing to push)", ok, "\"No local skill changes to push.\"", clip(push, 160))
	rm, ok := command(s, work.ID, "helper skills remove demo", "cxp skills remove")
	s.Check("`helper skills remove` points to the local CLI", ok, "\"cxp skills remove\"", clip(rm, 160))
	calls := 0
	for _, c := range s.Codex.Calls() {
		if strings.Contains(c.Prompt, "skills") {
			calls++
		}
	}
	s.Check("skills commands start no Codex turn", calls == 0, "0 turns", fmt.Sprint(calls))
}
