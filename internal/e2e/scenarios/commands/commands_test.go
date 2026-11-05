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
	s.WaitSUTMessage(control.ID, "Codex finished in", steps.CommandTimeout)

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

// `helper model`: status, list, switch (applies to the next turn), reset,
// and an unknown model is refused.
func TestModelStatusAndSwitch(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CMD-MODEL", harness.Options{
		Guarantees: []string{"cmd-model"},
		Title:      "`helper model` shows, lists, switches and resets the session model",
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "model")

	status, ok := command(s, work.ID, "helper model", "model")
	s.Check("`helper model` shows the current model", ok && strings.Contains(status, "gpt-5-codex"), "mentions gpt-5-codex", clip(status, 160))
	list, ok := command(s, work.ID, "helper model list", "gpt-5-codex")
	s.Check("`helper model list` lists Codex's models", ok, "lists gpt-5-codex", clip(list, 160))
	bad, ok := command(s, work.ID, "helper model no-such-model", "unknown model")
	s.Check("an unknown model is refused", ok, "\"Unknown model\"", clip(bad, 120))

	before, ok1 := ask(s, work.ID, "default")
	sw, ok := command(s, work.ID, "helper model gpt-5-codex high", "gpt-5-codex")
	s.Check("`helper model <id> <effort>` is confirmed", ok && strings.Contains(strings.ToLower(sw), "high"), "confirms model and effort", clip(sw, 160))
	after, ok2 := ask(s, work.ID, "switched")
	s.Check("the next request uses the chosen model and effort", ok2 && after.Model == "gpt-5-codex" && after.Effort == "high",
		"model=gpt-5-codex effort=high", fmt.Sprintf("ok=%v model=%q effort=%q", ok2, after.Model, after.Effort))
	s.Check("the switch keeps the same Codex thread", ok1 && ok2 && before.ThreadID == after.ThreadID, "same thread",
		fmt.Sprintf("%q -> %q", before.ThreadID, after.ThreadID))
	reset, ok := command(s, work.ID, "helper model reset", "reset")
	s.Check("`helper model reset` is confirmed", ok, "\"Model reset\"", clip(reset, 120))
	last, ok3 := ask(s, work.ID, "reset")
	// Codex keeps a turn's override for later turns, so the reset sends the
	// default (gpt-5-codex, medium) explicitly.
	s.Check("after reset the next request goes back to the Codex default", ok3 && last.Model == "gpt-5-codex" && last.Effort == "medium",
		"model=gpt-5-codex effort=medium", fmt.Sprintf("ok=%v model=%q effort=%q", ok3, last.Model, last.Effort))
}

// `helper stats` and control `stats`: requests and token usage.
func TestStats(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CMD-STATS", harness.Options{
		Guarantees: []string{"cmd-stats"},
		Title:      "`helper stats` / `stats` report requests and token usage",
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "stats")
	_, ok1 := ask(s, work.ID, "one")
	_, ok2 := ask(s, work.ID, "two")
	s.Check("two requests answered", ok1 && ok2, "2 answers", fmt.Sprintf("%v %v", ok1, ok2))
	// The fake Codex reports 1,280 tokens per turn.
	st, ok := command(s, work.ID, "helper stats", "stats")
	s.Check("`helper stats` counts this session's requests", ok && strings.Contains(st, "2 requests") && strings.Contains(st, "2 answered"),
		"2 requests, 2 answered", clip(st, 200))
	s.Check("`helper stats` reports token usage", strings.Contains(st, "2,560"), "2,560 tokens total", clip(st, 200))
	all, ok := command(s, control.ID, "stats", "all sessions")
	s.Check("control `stats` sums all sessions", ok && strings.Contains(all, "2,560"), "2,560 tokens", clip(all, 200))
	leaks := steps.InternalLeaks(s, st+" "+all)
	s.Check("stats carry no internal ids or paths", len(leaks) == 0, "none", fmt.Sprint(leaks))
}

// `skills` in the control chat and `helper skills` in a work chat answer
// from the local skill subscriptions; remove/push stay in the local CLI.
func TestSkills(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CMD-SKILLS", harness.Options{
		Guarantees: []string{"cmd-skills"},
		Title:      "`skills` lists skill subscriptions; push/remove point to the local CLI",
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
	push, ok := command(s, work.ID, "helper skills push", "cxp skills push")
	s.Check("`helper skills push` points to the local CLI", ok, "\"cxp skills push\"", clip(push, 160))
	calls := 0
	for _, c := range s.Codex.Calls() {
		if strings.Contains(c.Prompt, "skills") {
			calls++
		}
	}
	s.Check("skills commands start no Codex turn", calls == 0, "0 turns", fmt.Sprint(calls))
}
