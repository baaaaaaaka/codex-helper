package chats

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// A work chat with a coworker (v0.1.20 B:2031, B:8218, B:8328): a bare
// @codex is answered with the "no request text" hint; a coworker's helper
// command, mentioned or not prefixed the usual way, is refused with
// v0.1.20's owner-only text and runs nothing; a coworker's request is
// acknowledged with the coworker ACK; the owner's commands run with or
// without the mention.
func TestGroupChatMentionsAndCoworkerCommands(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "GROUP-CMDS", harness.Options{
		Guarantees: []string{"F-WRK-12", "F-WRK-13", "R-27"},
		Title:      "group work chat: bare @codex gets the hint; a coworker's `helper`/`!` command is refused (owner only); coworker ACK text; owner commands run",
		Budget:     150 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "group")
	if err := s.Graph.AddCoworker(work.ID); err != nil {
		t.Fatal(err)
	}
	before := len(s.Codex.Calls())
	reply := func(seq int64, sub string) (string, []string, bool) {
		var got string
		var quoted []string
		ok := s.WaitFor(fmt.Sprintf("reply containing %q", sub), steps.CommandTimeout, func() (bool, string) {
			for _, m := range s.SUTMessages(work.ID) {
				if m.RequestSeq > seq && strings.Contains(m.PlainText(), sub) {
					got, quoted = harness.NormalizeSpace(m.PlainText()), m.QuotedIDs
					return true, ""
				}
			}
			return false, ""
		})
		return got, quoted, ok
	}
	coworker := func(text string) (string, int64) {
		seq := s.Graph.LastSeq()
		m, err := s.Graph.PostAsCoworker(work.ID, text)
		if err != nil {
			t.Fatal(err)
		}
		return m.ID, seq
	}

	const bare = "Mention received, but there is no Codex request text. Add the task after @codex."
	_, seq := coworker("@codex")
	got, _, ok := reply(seq, "Mention received")
	s.Check("a coworker's bare @codex gets the hint", ok && strings.HasSuffix(got, bare), bare, got)

	const owner = "Only the helper owner can run helper commands in this Work chat. Send a normal question or task here and I will pass it to Codex."
	for _, text := range []string{"@codex helper status", "@codex !status"} {
		id, seq := coworker(text)
		got, quoted, ok := reply(seq, "Only the helper owner")
		s.Check(fmt.Sprintf("a coworker's `%s` is refused, quoting it", text), ok && strings.HasSuffix(got, owner) && len(quoted) == 1 && quoted[0] == id,
			owner+" (quoting the command)", fmt.Sprintf("%s quoted=%v", got, quoted))
	}

	token := s.Token("coworker")
	id, seq := coworker("@codex Please answer " + token)
	got, quoted, ok := reply(seq, "Request accepted")
	s.Check("a coworker's request gets the coworker ACK", ok && strings.HasSuffix(got, "⏳ Codex received your question. Request accepted.") && len(quoted) == 1 && quoted[0] == id,
		"⏳ Codex received your question. Request accepted. (quoting the request)", fmt.Sprintf("%s quoted=%v", got, quoted))
	_, answered := steps.WaitAnswered(s, work.ID, token, steps.AnswerTimeout)
	s.Check("the coworker's request is answered", answered, "answered", fmt.Sprint(answered))

	seq = s.Graph.LastSeq()
	steps.MustPost(s, work.ID, "@codex")
	got, _, ok = reply(seq, "Mention received")
	s.Check("the owner's bare @codex gets the hint", ok && strings.HasSuffix(got, bare), bare, got)
	for _, text := range []string{"@codex !status", "!status"} {
		seq := s.Graph.LastSeq()
		steps.MustPost(s, work.ID, text)
		got, _, ok := reply(seq, "queued. Directory:")
		s.Check(fmt.Sprintf("the owner's `%s` runs", text), ok, "the work-chat status", got)
	}
	s.Settle(time.Second, 4*time.Second)
	var prompts []string
	for _, c := range s.Codex.Calls()[before:] {
		if c.Kind == "work" && !strings.Contains(c.Prompt, token) {
			prompts = append(prompts, clip(c.Prompt, 60))
		}
	}
	s.Check("no command or bare mention reaches Codex", len(prompts) == 0, "only the coworker's request", strings.Join(prompts, " | "))
}
