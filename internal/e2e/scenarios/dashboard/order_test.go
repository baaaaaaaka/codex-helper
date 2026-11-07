package dashboard

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// inOrder reports whether every part occurs in text, in the given order.
func inOrder(text string, parts ...string) bool {
	at := 0
	for _, p := range parts {
		i := strings.Index(text[at:], p)
		if i < 0 {
			return false
		}
		at += i + len(p)
	}
	return true
}

// DASH-ORDER (owner request 2026-11-07): the control chat's chat lists
// (`st`, `list`, `stats`) put the most recently active chat last. Activity
// is Codex's (the chat's newest user or agent record), not Teams': a
// helper command in a chat does not move it, an imported session counts
// from its old local history, and a new chat without Codex history counts
// from its creation.
func TestChatListsMostRecentLast(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "DASH-ORDER", harness.Options{
		Guarantees: []string{"dash-order"},
		Title:      "`st`, `list` and `stats` list the chats by Codex activity, the most recent last; Teams-only activity does not count",
		Budget:     4 * time.Minute,
		NoStart:    true,
	})
	defer s.Finish()
	old := localSession(s, s.Env.WorkDir(t, "imported"), time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC), "Plan the old release", "Old release planned")
	s.StartSUT()
	control := steps.ReadyControlChat(s)
	alpha, _ := steps.NewWorkChat(s, control, "alpha") // s001
	beta, _ := steps.NewWorkChat(s, control, "beta")   // s002
	ask := func(chatID, label string) {
		token := s.Token(label)
		steps.MustPost(s, chatID, "Please answer "+token)
		_, ok := steps.WaitAnswered(s, chatID, token, steps.AnswerTimeout)
		s.Check("the "+label+" turn is answered", ok, "answered", strings.Join(s.Transcript(chatID, 4), " | "))
	}
	ask(beta.ID, "beta")
	ask(alpha.ID, "alpha")
	before := chatsNow(s)
	steps.MustPost(s, control.ID, "continue "+old.SessionID) // s003, local history from January
	imported := newChat(s, before, "the imported session's work chat")
	_, done := s.WaitSUTMessage(imported.ID, "Import complete", steps.CommandTimeout)
	s.Check("the old session is imported", done, "Import complete", strings.Join(s.Transcript(imported.ID, 4), " | "))
	steps.NewWorkChat(s, control, "gamma") // s004: no Codex history yet
	command(s, beta.ID, "helper model", "Selection:")
	// The mirror notices the rollouts' last change within its stat interval.
	s.Settle(2*time.Second, 15*time.Second)
	time.Sleep(3 * time.Second)

	want := []string{"s003", "s002", "s001", "s004"}
	st := command(s, control.ID, "st", "Active Work chats")
	var sessions []string
	for _, l := range want {
		sessions = append(sessions, "Session: "+l)
	}
	s.Check("`st` lists the chats by Codex activity, the most recent last", inOrder(st, sessions...),
		strings.Join(want, ", "), clip(st, 1200))
	list := command(s, control.ID, "list", "Sessions:")
	var rows []string
	for _, l := range want {
		rows = append(rows, l+" —")
	}
	s.Check("`list` uses the same order", inOrder(list, rows...), strings.Join(want, ", "), clip(list, 600))
	stats := command(s, control.ID, "stats", "Scope: all Work chats")
	s.Check("`stats` lists the chats with usage in the same order", inOrder(stats, "| s002 |", "| s001 |"),
		"s002, then s001", fmt.Sprint(clip(stats, 1200)))
}
