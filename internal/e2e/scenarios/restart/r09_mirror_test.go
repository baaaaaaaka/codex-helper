package restart

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// R-9 / R-10: a new local TUI session (Codex rollout JSONL written by the
// TUI) is mirrored into Teams exactly once; after a stop, appended records
// and a session created while down are caught up incrementally with no
// duplicates.
func TestR09_R10_MirrorTUIRollout(t *testing.T) {
	s := harness.NewScenario(t, "R-9-10", harness.Options{
		Guarantees: []string{"R-9", "R-10", "R-2", "R-34", "F-MIR-1", "F-MIR-4"},
		Title:      "TUI rollout mirrored once; incremental catch-up after stop; down-time session discovered",
		Budget:     150 * time.Second,
	})
	defer s.Finish()

	control := steps.ReadyControlChat(s)
	chatsBefore := len(s.Graph.Chats())

	// A new TUI session appears after the helper started.
	cwd := s.Env.WorkDir(t, "r09-tui")
	r := harness.NewRollout(t, s.Env, cwd)
	p1, a1 := s.Token("tuiprompt1"), s.Token("tuianswer1")
	r.AppendTurn(t, "Local question "+p1, "Looking into it", "Local answer "+a1)
	writtenAt := time.Now()

	var chat fakegraph.ChatInfo
	s.WaitFor("TUI session mirrored (answer visible)", 45*time.Second, func() (bool, string) {
		cs := steps.ChatsWithSUTText(s, a1)
		if len(cs) > 0 {
			chat = cs[0]
			return true, ""
		}
		return false, fmt.Sprintf("%d chats", len(s.Graph.Chats()))
	})
	if chat.ID != "" {
		s.Note("mirrored into %q %.1fs after the rollout was written", chat.Topic, time.Since(writtenAt).Seconds())
	}
	s.Settle(3*time.Second, 15*time.Second)
	if chat.ID == "" {
		s.Check("R-9 new TUI session gets a work chat", false, "a work chat with the mirrored turn", "none", s.Transcript(control.ID, 10)...)
		return
	}
	s.Check("R-9 exactly one new chat for the session", len(s.Graph.Chats()) == chatsBefore+1,
		"1 new chat", fmt.Sprintf("%d new chats", len(s.Graph.Chats())-chatsBefore))
	// The import intro repeats the session title (= first prompt), so count
	// the labelled user record only.
	s.Check("R-9 prompt mirrored once", steps.UserRecords(s, chat.ID, "Local question "+p1) == 1, "1", fmt.Sprint(steps.UserRecords(s, chat.ID, "Local question "+p1)), s.Transcript(chat.ID, 20)...)
	s.Check("R-9 final answer mirrored once", len(s.SUTMessagesContaining(chat.ID, a1)) == 1, "1", fmt.Sprint(len(s.SUTMessagesContaining(chat.ID, a1))))
	s.Check("R-9 TUI-only turn never runs Codex", len(s.Codex.Calls()) == 0, "0 Codex calls", fmt.Sprint(len(s.Codex.Calls())))
	detected := len(s.SUTMessagesContaining("", "detected"))
	s.Note("SUT messages mentioning 'detected': %d", detected)

	// R-9: an ongoing turn of the linked session is mirrored while running.
	p2, a2 := s.Token("tuiprompt2"), s.Token("tuianswer2")
	r.AppendTurn(t, "Second local question "+p2, "", "Second local answer "+a2)
	s.WaitFor("ongoing turn mirrored while running", 25*time.Second, func() (bool, string) {
		return len(s.SUTMessagesContaining(chat.ID, a2)) > 0, "second answer not mirrored yet"
	})

	// R-10: stop, append a third turn, create a brand-new session, restart.
	s.StopSUT()
	p3, a3 := s.Token("tuiprompt3"), s.Token("tuianswer3")
	r.AppendTurn(t, "Third local question "+p3, "", "Third local answer "+a3)
	cwd2 := s.Env.WorkDir(t, "r09-tui-down")
	r2 := harness.NewRollout(t, s.Env, cwd2)
	p4, a4 := s.Token("downprompt"), s.Token("downanswer")
	r2.AppendTurn(t, "Created while helper was down "+p4, "", "Down answer "+a4)
	s.StartSUT()

	s.WaitFor("turn appended while down caught up", 25*time.Second, func() (bool, string) {
		return len(s.SUTMessagesContaining(chat.ID, a3)) > 0, "third answer not mirrored yet"
	})
	s.WaitFor("down-time session discovered", 30*time.Second, func() (bool, string) {
		return len(steps.ChatsWithSUTText(s, a4)) > 0, "down-time session not mirrored yet"
	})
	s.Settle(3*time.Second, 15*time.Second)

	tr := s.Transcript(chat.ID, 30)
	for _, c := range []struct{ name, prompt, answer string }{
		{"turn 1", "Local question " + p1, a1}, {"turn 2", "Second local question " + p2, a2}, {"turn 3", "Third local question " + p3, a3},
	} {
		n := steps.UserRecords(s, chat.ID, c.prompt)
		s.Check("R-10 after restart "+c.name+" prompt present exactly once", n == 1, "1", fmt.Sprint(n), tr...)
		n = len(s.SUTMessagesContaining(chat.ID, c.answer))
		s.Check("R-10 after restart "+c.name+" answer present exactly once", n == 1, "1", fmt.Sprint(n), tr...)
	}
	msgs := s.SUTMessages(chat.ID)
	idx := func(tok string) int {
		for i, m := range msgs {
			if strings.Contains(m.PlainText(), tok) {
				return i
			}
		}
		return -1
	}
	order := []int{idx("User: Local question " + p1), idx(a1), idx(p2), idx(a2), idx(p3), idx(a3)}
	ordered := order[0] >= 0
	for i := 1; i < len(order); i++ {
		ordered = ordered && order[i] >= order[i-1]
	}
	s.Check("R-6 mirrored records in transcript order", ordered, "p1 <= a1 <= p2 <= a2 <= p3 <= a3 (message index)", fmt.Sprint(order))
	downChats := steps.ChatsWithSUTText(s, a4)
	s.Check("R-9 session created while down is mirrored into one chat", len(downChats) == 1, "1 chat", fmt.Sprint(len(downChats)))
	if len(downChats) == 1 {
		n := len(s.SUTMessagesContaining(downChats[0].ID, a4))
		s.Check("R-9 down-time answer mirrored once", n == 1, "1", fmt.Sprint(n), s.Transcript(downChats[0].ID, 20)...)
	}
	s.Check("R-9 mirroring never runs Codex", len(s.Codex.Calls()) == 0, "0", fmt.Sprint(len(s.Codex.Calls())))
}
