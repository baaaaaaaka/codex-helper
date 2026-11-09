package lifecycle

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel/teams"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// `r xxx` wake syntax (decision: "`r xxx` 唤醒 parked 会话的用法不变";
// v0.1.20 dashboard_commands.go accepts `r`, `resume` and `unpark`, and a
// session number or id: sessionMatchesResumeKey). A bare number wakes
// that session only, the id works too, an active session is not
// "woken" again, and an unknown one gets a pointer to `list`.
func TestWakeSyntax(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "WAKE-SYNTAX", harness.Options{
		Guarantees: []string{"F-CTL-park", "R-22"},
		Title:      "`r <n>`, `unpark <id>`, and `resume <n>` wake a session and include its direct Work chat link; a second `r` says it is active, an unknown number points to `list`",
		Budget:     90 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	a, _ := steps.NewWorkChat(s, control, "wake-a")
	b, _ := steps.NewWorkChat(s, control, "wake-b")
	c, _ := steps.NewWorkChat(s, control, "wake-c")
	var ids []string
	s.Require("session ids announced", 10*time.Second, func() (bool, string) {
		ids = []string{steps.SessionID(s, a.ID), steps.SessionID(s, b.ID), steps.SessionID(s, c.ID)}
		return ids[0] != "" && ids[1] != "" && ids[2] != "", fmt.Sprint(ids)
	})
	s.Note("sessions %v", ids)
	num := func(id string) string { return strings.TrimLeft(strings.TrimPrefix(id, "s"), "0") }

	// say posts a control command and returns the helper's replies to it.
	say := func(cmd string) []string {
		from := s.Graph.LastSeq()
		steps.MustPost(s, control.ID, cmd)
		var out []string
		s.Poll(steps.CommandTimeout, func() bool {
			out = out[:0]
			for _, m := range s.SUTMessages(control.ID) {
				if m.RequestSeq > from {
					out = append(out, harness.NormalizeSpace(m.PlainText()))
				}
			}
			return len(out) > 0
		})
		s.Settle(time.Second, 3*time.Second)
		return out
	}
	for _, id := range ids {
		say("park " + id)
	}
	has := func(replies []string, sub string) bool {
		for _, r := range replies {
			if strings.Contains(strings.ToLower(r), strings.ToLower(sub)) {
				return true
			}
		}
		return false
	}
	hasLinkedReply := func(session, url string) bool {
		for _, m := range s.SUTMessages(control.ID) {
			if strings.Contains(m.PlainText(), "Resumed "+session) && strings.Contains(m.Content, `href="`+url+`"`) {
				return true
			}
		}
		return false
	}

	r1 := say("r " + num(ids[0]))
	s.Check("`r <n>` wakes that session", has(r1, "Resumed "+ids[0]), "Resumed "+ids[0], fmt.Sprintf("%q", r1))
	urlA := teams.ChatURL(a.ID, fakegraph.TenantID)
	s.Check("the wake reply includes a clickable link to the resumed Work chat", has(r1, "Open Work chat: "+urlA) && hasLinkedReply(ids[0], urlA), "Open Work chat: "+urlA, fmt.Sprintf("%q", r1))
	r1again := say("r " + ids[0])
	s.Check("`r <id>` of an active session does not wake it again", !has(r1again, "Resumed") && len(r1again) == 1,
		"one reply that it is not parked", fmt.Sprintf("%q", r1again))
	unpark := say("unpark " + ids[1])
	s.Check("`unpark <id>` wakes a parked session (v0.1.20 alias)", has(unpark, "Resumed "+ids[1]), "Resumed "+ids[1], fmt.Sprintf("%q", unpark))
	urlB := teams.ChatURL(b.ID, fakegraph.TenantID)
	s.Check("`unpark` includes a clickable link to its resumed Work chat", has(unpark, "Open Work chat: "+urlB) && hasLinkedReply(ids[1], urlB), "Open Work chat: "+urlB, fmt.Sprintf("%q", unpark))
	resume := say("resume " + num(ids[2]))
	s.Check("`resume <n>` wakes a parked session", has(resume, "Resumed "+ids[2]), "Resumed "+ids[2], fmt.Sprintf("%q", resume))
	unknown := say("r 97")
	s.Check("an unknown number names no session and points to `list`", len(unknown) == 1 && has(unknown, "list") && !has(unknown, "Resumed"),
		"one reply mentioning `list`", fmt.Sprintf("%q", unknown))
	notices := 0
	for _, ch := range []string{a.ID, b.ID, c.ID} {
		notices += len(s.SUTMessagesContaining(ch, "This chat was resumed"))
	}
	s.Check("each woken chat gets one resumed notice", notices == 3, "3 notices (one per chat)", fmt.Sprint(notices))
}
