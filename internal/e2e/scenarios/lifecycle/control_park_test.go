package lifecycle

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Owner decision: `park <id>` in the control chat parks that work chat (the
// same effect as `helper park` inside it): it is acknowledged in the
// control chat, the chat is not read from Graph while parked, a prompt
// typed there does not run, and `r <id>` wakes it and runs that prompt
// once. PARK covers `helper park` + `r <id>`; this covers the control-chat
// form of park.
func TestControlChatPark(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "PARK-CTL", harness.Options{
		Guarantees: []string{"F-CTL-12", "F-WRK-6", "F-WRK-7"},
		Title:      "`park <id>` from the control chat: acknowledged, no reads, no turns until `r <id>`",
		Budget:     120 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	chat, _ := steps.NewWorkChat(s, control, "park-ctl")
	warm := s.Token("warm")
	steps.MustPost(s, chat.ID, "First task "+warm)
	steps.WaitAnswered(s, chat.ID, warm, steps.AnswerTimeout)
	id := steps.SessionID(s, chat.ID)
	s.Require("session id announced", 5*time.Second, func() (bool, string) { return id != "", "none" })
	s.Settle(2*time.Second, 10*time.Second)

	cmdSeq := s.Graph.LastSeq()
	steps.MustPost(s, control.ID, "park "+id)
	ackOK := s.WaitFor("control chat acknowledges `park "+id+"`", steps.CommandTimeout, func() (bool, string) {
		for _, m := range s.SUTMessages(control.ID) {
			if m.RequestSeq > cmdSeq && strings.Contains(strings.ToLower(harness.TokenPattern.ReplaceAllString(m.PlainText(), "")), "park") {
				return true, ""
			}
		}
		return false, "no reply"
	})
	time.Sleep(2 * time.Second)
	parkedFrom := s.Graph.LastSeq()
	held := s.Token("held")
	steps.MustPost(s, chat.ID, "Typed while asleep "+held)
	s.Poll(15*time.Second, func() bool { return false })
	reads := chatReads(s, chat.ID, parkedFrom)
	heldCalls := len(s.Codex.CallsWithToken(held))

	wokeAt := time.Now()
	steps.MustPost(s, control.ID, "r "+id)
	answered := s.Poll(30*time.Second, func() bool { return len(s.SUTMessagesContaining(chat.ID, "answer for "+held)) > 0 })
	s.Settle(2*time.Second, 8*time.Second)

	var replies []string
	for _, m := range s.SUTMessages(control.ID) {
		if m.RequestSeq > cmdSeq {
			replies = append(replies, clip(harness.NormalizeSpace(m.PlainText()), 120))
		}
	}
	tr := s.Transcript(chat.ID, 12)
	s.Check("`park <id>` in the control chat is acknowledged there", ackOK, "a park reply in the control chat", fmt.Sprintf("control replies: %q", replies), s.Transcript(control.ID, 6)...)
	s.Check("a chat parked from the control chat gets no Graph reads", len(reads) == 0, "0 reads in 15s", fmt.Sprintf("%d; first: %v", len(reads), firstN(reads, 3)))
	s.Check("a prompt typed into the parked chat does not run before the wake", heldCalls == 0, "0 turns", fmt.Sprint(heldCalls), tr...)
	calls := s.Codex.CallsWithToken(held)
	once := len(calls) == 1 && answered && !calls[0].Started.Before(wokeAt)
	s.Check("`r <id>` wakes it and the parked prompt runs exactly once", once, "1 turn after `r`", fmt.Sprintf("%d turns, answered=%v", len(calls), answered), tr...)
}
