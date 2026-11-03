package turns

import (
	"fmt"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// R-1 / R-2 / R-3: a user message in a work chat yields exactly one final
// answer in Teams and exactly one Codex call. Also covers the control chat
// basics (control chat created and ready; `new <dir>` creates one work chat).
func TestR01_R02_R03_AnswerExactlyOnce(t *testing.T) {
	s := harness.NewScenario(t, "R-1-2-3", harness.Options{
		Guarantees: []string{"R-1", "R-2", "R-3", "F-CTL-1", "F-CTL-4"},
		Title:      "one prompt -> one Codex call -> one final answer; control chat basics",
	})
	defer s.Finish()

	control := steps.ReadyControlChat(s)
	controlChats := 0
	for _, c := range s.Graph.Chats() {
		if c.ID == control.ID {
			controlChats++
		}
	}
	work, _ := steps.NewWorkChat(s, control, "r01")
	chatsAfterNew := len(s.Graph.Chats())

	token := s.Token("msg")
	if _, err := s.Graph.PostAsUser(work.ID, "Please reply to "+token); err != nil {
		t.Fatalf("post: %v", err)
	}
	s.WaitCodexCalls(token, 1, steps.AnswerTimeout)
	var answer string
	s.WaitFor("final answer posted", steps.AnswerTimeout, func() (bool, string) {
		calls := s.Codex.CallsWithToken(token)
		if len(calls) == 0 || calls[0].Answer == "" {
			return false, "codex not answered"
		}
		answer = calls[0].Answer
		return len(s.SUTMessagesContaining(work.ID, answer)) > 0, "answer not in chat"
	})
	s.Settle(3*time.Second, 15*time.Second)

	calls := s.Codex.CallsWithToken(token)
	s.Check("R-3 one Codex call per Teams message", len(calls) == 1, "1 call", fmt.Sprintf("%d calls", len(calls)))
	finals := 0
	if answer != "" {
		finals = len(s.SUTMessagesContaining(work.ID, answer))
	}
	transcript := s.Transcript(work.ID, 30)
	s.Check("R-1/R-2 final answer appears exactly once", finals == 1, "1 message containing the answer", fmt.Sprintf("%d", finals), transcript...)
	acks := steps.CountContaining(s.SUTMessages(work.ID), "Request accepted")
	s.Check("R-2 exactly one ACK", acks == 1, "1 ACK", fmt.Sprintf("%d ACKs", acks), transcript...)
	s.Check("F-CTL-4 `new` creates exactly one work chat", chatsAfterNew == 2, "2 chats (control + work)", fmt.Sprintf("%d chats", chatsAfterNew))
	s.Check("F-CTL-1 one control chat", controlChats == 1, "1", fmt.Sprint(controlChats))
	posts := s.Graph.Requests(fakegraph.RequestFilter{Route: "POST /chats/{chat}/messages", ChatID: work.ID})
	s.Note("work chat POSTs: %d; requests: %v", len(posts), s.RequestsSummary(fakegraph.RequestFilter{}))
}
