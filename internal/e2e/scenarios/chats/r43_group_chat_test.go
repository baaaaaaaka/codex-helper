package chats

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// R-43 (F-WRK-12): once a coworker is added to a work chat, plain text
// (from either member) starts no turn, an @codex message starts exactly one
// turn, the ACK quotes the request, and the chat gets the final only (no
// queue/progress status).
func TestR43_GroupChatAudience(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "R-43", harness.Options{
		Guarantees: []string{"R-43", "R-3"},
		Title:      "2-member work chat: plain text ignored, @codex runs once with a quoted ACK and the final only",
		Budget:     120 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "r43")
	// Make the helper see the chat as a 1:1 work chat first (as in real use
	// before a colleague is invited).
	warm := s.Token("warm")
	steps.MustPost(s, work.ID, "First solo request "+warm)
	steps.WaitAnswered(s, work.ID, warm, steps.AnswerTimeout)

	if err := s.Graph.AddCoworker(work.ID); err != nil {
		t.Fatal(err)
	}
	addedAt := s.Graph.LastSeq()
	plainCo, plainOwner, mention := s.Token("coplain"), s.Token("ownerplain"), s.Token("mention")
	if _, err := s.Graph.PostAsCoworker(work.ID, "Just chatting with you here "+plainCo); err != nil {
		t.Fatal(err)
	}
	steps.MustPost(s, work.ID, "Sure, talk later "+plainOwner)
	mentionMsg, err := s.Graph.PostAsCoworker(work.ID, "@codex please summarise the build log "+mention)
	if err != nil {
		t.Fatal(err)
	}
	steps.WaitAnswered(s, work.ID, mention, 45*time.Second)
	// Plain messages must stay ignored: give the helper several more polls.
	s.WaitPolled(work.ID, s.Graph.LastSeq(), 20*time.Second)
	s.Settle(4*time.Second, 15*time.Second)

	tr := s.Transcript(work.ID, 20)
	nCo, nOwner, nMention := len(s.Codex.CallsWithToken(plainCo)), len(s.Codex.CallsWithToken(plainOwner)), len(s.Codex.CallsWithToken(mention))
	s.Check("R-43 coworker plain text starts no turn", nCo == 0, "0 turns", fmt.Sprint(nCo), tr...)
	s.Check("R-43 owner plain text in a group chat starts no turn", nOwner == 0, "0 turns", fmt.Sprint(nOwner))
	s.Check("R-43/R-3 @codex message starts exactly one turn", nMention == 1, "1 turn", fmt.Sprint(nMention))

	var quoted, plainAck int
	var after []string
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Method: "POST", ChatID: work.ID, Since: addedAt}) {
		if r.Route == "POST /chats/{chat}/messages/replyWithQuote" && strings.Contains(r.Body, mentionMsg.ID) {
			quoted++
		}
	}
	for _, m := range s.SUTMessages(work.ID) {
		if m.RequestSeq <= addedAt {
			continue
		}
		text := harness.NormalizeSpace(m.PlainText())
		after = append(after, clip(text, 90))
		if strings.Contains(text, "Request accepted") && len(m.QuotedIDs) == 0 {
			plainAck++
		}
	}
	s.Check("R-43 the ACK quotes the @codex request", quoted == 1 && plainAck == 0, "1 quoted ACK, 0 unquoted", fmt.Sprintf("quoted=%d unquoted=%d", quoted, plainAck))
	status := 0
	for _, t := range after {
		if strings.Contains(t, "E2EANSWER") {
			continue // the final itself (may carry a closing line)
		}
		low := strings.ToLower(t)
		if strings.Contains(low, "now running") || strings.Contains(low, "still queued") || strings.Contains(low, "finished responding") || strings.Contains(low, "working on") {
			status++
		}
	}
	s.Check("R-43 status/progress is suppressed in the group chat", status == 0, "0 status messages after the coworker joined", fmt.Sprintf("%d; SUT messages after join: %q", status, after))
}

func clip(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}
