package inbound

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

var outboxMarker = regexp.MustCompile(`<!--\s*codex-helper-outbox:[^>]*-->`)

// R-7: the helper's own output never becomes input, genuine user text that
// merely looks like helper output still runs, and a message from another
// helper instance on the same account does not start a message loop.
func TestR07_NoSelfEchoNoPingPong(t *testing.T) {
	s := harness.NewScenario(t, "R-7", harness.Options{
		Guarantees: []string{"R-7", "R-3", "INC-02"},
		Title:      "own posts never trigger Codex; look-alike user text does; another instance causes no loop",
		Budget:     120 * time.Second,
	})
	defer s.Finish()

	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "r07")

	// Phase 1: a normal turn, then several polls over the helper's own
	// messages (ACK, start notice, answer containing the prompt token).
	tok := s.Token("prompt")
	steps.MustPost(s, work.ID, "Normal request "+tok)
	s.WaitFor("normal turn answered", steps.AnswerTimeout, func() (bool, string) {
		for _, c := range s.Codex.CallsWithToken(tok) {
			if c.Answer != "" && len(s.SUTMessagesContaining(work.ID, c.Answer)) > 0 {
				return true, ""
			}
		}
		return false, ""
	})
	seq := s.Graph.LastSeq()
	for i := 0; i < 3; i++ {
		next, ok := s.WaitPolled(work.ID, seq, 20*time.Second)
		if !ok {
			break
		}
		seq = next
	}
	s.Settle(2*time.Second, 10*time.Second)
	own := len(s.Codex.CallsWithToken(tok))
	s.Check("R-7 helper's own posts never start a turn", own == 1 && len(s.Codex.Calls()) == 1,
		"1 Codex call in total", fmt.Sprintf("%d calls with the prompt token, %d calls total", own, len(s.Codex.Calls())), s.Transcript(work.ID, 20)...)

	// Phase 2: the user types text that looks like helper output.
	look := s.Token("lookalike")
	steps.MustPost(s, work.ID, "🔧 Helper: please summarise "+look)
	steps.WaitAnswered(s, work.ID, look, steps.AnswerTimeout)
	s.Settle(3*time.Second, 15*time.Second)
	n := len(s.Codex.CallsWithToken(look))
	s.Check("R-7 genuine user text that looks like helper output still runs", n == 1, "1 Codex call", fmt.Sprint(n), s.Transcript(work.ID, 10)...)

	// Phase 3: another helper instance (same account) posts helper-looking
	// HTML carrying its own outbox marker, then keeps reacting to every new
	// SUT message the way a second helper that mistakes them for user input
	// would (it ACKs them). A loop shows up as the SUT replying round after
	// round.
	template := ""
	for _, m := range s.SUTMessages(work.ID) {
		if strings.Contains(m.PlainText(), "Request accepted") {
			template = m.Content
			break
		}
	}
	if template == "" {
		template = "<p>🔧 Helper: ⏳ Codex is working. Request accepted.</p>"
		s.Note("no SUT ACK to copy; using a synthetic helper-looking template")
	}
	otherHTML := func(token string, i int) string {
		body := outboxMarker.ReplaceAllString(template, "")
		body = strings.TrimSpace(body) + "<p>Now running: " + token + "</p>"
		return body + fmt.Sprintf("<!-- codex-helper-outbox:other-instance-e2e-%d -->", i)
	}
	baseline := len(s.SUTMessages(work.ID))
	callsBefore := len(s.Codex.Calls())
	var otherTokens []string
	rounds, responded := 0, 0
	lastSUT := baseline
	for rounds < 6 {
		rounds++
		ot := s.Token(fmt.Sprintf("other%d", rounds))
		otherTokens = append(otherTokens, ot)
		if _, err := s.Graph.PostAsOtherHelper(work.ID, otherHTML(ot, rounds)); err != nil {
			t.Fatalf("post other: %v", err)
		}
		deadline := time.Now().Add(6 * time.Second)
		gotReply := false
		for time.Now().Before(deadline) {
			if len(s.SUTMessages(work.ID)) > lastSUT {
				gotReply = true
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !gotReply {
			break
		}
		responded++
		// Let the SUT finish its burst, then react to it.
		s.Settle(1500*time.Millisecond, 10*time.Second)
		lastSUT = len(s.SUTMessages(work.ID))
	}
	s.Settle(2*time.Second, 10*time.Second)
	sutPosts := len(s.SUTMessages(work.ID)) - baseline
	otherCalls := 0
	for _, ot := range otherTokens {
		otherCalls += len(s.Codex.CallsWithToken(ot))
	}
	transcript := s.Transcript(work.ID, 30)
	s.Check("R-7 no ping-pong with another helper instance", responded <= 1,
		"SUT reacts in at most 1 round (bounded posts)",
		fmt.Sprintf("SUT reacted in %d of %d rounds; %d SUT posts after injection", responded, rounds, sutPosts), transcript...)
	s.Check("R-7 (§8.3a) another instance's marked message never runs Codex", otherCalls == 0,
		"0 Codex calls for other-instance messages",
		fmt.Sprintf("%d Codex calls (total calls %d -> %d)", otherCalls, callsBefore, len(s.Codex.Calls())))
	quarantine := len(s.SUTMessagesContaining("", "quarantin"))
	s.Note("SUT messages mentioning quarantine: %d; control chat tail: %v", quarantine, s.Transcript(control.ID, 5))
}
