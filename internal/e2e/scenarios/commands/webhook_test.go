package commands

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// testCards counts the cards the fake Workflow accepted whose texts
// contain the test card's heading.
func testCards(s *harness.Scenario) int {
	n := 0
	for _, c := range s.Graph.AcceptedWorkflowCards() {
		if strings.Contains(strings.Join(c.Texts, "\n"), "Codex helper notification test") {
			n++
		}
	}
	return n
}

// WEBHOOK-CTL (F-CTL-14): v0.1.20's `helper webhook …` in the control
// chat, against the fake Workflow endpoint only: help, setup, status,
// a refused non-https URL, `helper webhook <url>` saving the URL (never
// echoed) and turning cards on without a restart, `test` posting one test
// card, a final announced by a card, `off`, and a `test` sent while the
// helper is down posting one card and one reply across two restarts.
func TestWebhookFromControlChat(t *testing.T) {
	harness.NeedsCLI(t) // the Workflow setting lives in the cxp entry point
	harness.Parallel(t)
	s := harness.NewScenario(t, "WEBHOOK-CTL", harness.Options{
		Guarantees: []string{"F-CTL-14"},
		Title:      "`helper webhook setup|<url>|test|status|off` in the control chat with v0.1.20's texts, one test card per command",
		Budget:     200 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)

	expect(s, control.ID, "helper webhook", "Workflow webhook", "helper webhook setup - show the setup steps",
		"helper webhook <https-url> - save and enable the webhook", "helper webhook test - send one test card",
		"The webhook URL is stored locally as a private secret file and is not echoed back.")
	expect(s, control.ID, "helper webhook setup", "Workflow webhook setup", "Send webhook alerts to a chat",
		"Keep the target set to this Control chat:", "Chat ID: "+control.ID, "helper webhook <paste-url>")
	expect(s, control.ID, "helper webhook status", "Workflow webhook disabled.",
		"Send helper webhook <https-url> to enable Workflow notification cards.")
	expect(s, control.ID, "helper webhook test", "Workflow webhook test failed.", "Teams workflow notifications are disabled")

	hook := s.Graph.NewWorkflowWebhook()
	// A plain-http URL (the fake endpoint's own, so nothing leaves the
	// test even if it were used) is refused.
	expect(s, control.ID, "helper webhook "+strings.Replace(hook, "https://", "http://", 1), "Workflow webhook URL was not saved.",
		"Send an absolute https://... Teams Workflow webhook URL")
	s.Check("nothing was posted to the webhook while it was off", len(s.Graph.WorkflowCards()) == 0, "0 cards", fmt.Sprint(len(s.Graph.WorkflowCards())))

	expect(s, control.ID, "helper webhook "+hook, "Workflow webhook enabled.",
		"Webhook URL: saved locally as a private secret file; I will not echo it back.", "Send helper webhook test to send a test card.")
	expect(s, control.ID, "helper webhook status", "Workflow webhook enabled.", "Webhook URL: configured as a local private secret file.")
	// The CLI reads the state the running helper publishes, which follows
	// a change within seconds. How many tries that takes is timing, so the
	// tries stay out of the golden; the settled run is recorded.
	s.Poll(15*time.Second, func() bool {
		res := s.RunCLIAs(40*time.Second, harness.CLIAs{Poll: true}, "teams", "workflow", "status")
		return strings.Contains(res.Combined, "Teams workflow notifications: enabled")
	})
	out, code := s.RunCLI(40*time.Second, "teams", "workflow", "status")
	s.Check("`cxp teams workflow status` shows the chat's setting", code == 0 && strings.Contains(out, "Teams workflow notifications: enabled") &&
		strings.Contains(out, "Webhook URL file: configured"), "enabled, file configured", fmt.Sprintf("exit %d: %s", code, clip(out, 300)))

	expect(s, control.ID, "helper webhook test", "Workflow webhook test card queued.")
	s.Poll(10*time.Second, func() bool { return testCards(s) >= 1 })
	cards := s.Graph.AcceptedWorkflowCards()
	link := false
	for _, c := range cards {
		for _, a := range c.Actions {
			u, _ := a["url"].(string)
			if dec, err := url.PathUnescape(u); err == nil && strings.Contains(dec, control.ID) {
				link = true
			}
		}
	}
	s.Check("`helper webhook test` posts one test card that opens the control chat", testCards(s) == 1 && link,
		"1 test card, a control-chat link", fmt.Sprintf("%d test cards of %d, link=%v", testCards(s), len(cards), link))

	// The setting is live: the next final is announced by a card.
	work, _ := steps.NewWorkChat(s, control, "hook")
	before := len(s.Graph.AcceptedWorkflowCards())
	tok := s.Token("final")
	steps.MustPost(s, work.ID, "Short question "+tok)
	steps.WaitAnswered(s, work.ID, tok, steps.AnswerTimeout)
	s.Poll(20*time.Second, func() bool { return len(s.Graph.AcceptedWorkflowCards()) > before })
	s.Check("a final after `helper webhook <url>` is announced by a card, without a restart", len(s.Graph.AcceptedWorkflowCards()) == before+1,
		fmt.Sprint(before+1)+" cards", fmt.Sprint(len(s.Graph.AcceptedWorkflowCards())))

	// A test sent while the helper is down: one card and one reply, even
	// across a second restart.
	s.StopSUT()
	steps.MustPost(s, control.ID, "helper webhook test")
	s.StartSUT()
	s.WaitFor("the queued test card", 60*time.Second, func() (bool, string) {
		return testCards(s) == 2, fmt.Sprintf("%d test cards", testCards(s))
	})
	s.Settle(3*time.Second, 15*time.Second)
	s.StopSUT()
	s.StartSUT()
	if _, ok := s.WaitPolled(control.ID, s.Graph.LastSeq(), 30*time.Second); ok {
		s.Settle(4*time.Second, 15*time.Second)
	}
	s.Check("a test sent while the helper was down posts one card and one reply", testCards(s) == 2 &&
		steps.CountContaining(s.SUTMessages(control.ID), "Workflow webhook test card queued.") == 2,
		"2 test cards, 2 replies in all", fmt.Sprintf("%d test cards, %d replies", testCards(s),
			steps.CountContaining(s.SUTMessages(control.ID), "Workflow webhook test card queued.")))

	expect(s, control.ID, "helper webhook off", "Workflow webhook disabled.", "The helper will stop sending Workflow notification cards.")
	expect(s, control.ID, "helper webhook status", "Workflow webhook disabled.")
	n := len(s.Graph.WorkflowCards())
	expect(s, control.ID, "helper webhook test", "Workflow webhook test failed.", "Teams workflow notifications are disabled")
	s.Check("no card after `helper webhook off`", len(s.Graph.WorkflowCards()) == n, fmt.Sprint(n)+" cards", fmt.Sprint(len(s.Graph.WorkflowCards())))

	// The URL is a secret: no helper post repeats it.
	sig := hook[strings.LastIndex(hook, "sig=")+len("sig="):]
	leaked := 0
	for _, c := range s.Graph.Chats() {
		for _, m := range s.SUTMessages(c.ID) {
			if strings.Contains(m.PlainText(), sig) || strings.Contains(m.PlainText(), fakegraph.WorkflowHost) {
				leaked++
			}
		}
	}
	s.Check("the helper never echoes the webhook URL", leaked == 0, "0 posts", fmt.Sprint(leaked))
}
