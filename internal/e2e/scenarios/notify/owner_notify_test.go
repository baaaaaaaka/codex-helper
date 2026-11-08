package notify

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Owner decision (coordinator, M3; CORE-DESIGN §4): the owner is notified
// on EVERY final exactly once, by exactly one of
//
//   - a Workflow card, when the webhook is configured and accepts it, or
//   - one @mention message in the CONTROL chat (no webhook configured, or
//     the webhook failed: 5xx / unreachable).
//
// Never both, never zero, and no @mention anywhere else (not on the final
// in the work chat). A card is tried at most once (F-RND-6: an ambiguous
// delivery is not retried). SPEC R-49: "Webhook on: 0 in-chat mentions and
// 1 card per final. Webhook returns 500: one control-chat mention
// fallback."
//
// Each case sends two prompts, one after the other, in one work chat and
// judges each final separately.

type notifyCase struct {
	id      string
	title   string
	webhook bool
	fault   *fakegraph.Fault
}

// ownerMentions returns the SUT messages (any chat) that @mention the owner
// and are not in seen. Fake message stamps can run ahead of the wall clock,
// so "new" is judged by id, not by time.
func ownerMentions(s *harness.Scenario, seen map[string]bool) []fakegraph.Message {
	owner := s.Graph.Owner().ID
	var out []fakegraph.Message
	for _, c := range s.Graph.Chats() {
		for _, m := range s.Graph.Messages(c.ID) {
			if m.Origin != fakegraph.OriginSUT || seen[m.ID] {
				continue
			}
			for _, raw := range m.Mentions {
				var mention struct {
					Mentioned struct {
						User *struct {
							ID string `json:"id"`
						} `json:"user"`
					} `json:"mentioned"`
				}
				if json.Unmarshal(raw, &mention) == nil && mention.Mentioned.User != nil && mention.Mentioned.User.ID == owner {
					out = append(out, m)
					break
				}
			}
		}
	}
	return out
}

func describe(s *harness.Scenario, control string, msgs []fakegraph.Message) []string {
	var out []string
	for _, m := range msgs {
		where := "work"
		if m.ChatID == control {
			where = "control"
		}
		out = append(out, fmt.Sprintf("[%s] %.100s", where, harness.NormalizeSpace(m.PlainText())))
	}
	return out
}

func runNotify(t *testing.T, nc notifyCase) {
	harness.Parallel(t)
	s := harness.NewScenario(t, nc.id, harness.Options{
		Guarantees:      []string{"R-49", "F-RND-6", "owner-notify-every-final"},
		Title:           nc.title,
		Budget:          120 * time.Second,
		WorkflowWebhook: nc.webhook,
	})
	defer s.Finish()
	if nc.fault != nil {
		s.Graph.AddFault(*nc.fault)
	}
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, strings.ToLower(nc.id))
	if nc.webhook {
		s.WaitFor("ready card", 15*time.Second, func() (bool, string) {
			for _, c := range s.Graph.WorkflowCards() {
				if strings.Contains(c.Text(), "💬 Codex chat ready") {
					return true, ""
				}
			}
			return false, "not sent"
		})
		s.Settle(2*time.Second, 10*time.Second)
	}
	for i := 1; i <= 2; i++ {
		tok := s.Token(fmt.Sprintf("final%d", i))
		seen := map[string]bool{}
		for _, m := range ownerMentions(s, nil) {
			seen[m.ID] = true
		}
		cardsBefore := len(s.Graph.WorkflowCards())
		steps.MustPost(s, work.ID, "Short question "+tok)
		steps.WaitAnswered(s, work.ID, tok, steps.AnswerTimeout)
		if nc.webhook {
			s.WaitFor(fmt.Sprintf("a card attempt for final %d", i), 20*time.Second, func() (bool, string) {
				return len(s.Graph.WorkflowCards()) > cardsBefore, fmt.Sprintf("%d card attempts", len(s.Graph.WorkflowCards())-cardsBefore)
			})
		} else {
			s.WaitFor(fmt.Sprintf("an owner @mention for final %d", i), 15*time.Second, func() (bool, string) {
				return len(ownerMentions(s, seen)) > 0, "none yet"
			})
		}
		// Let retries, fallbacks and late notices happen.
		time.Sleep(8 * time.Second)
		s.Settle(3*time.Second, 15*time.Second)

		var attempts, accepted int
		var cardTexts []string
		for _, c := range s.Graph.WorkflowCards()[cardsBefore:] {
			attempts++
			if c.Status >= 200 && c.Status < 300 {
				accepted++
			}
			cardTexts = append(cardTexts, fmt.Sprintf("status=%d fault=%q %.120s", c.Status, c.Fault, strings.ReplaceAll(c.Text(), "\n", " / ")))
		}
		mentions := ownerMentions(s, seen)
		var inControl, elsewhere int
		for _, m := range mentions {
			if m.ChatID == control.ID {
				inControl++
			} else {
				elsewhere++
			}
		}
		label := fmt.Sprintf("final %d", i)
		ev := append([]string{"cards: " + strings.Join(cardTexts, " | ")}, describe(s, control.ID, mentions)...)
		total := accepted + inControl
		s.Check("notify-every-final "+label+": exactly one owner notification (card or control-chat @mention), never both, never zero", total == 1,
			"1", fmt.Sprintf("%d accepted cards + %d control-chat @mentions", accepted, inControl), ev...)
		s.Check("notify-every-final "+label+": no owner @mention outside the control chat", elsewhere == 0, "0", fmt.Sprint(elsewhere), ev...)
		switch {
		case nc.webhook && nc.fault == nil:
			s.Check("R-49 "+label+": the notification is the Workflow card (tried once)", accepted == 1 && attempts == 1 && inControl == 0, "1 card, 0 mentions",
				fmt.Sprintf("%d accepted of %d attempts, %d control mentions", accepted, attempts, inControl), ev...)
		case nc.webhook:
			s.Check("F-RND-6 "+label+": the card is tried at most once", attempts <= 1, "<= 1 card attempt", fmt.Sprint(attempts), ev...)
			s.Check("R-49 "+label+": the failed card is replaced by one control-chat fallback @mention", accepted == 0 && inControl == 1,
				"1 control-chat @mention", fmt.Sprintf("%d control mentions, %d accepted cards", inControl, accepted), ev...)
		default:
			s.Check("notify-every-final "+label+": without a webhook the notification is one control-chat @mention", inControl == 1 && attempts == 0,
				"1 control-chat @mention, no card", fmt.Sprintf("%d control mentions, %d card attempts", inControl, attempts), ev...)
		}
	}
}

// NOTIFY-WH: healthy webhook -> one card per final, no @mention.
func TestNotify_HealthyWebhook(t *testing.T) {
	runNotify(t, notifyCase{id: "NOTIFY-WH", title: "healthy Workflow webhook: one card per final and no @mention", webhook: true})
}

// NOTIFY-NOWH: no webhook -> one control-chat @mention per final.
func TestNotify_NoWebhook(t *testing.T) {
	runNotify(t, notifyCase{id: "NOTIFY-NOWH", title: "no webhook: one control-chat @mention per final"})
}

// NOTIFY-WH500: the webhook answers 500 -> no retry, one control-chat fallback mention.
func TestNotify_Webhook500(t *testing.T) {
	runNotify(t, notifyCase{id: "NOTIFY-WH500", title: "webhook returns 500: card tried at most once, then one control-chat fallback @mention", webhook: true,
		fault: &fakegraph.Fault{Name: "webhook-500", Host: fakegraph.WorkflowHost, Kind: fakegraph.FaultStatus, Status: 500}})
}

// NOTIFY-WHDOWN: the webhook is unreachable (connection dropped before the
// request is accepted) -> no retry, one fallback mention.
func TestNotify_WebhookUnreachable(t *testing.T) {
	runNotify(t, notifyCase{id: "NOTIFY-WHDOWN", title: "webhook unreachable: card tried at most once, then one control-chat fallback @mention", webhook: true,
		fault: &fakegraph.Fault{Name: "webhook-down", Host: fakegraph.WorkflowHost, Kind: fakegraph.FaultDropBeforeAccept}})
}
