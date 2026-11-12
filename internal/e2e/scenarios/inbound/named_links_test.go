package inbound

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

func TestNamedHyperlinksReachCodex(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "LINKS", harness.Options{
		Title: "named hyperlinks survive Teams admission into Codex input", Budget: 90 * time.Second,
		Golden: harness.GoldenOptions{Skip: "assert the exact named destination and queue prefix removal in app-server input; existing inbound goldens cover chat rendering"},
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "links")
	for _, queued := range []bool{false, true} {
		token := s.Token("named-link")
		body := `<p>Continue <a href="https://teams.microsoft.com/l/message/19%3Aexample/123?context=%7B%22contextType%22%3A%22chat%22%7D">移交对话</a> ` + token + `</p>`
		if queued {
			body = strings.Replace(body, "<p>", "<p>helper queue ", 1)
		}
		if _, err := s.Graph.PostHTMLAsUser(work.ID, body); err != nil {
			t.Fatal(err)
		}
		steps.WaitAnswered(s, work.ID, token, steps.AnswerTimeout)
		calls := s.Codex.CallsWithToken(token)
		good := len(calls) == 1 && strings.Contains(calls[0].Prompt, "移交对话: https://teams.microsoft.com/l/message/19%3Aexample/123?context=")
		if queued {
			good = good && !strings.Contains(calls[0].Prompt, "helper queue")
		}
		s.Check("Codex sees the named destination", good, "one call containing the label and URL, without the queue command", fmt.Sprintf("%d calls; expected input=%t", len(calls), good))
	}
}
