package lifecycle

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Codex CLI too old for the model (decision 11: keep Codex auto-upgrade;
// v0.1.20 codexErrorRequiresUpgrade). The e2e helper runs Codex chosen
// with --codex-path, so automatic upgrade is off (as in the old core): the
// user gets the "needs an update" notice with the manual wording and the
// request is not retried by itself; `helper retry last` runs it again.
// In the control chat `codex update now` explains that it is not
// available with an explicit Codex path, and `codex update` shows help.
// The upgrade itself (upgrader set) is unit-tested only.
func TestCodexTooOldAndUpdateCommand(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CODEX-UPGRADE", harness.Options{
		Guarantees: []string{"codex-auto-upgrade"},
		Title:      "Codex rejects a request as too old: one notice, no automatic retry, `helper retry last` answers; `codex update now` with --codex-path is refused with the reason",
		Budget:     90 * time.Second,
	})
	defer s.Finish()
	tok := s.Token("tooold")
	answer := "Answered after the update " + tok
	var mu sync.Mutex
	n := 0
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.Kind != "work" || !c.HasToken(tok) {
			return harness.CodexBehavior{}
		}
		mu.Lock()
		defer mu.Unlock()
		n++
		if n == 1 {
			return harness.CodexBehavior{Err: errors.New("The 'gpt-9' model requires a newer version of Codex. Please upgrade to the latest app or CLI and try again.")}
		}
		return harness.CodexBehavior{Answer: answer}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "tooold")
	steps.MustPost(s, work.ID, "Use the newest model "+tok)
	notice := s.Poll(30*time.Second, func() bool { return len(s.SUTMessagesContaining(work.ID, "Codex CLI needs an update")) > 0 })
	s.Settle(2*time.Second, 6*time.Second)
	tr := s.Transcript(work.ID, 10)
	msgs := s.SUTMessagesContaining(work.ID, "Codex CLI needs an update")
	text := ""
	if len(msgs) > 0 {
		text = harness.NormalizeSpace(msgs[0].PlainText())
	}
	s.Check("one too-old notice naming `helper retry last`", notice && len(msgs) == 1 && strings.Contains(text, "helper retry last"),
		"1 notice with `helper retry last`", fmt.Sprintf("%d: %.300q", len(msgs), text), tr...)
	s.Check("manual wording with --codex-path (no automatic upgrade promised)",
		strings.Contains(text, "Automatic Codex upgrade isn't enabled") && !strings.Contains(text, "I will upgrade Codex"),
		"manual wording", fmt.Sprintf("%.300q", text), tr...)
	s.Check("Codex's reason is quoted", strings.Contains(text, "requires a newer version"), "Codex said: …", fmt.Sprintf("%.300q", text), tr...)
	calls := len(s.Codex.CallsWithToken(tok))
	s.Check("not retried automatically", calls == 1, "1 call", fmt.Sprint(calls), tr...)

	steps.MustPost(s, work.ID, "helper retry last")
	answered := s.Poll(30*time.Second, func() bool { return len(s.SUTMessagesContaining(work.ID, answer)) > 0 })
	s.Settle(time.Second, 4*time.Second)
	tr = s.Transcript(work.ID, 12)
	s.Check("`helper retry last` runs the request again and it is answered once", answered && len(s.SUTMessagesContaining(work.ID, answer)) == 1,
		"1 answer", fmt.Sprintf("%d answers, %d calls", len(s.SUTMessagesContaining(work.ID, answer)), len(s.Codex.CallsWithToken(tok))), tr...)

	say := func(cmd string) string {
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
		return strings.Join(out, " || ")
	}
	now := say("codex update now")
	s.Check("`codex update now` with an explicit Codex path is refused with the reason",
		strings.Contains(now, "not available") && strings.Contains(now, "--codex-path"), "not available … --codex-path", fmt.Sprintf("%.300q", now))
	help := say("codex update")
	s.Check("`codex update` shows the command help", strings.Contains(help, "codex update now") && strings.Contains(help, "does not update cxp"),
		"help text", fmt.Sprintf("%.300q", help))
}
