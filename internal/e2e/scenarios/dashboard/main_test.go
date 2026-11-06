// Package dashboard holds black-box Teams E2E scenarios for the control
// dashboard (batch A): the workspace and session lists with number
// selection, `continue` and its history import, `open`, `details`, the
// control `status` list, `mkdir`, the path hint and `new` parity, and the
// work chat's `helper publish-history`.
//
// Each scenario package is its own test binary so `go test` can run the
// packages in parallel; scenarios inside a package also run in parallel
// (harness.Parallel), each against its own cxp process.
package dashboard

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut"
)

func TestMain(m *testing.M) {
	os.Exit(sut.Run(m))
}

// base is the fixed start of the local sessions the scenarios write before
// the helper starts (a fresh install baselines them: only `continue`
// brings them to Teams). Fixed times keep the lists' order and the
// golden stable.
var base = time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

// localSession writes a finished local Codex TUI session in cwd that
// started at start, one turn per prompt/answer pair, last modified an hour
// after it started.
func localSession(s *harness.Scenario, cwd string, start time.Time, pairs ...string) *harness.Rollout {
	s.T.Helper()
	r := harness.NewRolloutAt(s.T, s.Env, cwd, start)
	for i := 0; i+1 < len(pairs); i += 2 {
		r.AppendTurn(s.T, pairs[i], "", pairs[i+1])
	}
	if err := r.SetModTime(start.Add(time.Hour)); err != nil {
		s.T.Fatal(err)
	}
	// Real Codex resumes any rollout under $CODEX_HOME/sessions by its id;
	// the fake app-server resumes only the threads in its own store, so
	// the session is registered there with its rollout (turns run on it
	// are appended to that file, as with real Codex).
	st := fakecodex.Store{Dir: filepath.Join(s.Env.CodexHome, "e2e-fakecodex")}
	if err := st.Save(&fakecodex.Thread{ID: r.SessionID, Cwd: cwd, CreatedAt: start.Unix(), UpdatedAt: start.Add(time.Hour).Unix(),
		RolloutPath: r.Path, Turns: []*fakecodex.Turn{}}); err != nil {
		s.T.Fatal(err)
	}
	return r
}

// replyAfter waits for the first SUT message in chatID after seq that
// contains sub and returns its plain text.
func replyAfter(s *harness.Scenario, chatID string, seq int64, sub string, timeout time.Duration) (string, bool) {
	var got string
	ok := s.WaitFor(fmt.Sprintf("reply containing %q", sub), timeout, func() (bool, string) {
		var last string
		for _, m := range s.SUTMessages(chatID) {
			if m.RequestSeq <= seq {
				continue
			}
			last = harness.NormalizeSpace(m.PlainText())
			if strings.Contains(last, harness.NormalizeSpace(sub)) {
				got = last
				return true, ""
			}
		}
		return false, "last reply: " + clip(last, 200)
	})
	return got, ok
}

// command posts text in chatID and checks that the reply contains want.
func command(s *harness.Scenario, chatID, text, want string) string {
	s.T.Helper()
	seq := s.Graph.LastSeq()
	steps.MustPost(s, chatID, text)
	got, ok := replyAfter(s, chatID, seq, want, steps.CommandTimeout)
	s.Check(fmt.Sprintf("`%s` answers %q", text, clip(want, 60)), ok, want, clip(got, 300))
	return got
}

func clip(s string, n int) string {
	s = harness.NormalizeSpace(s)
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

// chatsNow lists the chats that exist now.
func chatsNow(s *harness.Scenario) map[string]bool {
	out := map[string]bool{}
	for _, c := range s.Graph.Chats() {
		out[c.ID] = true
	}
	return out
}

// newChat waits for a chat not in before.
func newChat(s *harness.Scenario, before map[string]bool, what string) fakegraph.ChatInfo {
	s.T.Helper()
	var chat fakegraph.ChatInfo
	s.Require(what, steps.CommandTimeout, func() (bool, string) {
		for _, c := range s.Graph.Chats() {
			if !before[c.ID] && !strings.Contains(c.Topic, harness.ControlChatTopicMarker) {
				chat = c
				return true, ""
			}
		}
		return false, "no new chat"
	})
	return chat
}

// countIn counts the helper's messages in chatID containing sub.
func countIn(s *harness.Scenario, chatID, sub string) int {
	return steps.CountContaining(s.SUTMessages(chatID), sub)
}

// importTitle starts the message an import posts first; it names the
// session by its first prompt.
const importTitle = "Imported Codex session history"

// onceEach checks that each text is in chatID exactly once (outside the
// import titles, which name the session by its first prompt).
func onceEach(s *harness.Scenario, chatID, where string, texts ...string) {
	s.T.Helper()
	for _, x := range texts {
		n := 0
		for _, m := range s.SUTMessages(chatID) {
			text := harness.NormalizeSpace(m.PlainText())
			if strings.Contains(text, harness.NormalizeSpace(x)) && (strings.Contains(x, importTitle) || !strings.Contains(text, importTitle)) {
				n++
			}
		}
		s.Check(fmt.Sprintf("%q is posted once in %s", clip(x, 50), where), n == 1, "1", fmt.Sprint(n))
	}
}
