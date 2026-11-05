// Package steps holds black-box scenario steps shared by the scenario
// packages: driving the control chat, creating work chats and waiting for
// answers, using only the fake Teams service and the fake Codex.
package steps

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
)

// Generous upper bounds; the old core polls a hot chat every ~1 s and the
// control chat every ~5 s.
const (
	StartupTimeout = 30 * time.Second
	CommandTimeout = 30 * time.Second
	AnswerTimeout  = 30 * time.Second
)

// ReadyControlChat waits for the control chat, its ready notice, and one
// SUT poll of it after the notice (so a following user message is not part
// of the initial baseline).
func ReadyControlChat(s *harness.Scenario) fakegraph.ChatInfo {
	s.T.Helper()
	control := s.WaitControlChat(StartupTimeout)
	if _, ok := s.WaitSUTMessage(control.ID, "control chat is ready", StartupTimeout); !ok {
		s.Finish()
		s.T.FailNow()
	}
	if _, ok := s.WaitPolled(control.ID, s.Graph.LastSeq(), StartupTimeout); !ok {
		s.Finish()
		s.T.FailNow()
	}
	return control
}

// NewWorkChat sends `new <dir>` in the control chat and waits for the work
// chat to be ready and polled.
func NewWorkChat(s *harness.Scenario, control fakegraph.ChatInfo, name string) (fakegraph.ChatInfo, string) {
	s.T.Helper()
	dir := s.Env.WorkDir(s.T, name)
	before := map[string]bool{}
	for _, c := range s.Graph.Chats() {
		before[c.ID] = true
	}
	if _, err := s.Graph.PostAsUser(control.ID, "new "+dir); err != nil {
		s.T.Fatalf("post new: %v", err)
	}
	var work fakegraph.ChatInfo
	s.Require("work chat created for "+name, CommandTimeout, func() (bool, string) {
		for _, c := range s.Graph.Chats() {
			if !before[c.ID] && !strings.Contains(c.Topic, harness.ControlChatTopicMarker) {
				work = c
				return true, ""
			}
		}
		return false, fmt.Sprintf("control transcript: %s", strings.Join(s.Transcript(control.ID, 4), " | "))
	})
	if _, ok := s.WaitSUTMessage(work.ID, "Work chat is ready", CommandTimeout); !ok {
		s.Finish()
		s.T.FailNow()
	}
	if _, ok := s.WaitPolled(work.ID, s.Graph.LastSeq(), CommandTimeout); !ok {
		s.Finish()
		s.T.FailNow()
	}
	s.Note("work chat %q for %s", work.Topic, filepath.Base(dir))
	return work, dir
}

// CountContaining counts messages whose whitespace-normalised text contains sub.
func CountContaining(msgs []fakegraph.Message, sub string) int {
	n := 0
	for _, m := range msgs {
		if strings.Contains(harness.NormalizeSpace(m.PlainText()), harness.NormalizeSpace(sub)) {
			n++
		}
	}
	return n
}

// AnswerPostMatcher matches a recorded POST whose body carries the fake
// Codex answer for token.
func AnswerPostMatcher(token string) func(fakegraph.Request) bool {
	return func(r fakegraph.Request) bool {
		return strings.Contains(r.Body, "E2EANSWER") && strings.Contains(r.Body, token)
	}
}

// AnswerPosts returns the recorded POSTs into chatID that carried the
// answer for token (including faulted attempts).
func AnswerPosts(s *harness.Scenario, chatID, token string) []fakegraph.Request {
	var out []fakegraph.Request
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Method: "POST", ChatID: chatID}) {
		if AnswerPostMatcher(token)(r) {
			out = append(out, r)
		}
	}
	return out
}

// Outcome returns the fake-Codex outcome of the first call, or "no call".
func Outcome(cs []harness.CodexCall) string {
	if len(cs) == 0 {
		return "no call"
	}
	return cs[0].Outcome
}

// MustPost posts text into chatID as the human owner.
func MustPost(s *harness.Scenario, chatID, text string) {
	s.T.Helper()
	if _, err := s.Graph.PostAsUser(chatID, text); err != nil {
		s.T.Fatalf("post: %v", err)
	}
}

// WaitAnswered waits until the fake Codex answer for token is visible in
// chatID and returns how long that took.
func WaitAnswered(s *harness.Scenario, chatID, token string, timeout time.Duration) (time.Duration, bool) {
	start := time.Now()
	ok := s.WaitFor("answer for "+token, timeout, func() (bool, string) {
		for _, c := range s.Codex.CallsWithToken(token) {
			if c.Answer != "" && len(s.SUTMessagesContaining(chatID, c.Answer)) > 0 {
				return true, ""
			}
		}
		return false, fmt.Sprintf("%d codex calls", len(s.Codex.CallsWithToken(token)))
	})
	return time.Since(start), ok
}

// ChatsWithSUTText returns the non-control chats where some SUT message
// contains sub.
func ChatsWithSUTText(s *harness.Scenario, sub string) []fakegraph.ChatInfo {
	var out []fakegraph.ChatInfo
	for _, c := range s.Graph.Chats() {
		if strings.Contains(c.Topic, harness.ControlChatTopicMarker) {
			continue
		}
		if len(s.SUTMessagesContaining(c.ID, sub)) > 0 {
			out = append(out, c)
		}
	}
	return out
}

// UserRecords counts SUT messages that contain the labelled user record for
// prompt (the helper renders mirrored prompts as "User: <text>").
func UserRecords(s *harness.Scenario, chatID, prompt string) int {
	return len(s.SUTMessagesContaining(chatID, "User: "+prompt))
}

var sessionIDRE = regexp.MustCompile(`(?:Work chat created|Local Codex chat detected): (s\d+)`)

// SessionID returns the helper's session id ("s001") announced in a work
// chat, or "".
func SessionID(s *harness.Scenario, chatID string) string {
	for _, m := range s.SUTMessages(chatID) {
		if sm := sessionIDRE.FindStringSubmatch(m.PlainText()); sm != nil {
			return sm[1]
		}
	}
	return ""
}

// ThreadRollout returns the rollout file the fake Codex app-server wrote
// for threadID (like real Codex, under $CODEX_HOME/sessions), so a scenario
// can continue that session "in the TUI".
func ThreadRollout(s *harness.Scenario, threadID string) (*rollout.Writer, error) {
	st := fakecodex.Store{Dir: filepath.Join(s.Env.CodexHome, "e2e-fakecodex")}
	th, err := st.Load(threadID)
	if err != nil {
		return nil, err
	}
	if th.RolloutPath == "" {
		return nil, fmt.Errorf("thread %s has no rollout", threadID)
	}
	w := rollout.Open(th.RolloutPath, th.ID, th.Cwd)
	s.Env.TrackRollout(w)
	return w, nil
}

// HistoryWatchReady writes a fresh local TUI session and waits until it is
// mirrored, proving the helper's history watcher has finished its first
// scan (the old core silently baselines every rollout that exists when that
// unobservable scan runs). It retries with a new session up to 3 times and
// notes each miss.
func HistoryWatchReady(s *harness.Scenario) bool {
	s.T.Helper()
	for attempt := 1; attempt <= 3; attempt++ {
		r := harness.NewRollout(s.T, s.Env, s.Env.WorkDir(s.T, fmt.Sprintf("probe-%d", attempt)))
		answer := s.Token("probe")
		r.AppendTurn(s.T, "Probe session", "", "Probe answer "+answer)
		if s.Poll(15*time.Second, func() bool { return len(ChatsWithSUTText(s, answer)) > 0 }) {
			return true
		}
		s.Note("history probe %d not mirrored within 15s (first-start baseline race?)", attempt)
	}
	return false
}
