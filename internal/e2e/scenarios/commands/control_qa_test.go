package commands

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// controlAck is the head of v0.1.20's control-chat question ACK
// (queueControlFallbackAck B:7488).
const controlAck = "Codex received your control-chat question."

// sutAfter lists the SUT messages of chatID posted after seq.
func sutAfter(s *harness.Scenario, chatID string, seq int64) []fakegraph.Message {
	var out []fakegraph.Message
	for _, m := range s.SUTMessages(chatID) {
		if m.RequestSeq > seq {
			out = append(out, m)
		}
	}
	return out
}

// CTL-QA (F-CTL-10): a question in the control chat is answered there by
// Codex, as in v0.1.20 (runControlFallback): `ask <question>` and text
// that is not a command or a path get v0.1.20's ACK naming the turn, one
// Codex turn with the control envelope (low effort, the control-chat
// history) and one answer that mentions the owner; no "Codex finished"
// notice follows. A bare `ask` gets the usage; `helper cancel` cancels a
// running control question; the control thread is never mirrored as a
// local session.
func TestControlQuestion(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CTL-QA", harness.Options{
		Guarantees: []string{"F-CTL-10"},
		Title:      "a control-chat question is acknowledged and answered by Codex in the control chat, once",
		Budget:     150 * time.Second,
	})
	defer s.Finish()
	slow := s.Token("slow")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(slow) {
			return harness.CodexBehavior{Block: true}
		}
		return harness.CodexBehavior{}
	})
	control := steps.ReadyControlChat(s)
	chatsBefore := len(s.Graph.Chats())

	// 1. `ask <question>`.
	q1 := s.Token("ask")
	from := s.Graph.LastSeq()
	steps.MustPost(s, control.ID, "ask what does "+q1+" mean?")
	_, answered := steps.WaitAnswered(s, control.ID, q1, steps.AnswerTimeout)
	calls := s.Codex.CallsWithToken(q1)
	var c1 harness.CodexCall
	if len(calls) > 0 {
		c1 = calls[0]
	}
	s.Check("`ask` runs one Codex turn and the answer is posted in the control chat", answered && len(calls) == 1,
		"1 call, answered", fmt.Sprintf("calls=%d answered=%v", len(calls), answered), s.Transcript(control.ID, 6)...)
	s.Check("the question gets v0.1.20's control ACK naming its turn",
		steps.CountContaining(sutAfter(s, control.ID, from), controlAck) == 1 &&
			steps.CountContaining(sutAfter(s, control.ID, from), "helper cancel turn-") == 1,
		"1 ACK with `helper cancel turn-<n>`", strings.Join(s.Transcript(control.ID, 4), " | "))
	s.Check("the prompt is v0.1.20's control envelope without `ask`",
		strings.HasPrefix(c1.Prompt, "[codex-helper-control]\n<codex-helper-control-context version=\"1\">") &&
			strings.HasSuffix(c1.Prompt, "User message:\nwhat does "+q1+" mean?") &&
			strings.Contains(c1.Prompt, "Current helper state:") && strings.Contains(c1.Prompt, "Relevant cxp / Teams helper help digest:"),
		"control envelope, question last", clip(c1.Prompt, 200))
	s.Check("a control question runs at low effort in the home directory", c1.Effort == "low" && c1.Cwd != "",
		"effort low, cwd set", fmt.Sprintf("effort=%q cwd=%q", c1.Effort, c1.Cwd))
	var answer fakegraph.Message
	for _, m := range sutAfter(s, control.ID, from) {
		if c1.Answer != "" && strings.Contains(m.PlainText(), c1.Answer) {
			answer = m
		}
	}
	s.Check("the answer mentions the owner", len(answer.Mentions) > 0, "an @mention on the answer", fmt.Sprintf("%d mentions", len(answer.Mentions)))

	// 2. Plain text is a question too; its prompt carries the history.
	q2 := s.Token("plain")
	steps.MustPost(s, control.ID, "How do I start project work? "+q2)
	_, answered = steps.WaitAnswered(s, control.ID, q2, steps.AnswerTimeout)
	calls = s.Codex.CallsWithToken(q2)
	prompt := ""
	if len(calls) > 0 {
		prompt = calls[0].Prompt
	}
	s.Check("plain control text is a Codex question", answered && len(calls) == 1, "1 call, answered", fmt.Sprintf("calls=%d answered=%v", len(calls), answered))
	s.Check("the prompt carries the recent control-chat history", strings.Contains(prompt, "- recent_control_chat_history:") &&
		strings.Contains(prompt, "user/control_inbound:\n  what does "+q1+" mean?") &&
		strings.Contains(prompt, "control-chat-history.txt"),
		"the first question in the history", clip(prompt, 200))
	s.Check("the same control thread continues", len(calls) == 1 && c1.ThreadID != "" && calls[0].ThreadID == c1.ThreadID,
		"one thread", fmt.Sprintf("%q vs %q", c1.ThreadID, func() string {
			if len(calls) > 0 {
				return calls[0].ThreadID
			}
			return ""
		}()))

	// 3. A bare `ask` gets the usage and runs nothing.
	n := len(s.Codex.Calls())
	got, ok := command(s, control.ID, "ask", "usage: ask <question>")
	s.Check("a bare `ask` gets v0.1.20's usage", ok && len(s.Codex.Calls()) == n, "usage, no call", clip(got, 120))

	// 4. Cancel a running question.
	steps.MustPost(s, control.ID, "please think about "+slow)
	s.WaitCodexCalls(slow, 1, steps.AnswerTimeout)
	got, ok = command(s, control.ID, "helper cancel running", "cancel requested for running turn")
	s.Check("`helper cancel running` cancels the running control question", ok && strings.Contains(got, "please think about "+slow),
		"cancel requested, prompt named", clip(got, 200))
	s.WaitFor("the cancelled question ends", steps.AnswerTimeout, func() (bool, string) {
		cs := s.Codex.CallsWithToken(slow)
		return len(cs) == 1 && cs[0].Outcome == "cancelled", steps.Outcome(cs)
	})
	got, ok = command(s, control.ID, "helper cancel last", "no running or queued turn")
	s.Check("nothing left to cancel", ok, "v0.1.20 text", clip(got, 120))
	s.Settle(2*time.Second, 10*time.Second)

	// No "Codex finished" notice for a control answer, no mirrored chat.
	s.Check("a control answer gets no separate finished notice", steps.CountContaining(s.SUTMessages(control.ID), "The answer is ready in the work chat") == 0,
		"0 notices", fmt.Sprint(steps.CountContaining(s.SUTMessages(control.ID), "The answer is ready in the work chat")))
	s.Check("the control thread is not mirrored into a chat", len(s.Graph.Chats()) == chatsBefore,
		fmt.Sprint(chatsBefore)+" chats", fmt.Sprint(len(s.Graph.Chats())))
}

// CTL-QA-RESTART (F-CTL-10, R-4): a control question sent while the helper
// is down is answered once after it starts, with one ACK; a second restart
// replays nothing.
func TestControlQuestionAcrossRestart(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CTL-QA-RESTART", harness.Options{
		Guarantees: []string{"F-CTL-10"},
		Title:      "a control question sent while the helper is down is acknowledged and answered exactly once",
		Budget:     150 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	s.StopSUT()
	q := s.Token("down")
	steps.MustPost(s, control.ID, "what is the status of "+q+"?")
	time.Sleep(500 * time.Millisecond)
	s.StartSUT()
	_, answered := steps.WaitAnswered(s, control.ID, q, 60*time.Second)
	s.Settle(3*time.Second, 15*time.Second)
	s.StopSUT()
	s.StartSUT()
	if _, ok := s.WaitPolled(control.ID, s.Graph.LastSeq(), 30*time.Second); ok {
		s.Settle(4*time.Second, 15*time.Second)
	}
	calls := s.Codex.CallsWithToken(q)
	msgs := s.SUTMessages(control.ID)
	acks := steps.CountContaining(msgs, controlAck)
	answers := 0
	if len(calls) == 1 && calls[0].Answer != "" {
		answers = steps.CountContaining(msgs, calls[0].Answer)
	}
	s.Check("the question runs exactly once", answered && len(calls) == 1, "1 call", fmt.Sprint(len(calls)), s.Transcript(control.ID, 10)...)
	s.Check("one ACK and one answer", acks == 1 && answers == 1, "1 ACK, 1 answer", fmt.Sprintf("%d ACKs, %d answers", acks, answers))
}

// CTL-QA-VOICE (F-RND-11, F-CTL-10): a voice clip in the control chat is
// transcribed and asked as a control question, as v0.1.20 ran control-chat
// media through runControlFallback: one ACK, one Codex turn whose prompt
// carries the transcript in the control envelope, one answer.
func TestControlVoiceQuestion(t *testing.T) {
	harness.NeedsCLI(t) // the voice path is wired in the cxp entry point only
	harness.Parallel(t)
	script := filepath.Join(t.TempDir(), "asr.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf 'transcribed: %s\\n' \"$(cat \"$1\")\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	s := harness.NewScenario(t, "CTL-QA-VOICE", harness.Options{
		Guarantees: []string{"F-RND-11", "F-CTL-10"},
		Title:      "a voice clip in the control chat is transcribed and answered there as a control question",
		Budget:     120 * time.Second,
		UserEnv:    map[string]string{"CODEX_HELPER_TEAMS_ASR_COMMAND": script},
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	q := s.Token("voice")
	from := s.Graph.LastSeq()
	if _, err := s.Graph.PostVoiceAsUser(control.ID, "question.ogg", []byte("what is "+q+"?")); err != nil {
		t.Fatal(err)
	}
	_, answered := steps.WaitAnswered(s, control.ID, q, steps.AnswerTimeout)
	s.Settle(2*time.Second, 10*time.Second)
	calls := s.Codex.CallsWithToken(q)
	prompt := ""
	if len(calls) > 0 {
		prompt = calls[0].Prompt
	}
	s.Check("the clip runs one control question and is answered", answered && len(calls) == 1, "1 call, answered",
		fmt.Sprintf("calls=%d answered=%v", len(calls), answered), s.Transcript(control.ID, 6)...)
	s.Check("the prompt is the control envelope carrying the transcript",
		strings.HasPrefix(prompt, "[codex-helper-control]\n") && strings.Contains(prompt, "transcribed: what is "+q+"?"),
		"control envelope + transcript", clip(prompt, 300))
	s.Check("one control ACK", steps.CountContaining(sutAfter(s, control.ID, from), controlAck) == 1, "1 ACK",
		strings.Join(s.Transcript(control.ID, 4), " | "))
}
