package chats

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Voice messages through the external-command ASR backend (decision: keep
// "external command" and llama.cpp; one E2E per failure class). A Teams
// voice clip (audio card + hosted content) is downloaded, transcribed by
// CODEX_HELPER_TEAMS_ASR_COMMAND and run as a prompt. Failure classes:
// recogniser heard nothing (no_speech), recogniser cannot load
// (native_load), recogniser missing (not_configured), recogniser crashed
// (backend). v0.1.20 (bridge.go prepareExecutionInput) answers a failed
// transcription with "Teams media transcription failed: … Next: …" and does
// not run Codex on it.
func TestVoiceExternalCommand(t *testing.T) {
	harness.NeedsCLI(t) // the voice path is wired in the cxp entry point only
	harness.Parallel(t)
	tools := t.TempDir()
	mode := filepath.Join(tools, "mode")
	script := filepath.Join(tools, "asr.sh")
	body := `#!/bin/sh
case "$(cat '` + mode + `' 2>/dev/null)" in
ok) printf 'transcribed: %s\n' "$(cat "$1")" ;;
empty) exit 0 ;;
native) echo "asr: error while loading shared libraries: libggml.so: cannot open shared object file: No such file or directory" >&2; exit 127 ;;
crash) echo "asr: recogniser crashed" >&2; exit 3 ;;
esac
`
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	setMode := func(m string) {
		if err := os.WriteFile(mode, []byte(m), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	setMode("ok")
	s := harness.NewScenario(t, "VOICE", harness.Options{
		Guarantees: []string{"F-ATT-voice", "R-37", "R-22"},
		Title:      "voice clip transcribed by the external ASR command runs as a prompt; each failure class gets a visible reason and no Codex turn",
		Budget:     150 * time.Second,
		UserEnv:    map[string]string{"CODEX_HELPER_TEAMS_ASR_COMMAND": script},
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "voice")

	ok := s.Token("voiceok")
	if _, err := s.Graph.PostVoiceAsUser(work.ID, "voice.ogg", []byte("please list the files "+ok)); err != nil {
		t.Fatal(err)
	}
	_, answered := steps.WaitAnswered(s, work.ID, ok, steps.AnswerTimeout)
	prompt := ""
	if cs := s.Codex.CallsWithToken(ok); len(cs) > 0 {
		prompt = cs[0].Prompt
	}
	s.Check("a voice clip is transcribed and answered", answered && strings.Contains(prompt, "transcribed: please list the files "+ok) &&
		strings.Contains(prompt, "Automatic local ASR transcript from Microsoft Teams media."), "1 turn whose prompt carries the transcript",
		fmt.Sprintf("answered=%v prompt=%q", answered, clip(prompt, 300)), s.Transcript(work.ID, 8)...)

	cases := []struct {
		name, mode, reason string
		prep, undo         func()
	}{
		{name: "no speech", mode: "empty", reason: "no speech was recognised"},
		{name: "recogniser cannot load (native_load)", mode: "native", reason: "cannot run on this machine"},
		{name: "recogniser crashed (backend)", mode: "crash", reason: "voice recognition failed"},
		{name: "recogniser missing (not_configured)", mode: "ok", reason: "not set up",
			prep: func() { _ = os.Rename(script, script+".gone") }, undo: func() { _ = os.Rename(script+".gone", script) }},
	}
	for i, c := range cases {
		setMode(c.mode)
		if c.prep != nil {
			c.prep()
		}
		callsBefore := len(s.Codex.Calls())
		from := s.Graph.LastSeq()
		if _, err := s.Graph.PostVoiceAsUser(work.ID, fmt.Sprintf("clip%d.ogg", i), []byte("words")); err != nil {
			t.Fatal(err)
		}
		visible := s.Poll(30*time.Second, func() bool { return len(sutAfterContaining(s, work.ID, from, c.reason)) > 0 })
		s.Settle(2*time.Second, 8*time.Second)
		if c.undo != nil {
			c.undo()
		}
		var replies []string
		for _, m := range s.SUTMessages(work.ID) {
			if m.RequestSeq > from {
				replies = append(replies, clip(harness.NormalizeSpace(m.PlainText()), 200))
			}
		}
		turns := len(s.Codex.Calls()) - callsBefore
		s.Check(c.name+": the reason is shown in the chat", visible, "a helper message containing "+fmt.Sprintf("%q", c.reason),
			fmt.Sprintf("replies %q", replies))
		s.Check(c.name+": no Codex turn runs on a failed transcription", turns == 0, "0 turns (v0.1.20)",
			fmt.Sprintf("%d turns; prompt %q", turns, lastPrompt(s, callsBefore)))
	}

	setMode("ok")
	again := s.Token("again")
	if _, err := s.Graph.PostVoiceAsUser(work.ID, "again.ogg", []byte("and again "+again)); err != nil {
		t.Fatal(err)
	}
	_, ok2 := steps.WaitAnswered(s, work.ID, again, steps.AnswerTimeout)
	s.Check("after the recogniser is back a clip is answered again", ok2, "answered", fmt.Sprint(ok2))
}

func sutAfterContaining(s *harness.Scenario, chatID string, seq int64, sub string) []string {
	var out []string
	for _, m := range s.SUTMessages(chatID) {
		if t := harness.NormalizeSpace(m.PlainText()); m.RequestSeq > seq && strings.Contains(strings.ToLower(t), strings.ToLower(sub)) {
			out = append(out, t)
		}
	}
	return out
}

func lastPrompt(s *harness.Scenario, from int) string {
	cs := s.Codex.Calls()
	if len(cs) <= from {
		return ""
	}
	return clip(cs[len(cs)-1].Prompt, 300)
}
