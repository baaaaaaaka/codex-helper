package commands

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// workCalls lists the work-chat Codex prompts after the first `before`
// calls.
func workCalls(s *harness.Scenario, before int) []string {
	var out []string
	for _, c := range s.Codex.Calls()[before:] {
		if c.Kind == "work" {
			out = append(out, clip(c.Prompt, 60))
		}
	}
	return out
}

// The work-chat command grammar of v0.1.20 (dashboard_commands.go):
// the prefixes `helper`, `helper:`, `!`, `/`, `cx`, `codex`; the short
// names after `!`, `/`, `cx`; the bare words `help`, `model`, `models`,
// `effort`, `efforts`, `default`. None of them reaches Codex; `!x` with an
// unknown name gets the unknown-command reply, while an unknown `/x` is a
// prompt for Codex (as in v0.1.20).
func TestWorkChatCommandForms(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CMD-WORK-FORMS", harness.Options{
		Guarantees: []string{"F-TRN-7", "F-TRN-8", "F-WRK-3"},
		Title:      "work chat: `!x` `/x` `cx x` `codex x` `helper: x`, short names and bare `help/model/effort/default` run as commands; unknown `!x` is answered, unknown `/x` goes to Codex",
		Budget:     150 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "forms")
	before := len(s.Codex.Calls())
	for _, c := range []struct{ text, want string }{
		{"!status", "queued. Directory:"},
		{"/status", "queued. Directory:"},
		{"cx st", "queued. Directory:"},
		{"codex status", "queued. Directory:"},
		{"Helper: status", "queued. Directory:"},
		{"!st", "queued. Directory:"},
		{"!x", "Nothing to cancel"},
		{"cx rt", "usage: helper retry last"},
		{"help", "💬 Work chat"},
		{"model", "Chat: Work"},
		{"models", "Verified models"},
		{"effort", "Current chat effort (Work)"},
		{"efforts", "Current chat effort choices"},
		{"default model gpt-5", "can only be used in the Control chat"},
		{"!frobnicate", "unknown work chat command: frobnicate"},
		{"!rs", "unknown work chat command: rs"},
		{"!f out/report.md", "Teams file upload rejected: the file was not found."},
	} {
		got, ok := command(s, work.ID, c.text, c.want)
		s.Check(fmt.Sprintf("`%s` runs as a command", c.text), ok, c.want, clip(got, 160))
	}
	s.Settle(time.Second, 4*time.Second)
	sent := workCalls(s, before)
	s.Check("no command reaches Codex", len(sent) == 0, "0 work turns", strings.Join(sent, " | "))

	// An unknown slash name is text for Codex (v0.1.20: "Other text,
	// including unknown slash-prefixed text, is sent to Codex").
	token := s.Token("slash")
	steps.MustPost(s, work.ID, "/explain Please answer "+token)
	_, answered := steps.WaitAnswered(s, work.ID, token, steps.AnswerTimeout)
	s.Check("an unknown `/x` is a prompt for Codex", answered && len(s.Codex.CallsWithToken(token)) == 1, "answered once",
		fmt.Sprintf("answered=%v turns=%d", answered, len(s.Codex.CallsWithToken(token))))
}

// The control chat's forms: `!x`, `/x`, `helper x` and the short names;
// `helper …` work-chat commands get "⚠️ Wrong chat"; unknown `!x` the
// unknown-command reply with the help; a bare number with no list the
// v0.1.20 hint.
func TestControlChatCommandForms(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CMD-CONTROL-FORMS", harness.Options{
		Guarantees: []string{"F-CTL-16"},
		Title:      "control chat: `!st` `/status` `helper st` run; `helper close|retry|file` get ⚠️ Wrong chat; `!frobnicate` gets the unknown-command help",
		Budget:     120 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	before := len(s.Codex.Calls())
	for _, c := range []struct{ text, want string }{
		{"!st", "Control status: no linked work chats yet."},
		{"/status", "Control status: no linked work chats yet."},
		{"helper st", "Control status: no linked work chats yet."},
		{"helper close", "⚠️ Wrong chat"},
		{"helper retry last", "⚠️ Wrong chat"},
		{"helper file out.txt", "⚠️ Wrong chat"},
		{"!frobnicate", "unknown control command: frobnicate"},
		{"cx p", "usage: continue <number-or-session-id>"}, // v0.1.20: legacy `p` is continue
		{"2", "I do not have a current list yet"},
	} {
		got, ok := command(s, control.ID, c.text, c.want)
		s.Check(fmt.Sprintf("`%s` is answered", c.text), ok, c.want, clip(got, 200))
	}
	s.Settle(time.Second, 4*time.Second)
	s.Check("no control command reaches Codex", len(s.Codex.Calls()) == before, fmt.Sprint(before), fmt.Sprint(len(s.Codex.Calls())))
}

// `helper close` (v0.1.20 B:8007): the reply, then the chat is no longer
// read: a later message there starts nothing, and the session leaves the
// control chat's list.
func TestHelperClose(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CMD-CLOSE", harness.Options{
		Guarantees: []string{"F-WRK-4", "F-WRK-3"},
		Title:      "`helper close` answers with v0.1.20's text; the chat is no longer read and leaves `list`",
		Budget:     120 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "close")
	_, ok := ask(s, work.ID, "before")
	s.Check("the chat answers before `helper close`", ok, "answered", fmt.Sprint(ok))
	s.WaitSUTMessage(control.ID, "Codex finished", steps.CommandTimeout)

	got, ok := command(s, work.ID, "helper close", "Session closed")
	want := "Session closed. The helper will no longer read or respond in this Teams chat. This chat remains visible in Teams. Use the 🏠 control chat to open or create another work chat."
	s.Check("`helper close` is answered with v0.1.20's text", ok && strings.HasSuffix(harness.NormalizeSpace(got), want), want, harness.NormalizeSpace(got))
	closedAt := s.Graph.LastSeq()
	after := s.Token("after")
	steps.MustPost(s, work.ID, "Please answer "+after)
	s.Settle(3*time.Second, 10*time.Second)
	s.Check("a message after `helper close` starts nothing", len(s.Codex.CallsWithToken(after)) == 0, "0 turns", fmt.Sprint(len(s.Codex.CallsWithToken(after))))
	var replies []string
	for _, m := range s.SUTMessages(work.ID) {
		if m.RequestSeq > closedAt {
			replies = append(replies, clip(m.PlainText(), 80))
		}
	}
	s.Check("the closed chat gets no reply", len(replies) == 0, "0 messages", strings.Join(replies, " | "))
	list, ok := command(s, control.ID, "list", "session")
	s.Check("the closed session leaves `list`", ok && !strings.Contains(list, "close"), "no closed session listed", clip(list, 200))
}

// Help (v0.1.20 B:5123, B:5150, B:5216, B:5241, adapted to the owner's
// removals and the core's own commands).
func TestHelpTexts(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CMD-HELP", harness.Options{
		Guarantees: []string{"F-CTL-9"},
		Title:      "control `help` / `help advanced` and work `helper help` / `helper help advanced` are v0.1.20's texts without removed commands",
		Budget:     120 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "help")
	for _, c := range []struct {
		chat, text, want string
		has              []string
	}{
		{control.ID, "help", "Send help advanced for all commands.", []string{"🏠 Control chat", "p / projects - choose a workspace", "shutdown - stop the helper on all machines"}},
		{control.ID, "help advanced", "Control chat advanced help", []string{"Workspace flow:", "cxp teams stop / cxp teams start", "codex update now"}},
		{work.ID, "helper help", "Send helper help advanced for retry, cancel, and rename commands.", []string{"💬 Work chat", "helper close or !close"}},
		{work.ID, "!help advanced", "Work chat advanced help", []string{"helper unpause", "Other text, including unknown slash-prefixed text, is sent to Codex."}},
	} {
		got, ok := command(s, c.chat, c.text, c.want)
		norm := harness.NormalizeSpace(got)
		missing := []string{}
		for _, h := range c.has {
			if !strings.Contains(norm, h) {
				missing = append(missing, h)
			}
		}
		s.Check(fmt.Sprintf("`%s` is v0.1.20's help", c.text), ok && len(missing) == 0, strings.Join(c.has, "; "), fmt.Sprintf("missing %q in %s", missing, clip(norm, 200)))
		low := strings.ToLower(norm)
		var removed []string
		for _, r := range []string{"beacon", "restore-thread", "reload", "model setup"} {
			if strings.Contains(low, r) {
				removed = append(removed, r)
			}
		}
		s.Check(fmt.Sprintf("`%s` lists no removed command", c.text), len(removed) == 0, "none", strings.Join(removed, ", "))
	}
}

// Control-chat attachments (v0.1.20 text.go:113/:121, B:4626): an image or
// file is refused with v0.1.20's text and the command does not run; a
// quote with an explicit command gets the quote variant.
func TestControlChatAttachments(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CTL-ATTACH", harness.Options{
		Guarantees: []string{"F-CTL-16"},
		Title:      "control chat: an image, a file or a quoted command is refused with v0.1.20's attachment text and runs nothing",
		Budget:     120 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	png := []byte("\x89PNG\r\n\x1a\nE2E-control-image")
	seq := s.Graph.LastSeq()
	if _, err := s.Graph.PostImageAsUser(control.ID, "status", png); err != nil {
		t.Fatal(err)
	}
	got, ok := replyAfter(s, control.ID, seq, "I could not process", steps.CommandTimeout)
	want := "I could not process 1 Teams attachment(s): Teams-hosted inline content."
	s.Check("an image is refused with v0.1.20's text", ok && strings.Contains(got, want) && strings.Contains(got, "Files and images belong in a 💬 Work chat"),
		want+" … Files and images belong in a 💬 Work chat", clip(harness.NormalizeSpace(got), 300))

	seq = s.Graph.LastSeq()
	if _, err := s.Graph.PostFileAsUser(control.ID, "st", "notes.txt", []byte("notes")); err != nil {
		t.Fatal(err)
	}
	got, ok = replyAfter(s, control.ID, seq, "I could not process", steps.CommandTimeout)
	s.Check("a file is refused with its content type", ok && strings.Contains(got, "1 Teams attachment(s): reference."), "1 Teams attachment(s): reference.",
		clip(harness.NormalizeSpace(got), 200))

	seq = s.Graph.LastSeq()
	if _, err := s.Graph.PostWithQuotesAsUser(control.ID, "status", fakegraph.QuoteSpec{MessageID: "1", Preview: "earlier", SenderName: "E2E Owner"}); err != nil {
		t.Fatal(err)
	}
	got, ok = replyAfter(s, control.ID, seq, "I could not process", steps.CommandTimeout)
	s.Check("a quote with an explicit command is refused with the quote variant", ok && strings.Contains(got, "explicit control commands do not consume quoted context"),
		"… explicit control commands do not consume quoted context.", clip(harness.NormalizeSpace(got), 300))
	s.Settle(time.Second, 4*time.Second)
	n := 0
	for _, m := range s.SUTMessages(control.ID) {
		if strings.Contains(m.PlainText(), "Speech recognition:") {
			n++ // a `status` reply (the control status list)
		}
	}
	s.Check("the refused commands do not run", n == 0, "0 status replies", fmt.Sprint(n))
}
