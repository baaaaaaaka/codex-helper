package commands

import (
	"fmt"
	"html"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// askControl asks a control-chat question and returns its Codex call.
func askControl(s *harness.Scenario, chatID, label string) (harness.CodexCall, bool) {
	token := s.Token(label)
	steps.MustPost(s, chatID, "ask what is "+token+"?")
	_, ok := steps.WaitAnswered(s, chatID, token, steps.AnswerTimeout)
	calls := s.Codex.CallsWithToken(token)
	if len(calls) != 1 {
		return harness.CodexCall{}, false
	}
	return calls[0], ok
}

// plain is a reply text as fakegraph's PlainText shows it: no code
// marks, no list bullets.
func plain(p string) string {
	return harness.NormalizeSpace(strings.TrimPrefix(strings.ReplaceAll(p, "`", ""), "- "))
}

// expect posts a command and checks that the reply contains every part
// (Markdown written as the reply's plain text shows it).
func expect(s *harness.Scenario, chatID, text string, parts ...string) string {
	got, ok := command(s, chatID, text, plain(parts[0]))
	norm := html.UnescapeString(harness.NormalizeSpace(got))
	var missing []string
	for _, p := range parts[1:] {
		if !strings.Contains(norm, plain(p)) {
			missing = append(missing, p)
		}
	}
	s.Check(fmt.Sprintf("`%s` gets v0.1.20's reply", text), ok && len(missing) == 0, strings.Join(parts, " + "),
		fmt.Sprintf("missing %q in %s", missing, clip(norm, 400)))
	return norm
}

// CMD-MODEL (F-TRN-7): v0.1.20's `model` grammar in both chats, for the
// Codex models of the account: bare `model` / `model status`, `models` /
// `model list` (and a natural-language list question), `model switch|use
// <m>` keeping the Codex thread and reconciling the effort, `model reset`
// to the global default, `model fork <m>` into a new Work chat on a new
// thread; removed subcommands and unknown models refused. The control
// chat's own model is CMD-MODEL-CTL.
func TestModelCommands(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CMD-MODEL", harness.Options{
		Guarantees: []string{"cmd-model", "F-TRN-7"},
		Title:      "`model status|list|switch|use|reset|fork` in a Work chat, with v0.1.20's texts",
		Budget:     240 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, dir := steps.NewWorkChat(s, control, "model")

	expect(s, work.ID, "model", "Chat: Work", "Effective: Codex Official (`gpt-5-codex`)", "Selection: global default",
		"Effective effort: `medium` (Codex runtime default)", "Switchable: yes.")
	expect(s, work.ID, "models", "Verified models", "Current chat model (Work)", "Codex account default: GPT-5-Codex (`gpt-5-codex`)",
		"- GPT-5-Mini (`gpt-5-mini`) effort=low")
	expect(s, work.ID, "helper model what models are available?", "Verified models")
	before, ok1 := ask(s, work.ID, "default")

	expect(s, work.ID, "helper model switch gpt-5-mini", "Model profile switched for this Work chat",
		"Previous: Codex Official (`gpt-5-codex`)", "Effective next-turn model: Codex Official (`gpt-5-mini`)", "Selection: chat override",
		"The next message will continue the current Codex thread")
	mini, ok2 := ask(s, work.ID, "mini")
	s.Check("the next turn runs on the chosen model with its default effort, on the same thread",
		ok1 && ok2 && mini.Model == "gpt-5-mini" && mini.Effort == "low" && mini.ThreadID == before.ThreadID,
		"gpt-5-mini, low, same thread", fmt.Sprintf("model=%q effort=%q thread %q -> %q", mini.Model, mini.Effort, before.ThreadID, mini.ThreadID))
	expect(s, work.ID, "helper model use gpt-5-mini", "Model profile already selected for this Work chat: Codex Official (`gpt-5-mini`)")
	expect(s, work.ID, "helper effort set high", "is not supported by model", `reasoning effort "high" is not supported by model "gpt-5-mini"`, "available values: `low`, `medium`")
	expect(s, work.ID, "helper model switch gpt-5-codex", "Model profile switched for this Work chat")
	expect(s, work.ID, "helper effort high", "Reasoning effort updated for this Work chat", "Effective for next turn: `high`")
	expect(s, work.ID, "helper model switch gpt-5-mini", "Model profile switched",
		"Your explicit reasoning effort `high` is not supported by the target model; it was reset to `low`.")
	expect(s, work.ID, "helper model switch no-such-model", "Unknown model")
	expect(s, work.ID, "helper model no-such-model", "Work chat model commands:", "`model fork <name>`")
	expect(s, work.ID, "helper model switch", "usage: `model switch <model>`")
	expect(s, work.ID, "helper model provider", "Unknown model command `provider`", "Third-party model setup was removed")

	expect(s, work.ID, "helper model reset", "Model profile switched for this Work chat", "Selection: global default")
	back, ok3 := ask(s, work.ID, "reset")
	// Codex keeps a turn's model for the thread's later turns, so the
	// reset names the account default.
	s.Check("after `model reset` the next turn runs on the account default", ok3 && back.Model == "gpt-5-codex" && back.Effort == "medium",
		"gpt-5-codex, medium", fmt.Sprintf("model=%q effort=%q", back.Model, back.Effort))

	// `model fork <m>`: a new Work chat in the same directory, on a new
	// thread, pinned to the model; the link comes back in this chat.
	known := map[string]bool{}
	for _, c := range s.Graph.Chats() {
		known[c.ID] = true
	}
	seq := s.Graph.LastSeq()
	steps.MustPost(s, work.ID, "helper model fork gpt-5-mini")
	link, linked := replyAfter(s, work.ID, seq, "Forked Work chat", steps.CommandTimeout)
	var fork fakegraph.ChatInfo
	s.WaitFor("fork chat created", steps.CommandTimeout, func() (bool, string) {
		for _, c := range s.Graph.Chats() {
			if !known[c.ID] {
				fork = c
				return true, ""
			}
		}
		return false, fmt.Sprint(len(s.Graph.Chats()), " chats")
	})
	s.Check("`model fork` answers with v0.1.20's text and the new chat's link", linked && strings.Contains(link, "with model profile gpt-5-mini:") && fork.ID != "",
		"Forked Work chat … with model profile gpt-5-mini: <link>", clip(link, 200))
	if fork.ID != "" {
		ready, ok := s.WaitSUTMessage(fork.ID, "Work chat is ready", steps.CommandTimeout)
		s.Check("the fork's ready anchor names the model", ok && strings.Contains(ready.PlainText(), "Execution target: model:gpt-5-mini"),
			"Execution target: model:gpt-5-mini", clip(ready.PlainText(), 300))
		s.WaitPolled(fork.ID, seq, steps.CommandTimeout)
		inFork, ok := ask(s, fork.ID, "infork")
		s.Check("a prompt in the fork runs on the model, in the same directory, on a new thread",
			ok && inFork.Model == "gpt-5-mini" && inFork.Cwd == dir && inFork.ThreadID != "" && inFork.ThreadID != before.ThreadID,
			"gpt-5-mini, same cwd, new thread", fmt.Sprintf("model=%q cwd=%q thread=%q", inFork.Model, inFork.Cwd, inFork.ThreadID))
	}

	s.Settle(time.Second, 4*time.Second)
	var replies []string
	for _, id := range []string{work.ID, control.ID} {
		for _, m := range s.SUTMessages(id) {
			replies = append(replies, m.PlainText())
		}
	}
	leaks := steps.InternalLeaks(s, strings.Join(replies, " "))
	s.Check("model replies carry no internal ids", len(leaks) == 0, "none", fmt.Sprint(leaks))
}

// CMD-MODEL-CTL (F-TRN-7): the control chat's own model: `model`, `model
// list`, `model switch <m>` (the control questions run on it), `model
// default` pointing to the `default` command, `model fork` refused with
// the usage, `model reset`. A scenario of its own: its control questions
// carry the control-chat history, which a work chat's notices (with their
// temp-directory paths) would push over the prompt's byte limit, so the
// cut would move with the path length.
func TestControlModelCommands(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CMD-MODEL-CTL", harness.Options{
		Guarantees: []string{"F-TRN-7"},
		Title:      "`model status|list|switch|reset` in the Control chat change the control questions' model, with v0.1.20's texts",
		Budget:     120 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	expect(s, control.ID, "model", "Chat: Control", "Effective effort: `low` (helper fallback)")
	expect(s, control.ID, "model list", "Verified models", "Current chat model (Control)")
	expect(s, control.ID, "model switch gpt-5-mini", "Model profile switched for this Control chat", "Effective next-turn model: Codex Official (`gpt-5-mini`)")
	q, ok := askControl(s, control.ID, "ctlmini")
	s.Check("a control question runs on the Control chat's model", ok && q.Model == "gpt-5-mini" && q.Effort == "low",
		"gpt-5-mini, low", fmt.Sprintf("model=%q effort=%q", q.Model, q.Effort))
	expect(s, control.ID, "model default", "Global defaults moved to the Control-only `default` command.")
	expect(s, control.ID, "model fork gpt-5-mini", "Model commands:")
	expect(s, control.ID, "model reset", "Model profile switched for this Control chat", "Selection: global default")
	s.Settle(time.Second, 4*time.Second)
	var replies []string
	for _, m := range s.SUTMessages(control.ID) {
		replies = append(replies, m.PlainText())
	}
	leaks := steps.InternalLeaks(s, strings.Join(replies, " "))
	s.Check("model replies carry no internal ids", len(leaks) == 0, "none", fmt.Sprint(leaks))
}

// CMD-EFFORT (F-TRN-8): v0.1.20's `effort` grammar in both chats: bare
// `effort` / `effort status`, `efforts` / `effort list`, `effort set <v>`
// and the bare value, `effort reset` (the model's advertised default), an
// unsupported value and the usage; `model effort …` is the same command.
func TestEffortCommands(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CMD-EFFORT", harness.Options{
		Guarantees: []string{"F-TRN-8"},
		Title:      "`effort status|list|set|reset` in a Work chat and in the Control chat, with v0.1.20's texts; the next turn uses the effort",
		Budget:     180 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "effort")

	expect(s, work.ID, "effort", "Current chat effort (Work)", "Effective for next turn: `medium`", "Source: Codex runtime default",
		"Model for this chat: `gpt-5-codex`", "Model-advertised default: `medium`", "Available: `low`, `medium`, `high`")
	expect(s, work.ID, "efforts", "Current chat effort choices for GPT-5-Codex:", "- `medium` (current, model-advertised default)",
		"Use `effort set <value>` to switch this chat.")
	expect(s, work.ID, "helper effort set", "usage: `effort set <value>`")
	expect(s, work.ID, "helper effort set high", "Reasoning effort updated for this Work chat", "Previous: `unset`",
		"Effective for next turn: `high`", "Model: `gpt-5-codex`", "Source: chat override")
	hi, ok := ask(s, work.ID, "high")
	s.Check("the next turn runs at the chosen effort", ok && hi.Effort == "high", "effort high", fmt.Sprintf("effort=%q", hi.Effort))
	expect(s, work.ID, "helper effort low", "Reasoning effort updated", "Previous: `high`", "Effective for next turn: `low`")
	expect(s, work.ID, "helper model effort status", "Current chat effort (Work)", "Effective for next turn: `low`", "Source: chat override")
	expect(s, work.ID, "helper effort turbo", "is not supported by model", `reasoning effort "turbo" is not supported by model "gpt-5-codex"`)
	expect(s, work.ID, "helper effort help", "Reasoning effort commands:", "`effort reset`")
	expect(s, work.ID, "helper effort reset", "Reasoning effort updated", "Effective for next turn: `medium`", "Source: model-advertised default")
	md, ok := ask(s, work.ID, "medium")
	s.Check("after `effort reset` the next turn runs at the model's default", ok && md.Effort == "medium", "effort medium", fmt.Sprintf("effort=%q", md.Effort))

	expect(s, control.ID, "effort", "Current chat effort (Control)", "Effective for next turn: `low`", "Source: helper fallback")
	expect(s, control.ID, "effort set high", "Reasoning effort updated for this Control chat", "Effective for next turn: `high`")
	q, ok := askControl(s, control.ID, "ctlhigh")
	s.Check("a control question runs at the Control chat's effort", ok && q.Effort == "high", "effort high", fmt.Sprintf("effort=%q", q.Effort))
	expect(s, control.ID, "efforts", "Current chat effort choices for GPT-5-Codex:", "- `high` (current)")
	expect(s, control.ID, "effort reset", "Reasoning effort updated for this Control chat", "Effective for next turn: `medium`")
	w, ok := ask(s, work.ID, "after")
	s.Check("the control chat's effort does not change the Work chat", ok && w.Effort == "medium", "effort medium", fmt.Sprintf("effort=%q", w.Effort))
}

// CMD-DEFAULT (F-CTL-15): v0.1.20's `default` grammar in the Control
// chat: `default` / `default status`, `default model status|list|set|
// reset`, `default effort status|list|set|reset`, effort reconciliation
// when the model changes, unknown settings and actions; a new chat takes
// the defaults, an existing chat and the Control chat do not change; the
// Work chat refuses `default`.
func TestGlobalDefaults(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CMD-DEFAULT", harness.Options{
		Guarantees: []string{"F-CTL-15"},
		Title:      "`default status|model …|effort …` in the Control chat with v0.1.20's texts; new chats take the defaults, existing chats keep theirs",
		Budget:     180 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	old, _ := steps.NewWorkChat(s, control, "old")

	expect(s, control.ID, "default", "Global defaults (Control chat only)", "Does not change: this Control chat or existing Work chats.",
		"Configured selector: `default`", "Source: Codex account default", "Configured: `unset`", "Effective for new chats: `runtime fallback`")
	expect(s, control.ID, "default model list", "Global default model choices", "Verified models", "- GPT-5-Mini (`gpt-5-mini`)")
	expect(s, control.ID, "default model set no-such-model", "Unknown model")
	expect(s, control.ID, "default effort set high", "Global default effort: `high`", "Previous configured effort: `unset`")
	expect(s, control.ID, "default model set gpt-5-mini", "Global default model: `official:gpt-5-mini`", "Previous selector: `default`",
		"Effective value for new chats: `gpt-5-mini`",
		"Global default effort `high` is not supported by the selected model and was reset to `low`.")
	expect(s, control.ID, "default effort list", "Global default effort choices for `gpt-5-mini`", "- `low` (global override, model-advertised default)")
	expect(s, control.ID, "default effort set medium", "Global default effort: `medium`", "Previous configured effort: `low`")
	expect(s, control.ID, "default status", "Configured selector: `official:gpt-5-mini`", "Source: global override", "Configured: `medium`")
	expect(s, control.ID, "default effort set high", "is not supported by global default model", `reasoning effort "high" is not supported by global default model "gpt-5-mini"`)
	expect(s, control.ID, "default colour set red", "unknown global default", `unknown global default "colour"`)
	expect(s, control.ID, "default model frob", "unknown default action", `unknown default action "frob"`)
	expect(s, control.ID, "default model set", "usage: `default model set <value>`")
	expect(s, control.ID, "default help", "Global default commands (Control chat only):")

	fresh, _ := steps.NewWorkChat(s, control, "fresh")
	n, ok := ask(s, fresh.ID, "fresh")
	s.Check("a new chat takes the global defaults", ok && n.Model == "gpt-5-mini" && n.Effort == "medium",
		"gpt-5-mini, medium", fmt.Sprintf("model=%q effort=%q", n.Model, n.Effort))
	expect(s, fresh.ID, "helper model", "Selection: global default", "Effective effort: `medium` (global default)",
		"Global default for new chats: Codex Official (`gpt-5-mini`)")
	o, ok := ask(s, old.ID, "old")
	s.Check("an existing chat keeps its model", ok && o.Model == "" && o.Effort == "", "Codex default", fmt.Sprintf("model=%q effort=%q", o.Model, o.Effort))
	expect(s, old.ID, "helper default status", "can only be used in the Control chat")

	expect(s, control.ID, "default model reset", "Global default model reset to the built-in Codex account default.", "Previous selector: `official:gpt-5-mini`")
	expect(s, control.ID, "default effort reset", "Global default effort reset (previous configured effort: `medium`).")
	expect(s, control.ID, "default status", "Configured selector: `default`", "Configured: `unset`")
	n2, ok := ask(s, fresh.ID, "fresh2")
	s.Check("resetting the defaults does not change a chat created with them", ok && n2.Model == "gpt-5-mini" && n2.Effort == "medium",
		"gpt-5-mini, medium", fmt.Sprintf("model=%q effort=%q", n2.Model, n2.Effort))
}
