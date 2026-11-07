package commands

import (
	"encoding/json"
	"fmt"
	"html"
	"os"
	"path/filepath"
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
	hi, ok := askNotified(s, control.ID, work.ID, "high")
	s.Check("the next turn runs at the chosen effort", ok && hi.Effort == "high", "effort high", fmt.Sprintf("effort=%q", hi.Effort))
	expect(s, work.ID, "helper effort low", "Reasoning effort updated", "Previous: `high`", "Effective for next turn: `low`")
	expect(s, work.ID, "helper model effort status", "Current chat effort (Work)", "Effective for next turn: `low`", "Source: chat override")
	expect(s, work.ID, "helper effort turbo", "is not supported by model", `reasoning effort "turbo" is not supported by model "gpt-5-codex"`)
	expect(s, work.ID, "helper effort help", "Reasoning effort commands:", "`effort reset`")
	expect(s, work.ID, "helper effort reset", "Reasoning effort updated", "Effective for next turn: `medium`", "Source: model-advertised default")
	md, ok := askNotified(s, control.ID, work.ID, "medium")
	s.Check("after `effort reset` the next turn runs at the model's default", ok && md.Effort == "medium", "effort medium", fmt.Sprintf("effort=%q", md.Effort))

	expect(s, control.ID, "effort", "Current chat effort (Control)", "Effective for next turn: `low`", "Source: helper fallback")
	expect(s, control.ID, "effort set high", "Reasoning effort updated for this Control chat", "Effective for next turn: `high`")
	q, ok := askControl(s, control.ID, "ctlhigh")
	s.Check("a control question runs at the Control chat's effort", ok && q.Effort == "high", "effort high", fmt.Sprintf("effort=%q", q.Effort))
	expect(s, control.ID, "efforts", "Current chat effort choices for GPT-5-Codex:", "- `high` (current)")
	expect(s, control.ID, "effort reset", "Reasoning effort updated for this Control chat", "Effective for next turn: `medium`")
	w, ok := askNotified(s, control.ID, work.ID, "after")
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
	m, eff := configDefaults(s)
	s.Check("the global defaults are kept in the cxp config, as before the rebuilt core", m == "official:gpt-5-mini" && eff == "medium",
		"defaults.model official:gpt-5-mini, defaults.reasoningEffort medium", fmt.Sprintf("model=%q reasoningEffort=%q", m, eff))
	expect(s, control.ID, "default effort set high", "is not supported by global default model", `reasoning effort "high" is not supported by global default model "gpt-5-mini"`)
	expect(s, control.ID, "default colour set red", "unknown global default", `unknown global default "colour"`)
	expect(s, control.ID, "default model frob", "unknown default action", `unknown default action "frob"`)
	expect(s, control.ID, "default model set", "usage: `default model set <value>`")
	expect(s, control.ID, "default help", "Global default commands (Control chat only):")

	fresh, _ := steps.NewWorkChat(s, control, "fresh")
	n, ok := askNotified(s, control.ID, fresh.ID, "fresh")
	s.Check("a new chat takes the global defaults", ok && n.Model == "gpt-5-mini" && n.Effort == "medium",
		"gpt-5-mini, medium", fmt.Sprintf("model=%q effort=%q", n.Model, n.Effort))
	expect(s, fresh.ID, "helper model", "Selection: global default", "Effective effort: `medium` (global default)",
		"Global default for new chats: Codex Official (`gpt-5-mini`)")
	o, ok := askNotified(s, control.ID, old.ID, "old")
	s.Check("an existing chat keeps its model (the account default)", ok && o.Model == "gpt-5-codex" && o.Effort == "medium", "gpt-5-codex, medium", fmt.Sprintf("model=%q effort=%q", o.Model, o.Effort))
	expect(s, old.ID, "helper default status", "can only be used in the Control chat")

	expect(s, control.ID, "default model reset", "Global default model reset to the built-in Codex account default.", "Previous selector: `official:gpt-5-mini`")
	expect(s, control.ID, "default effort reset", "Global default effort reset (previous configured effort: `medium`).")
	expect(s, control.ID, "default status", "Configured selector: `default`", "Configured: `unset`")
	m, eff = configDefaults(s)
	s.Check("a reset clears the cxp config's defaults", m == "" && eff == "", "no defaults.model, no defaults.reasoningEffort", fmt.Sprintf("model=%q reasoningEffort=%q", m, eff))
	n2, ok := askNotified(s, control.ID, fresh.ID, "fresh2")
	s.Check("resetting the defaults does not change a chat created with them", ok && n2.Model == "gpt-5-mini" && n2.Effort == "medium",
		"gpt-5-mini, medium", fmt.Sprintf("model=%q effort=%q", n2.Model, n2.Effort))
}

// configPath is the SUT's cxp config file.
func configPath(s *harness.Scenario) string {
	return filepath.Join(s.Env.ConfigDir, "codex-proxy", "config.json") // os.UserConfigDir with XDG_CONFIG_HOME
}

// configDefaults reads the cxp config's global defaults.
func configDefaults(s *harness.Scenario) (model, effort string) {
	var cfg struct {
		Defaults struct {
			Model           string `json:"model"`
			ReasoningEffort string `json:"reasoningEffort"`
		} `json:"defaults"`
	}
	if b, err := os.ReadFile(configPath(s)); err == nil {
		_ = json.Unmarshal(b, &cfg)
	}
	return cfg.Defaults.Model, cfg.Defaults.ReasoningEffort
}

// Global defaults set by an older release (cxp config `defaults.model` /
// `defaults.reasoningEffort`, as `default model|effort set` wrote them
// before the rebuilt core) apply to new chats after the upgrade; other
// config keys are kept when `default` changes them.
func TestGlobalDefaultsFromTheConfig(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CMD-DEFAULT-CONFIG", harness.Options{
		Guarantees: []string{"F-CTL-15"},
		Title:      "the cxp config's defaults.model official:gpt-5-mini and defaults.reasoningEffort medium, written by an older release: `default status` shows them, a new chat runs on them",
		Budget:     120 * time.Second,
		NoStart:    true,
	})
	defer s.Finish()
	cfg := map[string]any{}
	if b, err := os.ReadFile(configPath(s)); err == nil {
		if err := json.Unmarshal(b, &cfg); err != nil {
			t.Fatalf("config: %v", err)
		}
	}
	cfg["defaults"] = map[string]any{"model": "official:gpt-5-mini", "reasoningEffort": "medium", "e2eKeep": "kept"}
	b, _ := json.MarshalIndent(cfg, "", "  ")
	if err := os.MkdirAll(filepath.Dir(configPath(s)), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath(s), b, 0o600); err != nil {
		t.Fatal(err)
	}
	s.StartSUT()
	control := steps.ReadyControlChat(s)
	expect(s, control.ID, "default status", "Configured selector: `official:gpt-5-mini`", "Effective for new chats: `gpt-5-mini`", "Configured: `medium`")
	fresh, _ := steps.NewWorkChat(s, control, "fresh")
	n, ok := askNotified(s, control.ID, fresh.ID, "fresh")
	s.Check("a new chat takes the config's defaults", ok && n.Model == "gpt-5-mini" && n.Effort == "medium",
		"gpt-5-mini, medium", fmt.Sprintf("model=%q effort=%q", n.Model, n.Effort))
	expect(s, control.ID, "default effort set low", "Global default effort: `low`", "Previous configured effort: `medium`")
	var after struct {
		Defaults map[string]string `json:"defaults"`
	}
	raw, _ := os.ReadFile(configPath(s))
	_ = json.Unmarshal(raw, &after)
	s.Check("`default effort set` writes the config and keeps its other keys",
		after.Defaults["model"] == "official:gpt-5-mini" && after.Defaults["reasoningEffort"] == "low" && after.Defaults["e2eKeep"] == "kept",
		"model official:gpt-5-mini, reasoningEffort low, e2eKeep kept", fmt.Sprintf("%v", after.Defaults))
}

// MODEL-SHOWN (F-TRN-7; field report 2026-11-07): the model and effort a
// chat shows are the ones its turns run with. Codex's own config.toml
// names another model and effort, which model/list does not mark as its
// default; a chat without a model of its own used to show the account
// default but leave the turn's model out, so Codex ran the config's model
// and the answer's footer named another model than `model status`. As
// v0.1.20 did, every turn sends the model and effort the chat shows.
func TestModelShownIsTheModelRun(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "MODEL-SHOWN", harness.Options{
		Guarantees: []string{"F-TRN-7"},
		Title:      "with another model in Codex's config.toml, a chat's turns run with the model and effort `model status` shows, and the footer names them",
		Budget:     120 * time.Second,
		NoStart:    true,
	})
	defer s.Finish()
	config := "model = \"gpt-5-mini\"\nmodel_reasoning_effort = \"high\"\n"
	if err := os.WriteFile(filepath.Join(s.Env.CodexHome, "config.toml"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	s.StartSUT()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "shown")

	footer := func(chatID string, c harness.CodexCall) string {
		for _, m := range s.SUTMessagesContaining(chatID, c.Answer) {
			if text := harness.NormalizeSpace(m.PlainText()); strings.Contains(text, "Helper: model - ") {
				return text[strings.Index(text, "Helper: model - "):]
			}
		}
		return ""
	}
	expect(s, work.ID, "model", "Chat: Work", "Effective: Codex Official (`gpt-5-codex`)", "Effective effort: `medium` (Codex runtime default)")
	w, ok := ask(s, work.ID, "work")
	s.Check("a Work chat's turn runs with the model and effort `model status` shows", ok && w.Model == "gpt-5-codex" && w.Effort == "medium",
		"gpt-5-codex, medium", fmt.Sprintf("model=%q effort=%q", w.Model, w.Effort))
	f := footer(work.ID, w)
	s.Check("the answer's footer names them", strings.HasPrefix(f, "Helper: model - gpt-5-codex, effort - medium,"),
		"Helper: model - gpt-5-codex, effort - medium, …", clip(f, 200))

	expect(s, work.ID, "helper model switch gpt-5-mini", "Model profile switched for this Work chat",
		"Effective next-turn model: Codex Official (`gpt-5-mini`)")
	expect(s, work.ID, "model", "Effective: Codex Official (`gpt-5-mini`)", "Effective effort: `low`")
	m, ok := ask(s, work.ID, "mini")
	f = footer(work.ID, m)
	s.Check("after a switch the turn and its footer follow the chat's model and effort", ok && m.Model == "gpt-5-mini" && m.Effort == "low" &&
		strings.HasPrefix(f, "Helper: model - gpt-5-mini, effort - low,"), "gpt-5-mini, low", fmt.Sprintf("model=%q effort=%q footer %s", m.Model, m.Effort, clip(f, 200)))

	// The answer and its control notification use independent outbox rows.
	// Wait for both Work completions before recording the control exchange;
	// their ordering against a later user message is otherwise scheduler-dependent.
	s.Require("both Work completion notices are delivered", 15*time.Second, func() (bool, string) {
		n := len(s.SUTMessagesContaining(control.ID, "✅ Codex finished"))
		return n == 2, fmt.Sprint(n)
	})

	expect(s, control.ID, "model", "Chat: Control", "Effective: Codex Official (`gpt-5-codex`)", "Effective effort: `low` (helper fallback)")
	q, ok := askControl(s, control.ID, "ctl")
	s.Check("a control question runs with the Control chat's model and effort", ok && q.Model == "gpt-5-codex" && q.Effort == "low",
		"gpt-5-codex, low", fmt.Sprintf("model=%q effort=%q", q.Model, q.Effort))
	s.Settle(time.Second, 4*time.Second)
}
