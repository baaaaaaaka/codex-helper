package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Global defaults (gap batch B, F-CTL-15): the model and reasoning effort
// newly created chats start with, managed from the control chat with
// v0.1.20's `default …` grammar (default_commands.go) and texts
// (cli/teams_defaults.go). They live where the helper kept them before
// the rebuild: the cxp config's `defaults.model` and
// `defaults.reasoningEffort` (Config.Defaults), so the settings made
// with an older release still apply. Without a config (engine tests)
// they are kept in the store. A chat takes them when it is created
// (`new`, `model fork`, the control chat's first question); existing
// chats are unchanged.
const (
	metaDefaultModel  = "engine_default_model"  // "" unset, "default" the account default, else a model id
	metaDefaultEffort = "engine_default_effort" // "" unset
)

// DefaultsFile reads and writes the global defaults. model is "" (unset),
// "default" (the account default) or a model id; effort "" is unset.
type DefaultsFile interface {
	ReadDefaults() (model, effort string, err error)
	WriteDefaults(model, effort string) error
}

type globalDefaults struct{ Model, Effort string }

func (e *Engine) readGlobalDefaults(tx *store.Tx) (globalDefaults, error) {
	var d globalDefaults
	var err error
	if f := e.cfg.Defaults; f != nil {
		d.Model, d.Effort, err = f.ReadDefaults()
		return d, err
	}
	if d.Model, _, err = tx.Meta(metaDefaultModel); err != nil {
		return d, err
	}
	d.Effort, _, err = tx.Meta(metaDefaultEffort)
	return d, err
}

func (e *Engine) writeGlobalDefaults(tx *store.Tx, d globalDefaults) error {
	if f := e.cfg.Defaults; f != nil {
		return f.WriteDefaults(d.Model, d.Effort)
	}
	if err := tx.SetMeta(metaDefaultModel, d.Model); err != nil {
		return err
	}
	return tx.SetMeta(metaDefaultEffort, d.Effort)
}

// newChatThread is the thread row of a newly created chat: the global
// default model and effort (v0.1.20 resolveNewSessionModelProfile("") and
// reasoningEffortFromGlobalDefault; the effort was checked against the
// default model when it was set). A config that cannot be read leaves the
// chat on the account default rather than failing its creation.
func (e *Engine) newChatThread(tx *store.Tx, conv int64, cwd string) (store.Thread, error) {
	d, err := e.readGlobalDefaults(tx)
	if err != nil {
		e.logf("global defaults: %v; the new chat uses the Codex account default", err)
		d = globalDefaults{}
	}
	th := store.Thread{ConvID: conv, Cwd: cwd, Model: selector(d.Model), ModelSource: modelSrcGlobalDefault}
	if d.Effort != "" {
		th.Effort, th.EffortSource = d.Effort, effortSrcGlobalDefault
	}
	return th, nil
}

const textDefaultScope = "newly created Teams chats; existing chats are unchanged."

// defaultSettings are v0.1.20's registered settings and their aliases.
var defaultSettings = []struct{ key, desc string }{
	{"model", "model used by newly created Teams chats"},
	{"effort", "reasoning effort used by newly created Teams chats"},
}

func defaultSetting(ref string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(ref)) {
	case "model", "model-profile":
		return "model", true
	case "effort", "reasoning-effort", "thinking-effort":
		return "effort", true
	}
	return "", false
}

func globalDefaultUsage(setting string) string {
	if setting != "" {
		return strings.Join([]string{
			"Global default `" + setting + "` commands:",
			"- `default " + setting + " status`",
			"- `default " + setting + " list`",
			"- `default " + setting + " set <value>`",
			"- `default " + setting + " reset`",
		}, "\n")
	}
	lines := []string{"Global default commands (Control chat only):", "- `default status` - show every registered global default"}
	for _, s := range defaultSettings {
		lines = append(lines, "- `default "+s.key+" status|list|set|reset` - "+s.desc)
	}
	return strings.Join(lines, "\n")
}

// cmdDefaultControl is `default …` in the control chat (v0.1.20
// parseDefaultCommand + HandleDefaultCommand).
func (e *Engine) cmdDefaultControl(ctx context.Context, j cmdJob, arg string) {
	setting, rest := commandParts(arg)
	action, value := "", ""
	switch {
	case setting == "" || setting == "status" || setting == "current":
		if rest != "" {
			e.reply(j, "usage: `default status`", nil)
			return
		}
		setting, action = "", "status"
	case setting == "help" || setting == "?":
		e.reply(j, globalDefaultUsage(""), nil)
		return
	default:
		action, value = commandParts(rest)
		switch action {
		case "", "status", "current":
			action = "status"
		case "list", "ls", "options", "choices":
			if value != "" {
				e.reply(j, "usage: `default "+setting+" list`", nil)
				return
			}
			action = "list"
		case "set", "use", "switch":
			if value == "" {
				e.reply(j, "usage: `default "+setting+" set <value>`", nil)
				return
			}
			action = "set"
		case "reset", "clear", "unset":
			if value != "" {
				e.reply(j, "usage: `default "+setting+" reset`", nil)
				return
			}
			action = "reset"
		case "help", "?":
			action = "help"
		default:
			e.reply(j, fmt.Sprintf("unknown default action %q; use `default %s status|list|set|reset`", action, setting), nil)
			return
		}
	}
	key := ""
	if setting != "" {
		var ok bool
		if key, ok = defaultSetting(setting); !ok {
			e.reply(j, fmt.Sprintf("unknown global default %q; available settings: `model`, `effort`", setting), nil)
			return
		}
	}
	if action == "help" {
		e.reply(j, globalDefaultUsage(key), nil)
		return
	}
	models, err := e.ag.Models(ctx)
	if err != nil {
		e.reply(j, "Codex did not list its models ("+userError(err.Error())+"). Try again later.", nil)
		return
	}
	var d globalDefaults
	if err := e.update(func(tx *store.Tx) error {
		var err error
		d, err = e.readGlobalDefaults(tx)
		return err
	}); err != nil {
		e.reply(j, "Could not read the global defaults: "+userError(err.Error()), nil)
		return
	}
	switch key + " " + action {
	case " status":
		e.reply(j, strings.Join([]string{
			"Global defaults (Control chat only)",
			"Scope: newly created Teams chats.",
			"Does not change: this Control chat or existing Work chats.",
			"", defaultModelStatus(models, d),
			"", defaultEffortStatus(models, d),
		}, "\n"), nil)
	case "model status":
		e.reply(j, defaultModelStatus(models, d), nil)
	case "model list":
		e.reply(j, "Global default model choices\nScope: "+textDefaultScope+"\n\n"+verifiedModels(models, d), nil)
	case "model set":
		e.defaultModelSet(j, models, d, value)
	case "model reset":
		e.defaultModelSet(j, models, d, "")
	case "effort status":
		e.reply(j, defaultEffortStatus(models, d), nil)
	case "effort list":
		m, _ := resolveModel(models, selector(d.Model))
		if len(m.Efforts) == 0 {
			e.reply(j, fmt.Sprintf("model %q did not advertise reasoning effort choices", firstNonEmpty(m.ID, d.Model)), nil)
			return
		}
		lines := []string{"Global default effort choices for `" + m.ID + "`", "Scope: " + textDefaultScope}
		for _, opt := range m.Efforts {
			var marks []string
			if strings.EqualFold(opt, d.Effort) {
				marks = append(marks, "global override")
			}
			if strings.EqualFold(opt, m.Effort) {
				marks = append(marks, "model-advertised default")
			}
			line := "- `" + opt + "`"
			if len(marks) > 0 {
				line += " (" + strings.Join(marks, ", ") + ")"
			}
			lines = append(lines, line)
		}
		e.reply(j, strings.Join(lines, "\n"), nil)
	case "effort set":
		m, _ := resolveModel(models, selector(d.Model))
		eff, ok := findEffort(m, value)
		if !ok || len(m.Efforts) == 0 {
			e.reply(j, fmt.Sprintf("reasoning effort %q is not supported by global default model %q; available values: %s",
				value, firstNonEmpty(m.ID, d.Model), strings.Join(m.Efforts, ", ")), nil)
			return
		}
		e.reply(j, "Global default effort: `"+eff+"`\nPrevious configured effort: `"+firstNonEmpty(d.Effort, "unset")+"`\nScope: "+textDefaultScope,
			func(tx *store.Tx) error { return e.writeGlobalDefaults(tx, globalDefaults{d.Model, eff}) })
	case "effort reset":
		e.reply(j, "Global default effort reset (previous configured effort: `"+firstNonEmpty(d.Effort, "unset")+"`). New chats will use the selected model's advertised default when available, otherwise the Codex runtime fallback. Existing chats are unchanged.",
			func(tx *store.Tx) error { return e.writeGlobalDefaults(tx, globalDefaults{d.Model, ""}) })
	}
}

// configuredSelector is v0.1.20's EffectiveDefaultModelSelector.
func configuredSelector(model string) string {
	if model == "" || strings.EqualFold(model, "default") {
		return "default"
	}
	return "official:" + model
}

func defaultModelStatus(models []Model, d globalDefaults) string {
	m, _ := resolveModel(models, selector(d.Model))
	source := "Codex account default"
	if d.Model != "" {
		source = "global override"
	}
	return strings.Join([]string{
		"Global default model",
		"Configured selector: `" + configuredSelector(d.Model) + "`",
		"Effective for new chats: `" + firstNonEmpty(m.ID, selector(d.Model), "Codex account default (exact slug unavailable)") + "`",
		"Provider: `Codex Official`",
		"Source: " + source,
		"Applies to: " + textDefaultScope,
	}, "\n")
}

func defaultEffortStatus(models []Model, d globalDefaults) string {
	m, _ := resolveModel(models, selector(d.Model))
	effective, source := d.Effort, "global override"
	if effective == "" {
		if d.Model != "" && m.Effort != "" {
			effective, source = m.Effort, "model-advertised default"
		} else {
			effective, source = "runtime fallback", "Codex runtime fallback"
		}
	}
	return strings.Join([]string{
		"Global default effort",
		"Configured: `" + firstNonEmpty(d.Effort, "unset") + "`",
		"Effective for new chats: `" + effective + "`",
		"Model: `" + firstNonEmpty(m.ID, selector(d.Model), "Codex account default (exact slug unavailable)") + "`",
		"Source: " + source,
		"Applies to: " + textDefaultScope,
	}, "\n")
}

// defaultModelSet is v0.1.20's setGlobalDefaultModel (value "" is
// `default model reset`): the effort override is kept when the new model
// supports it, else reset to the model's default.
func (e *Engine) defaultModelSet(j cmdJob, models []Model, d globalDefaults, value string) {
	stored := ""
	if value != "" {
		v := strings.Trim(strings.TrimSpace(value), "`")
		v = strings.TrimPrefix(v, "official:")
		if strings.EqualFold(v, "default") {
			stored = "default"
		} else {
			m, ok := findModel(models, v)
			if !ok {
				e.reply(j, fmt.Sprintf("Unknown model %q. Send `default model list` to see the models of this Codex account.", value), nil)
				return
			}
			stored = m.ID
		}
	}
	m, _ := resolveModel(models, selector(stored))
	effort, notice := d.Effort, ""
	if effort != "" {
		if _, ok := findEffort(m, effort); !ok || len(m.Efforts) == 0 {
			switch {
			case len(m.Efforts) == 0 && m.Effort == "":
				effort, notice = "", "Global default effort `"+d.Effort+"` was cleared because the selected model does not advertise reasoning effort."
			case m.Effort == "":
				e.reply(j, fmt.Sprintf("model %q does not advertise a valid default reasoning effort", m.ID), nil)
				return
			default:
				effort, notice = m.Effort, "Global default effort `"+d.Effort+"` is not supported by the selected model and was reset to `"+m.Effort+"`."
			}
		}
	}
	var text string
	if value == "" {
		text = "Global default model reset to the built-in Codex account default. Previous selector: `" + configuredSelector(d.Model) + "`. Scope: " + textDefaultScope
		if notice != "" {
			text += "\n" + notice
		}
	} else {
		lines := []string{
			"Global default model: `" + configuredSelector(stored) + "`",
			"Previous selector: `" + configuredSelector(d.Model) + "`",
			"Effective value for new chats: `" + firstNonEmpty(m.ID, "Codex account default (exact slug unavailable)") + "`",
			"Applies to newly created Teams chats; this Control chat is unchanged.",
		}
		if notice != "" {
			lines = append(lines, notice)
		}
		text = strings.Join(lines, "\n")
	}
	e.reply(j, text, func(tx *store.Tx) error { return e.writeGlobalDefaults(tx, globalDefaults{stored, effort}) })
}
