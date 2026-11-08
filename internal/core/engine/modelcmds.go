package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Model and reasoning-effort commands of both chats (gap batch B, F-TRN-7,
// F-TRN-8): v0.1.20's grammar and texts (model_profile_commands.go,
// reasoning_effort_commands.go, model_settings_summary.go) for the Codex
// models of the signed-in account. Third-party model profiles, providers,
// catalogs, key intake and `model doctor` were removed by the owner.
//
// The chat's model and effort live on its thread row; a turn reads them
// when it starts, so a change applies to every turn that starts after it
// (v0.1.20 captured them when a turn was queued).

// Selection sources stored on the thread (v0.1.20's values, which migrated
// threads carry; "user" / "default" are the rebuilt core's earlier ones).
const (
	modelSrcGlobalDefault = "global_default"
	modelSrcChatOverride  = "chat_override"
	modelSrcLegacy        = "legacy"

	effortSrcExplicit        = "explicit"
	effortSrcModelDefault    = "model_default"
	effortSrcExecutorDefault = "executor_default"
	effortSrcHelperDefault   = "helper_default"
	effortSrcGlobalDefault   = "global_default"
)

// textModelRemoved answers the removed third-party model subcommands.
const textModelRemoved = "Third-party model setup was removed from this helper; it uses the Codex models of the signed-in account."

// modelChat is what a model or effort command knows about its chat.
type modelChat struct {
	cs      *convState
	kind    string // "Work" or "Control"
	control bool
	label   string
	running bool
	th      store.Thread
	defs    globalDefaults
	models  []Model
}

// loadModelChat reads the chat's thread, the global defaults and the Codex
// models; "" text means ok, else it is the reply.
func (e *Engine) loadModelChat(ctx context.Context, cs *convState) (modelChat, string) {
	c := modelChat{cs: cs, kind: "Work"}
	e.mu.Lock()
	c.control, c.label, c.running = cs.kind == store.KindControl, cs.label, cs.running != nil
	e.mu.Unlock()
	if c.control {
		c.kind = "Control"
	}
	models, err := e.ag.Models(ctx)
	if err != nil {
		return c, "Codex did not list its models (" + userError(err.Error()) + "). Try again later."
	}
	c.models = models
	err = e.update(func(tx *store.Tx) error {
		th, ok, err := tx.Thread(cs.id)
		if err != nil {
			return err
		}
		if !ok {
			// A chat without a thread row yet (the control chat before
			// its first question) gets the global defaults with it.
			if th, err = e.newChatThread(tx, cs.id, ""); err != nil {
				return err
			}
		}
		c.th = th
		c.defs, err = e.readGlobalDefaults(tx)
		return err
	})
	if err != nil {
		return c, "Could not read this chat's model settings: " + userError(err.Error())
	}
	return c, ""
}

// selector is a stored model name: "" (or v0.1.20's "default") is the
// account default of Codex's model/list (TurnSettings).
func selector(model string) string {
	if strings.EqualFold(strings.TrimSpace(model), "default") {
		return ""
	}
	return strings.TrimSpace(model)
}

func (c modelChat) modelID() string { return selector(firstNonEmpty(c.th.PendingModel, c.th.Model)) }

// resolve finds a selector's model: the account default for "".
func resolveModel(models []Model, sel string) (Model, bool) {
	if sel == "" {
		return defaultModel(models)
	}
	return findModel(models, sel)
}

func (c modelChat) model() (Model, bool) { return resolveModel(c.models, c.modelID()) }

// TurnSettings is the model and effort a turn sends to Codex: the chat's
// selector resolved as `model status` shows it ("" is the account default
// of model/list), and its effort, else that model's default. v0.1.20 sent
// both with every turn; a turn that left them out ran with whatever
// Codex's own config or the thread chose, which the chat did not show.
// A selector Codex does not list is sent as it is.
func TurnSettings(models []Model, sel, effort string) (string, string) {
	m, ok := resolveModel(models, selector(sel))
	if !ok {
		return selector(sel), effort
	}
	return m.ID, firstNonEmpty(effort, m.Effort)
}

// officialLabel is v0.1.20's modelSnapshotExactLabel of a Codex model.
func officialLabel(models []Model, sel string) string {
	id := sel
	if m, ok := resolveModel(models, sel); ok {
		id = m.ID
	}
	if id == "" {
		return "Codex Official (account default; exact slug unavailable)"
	}
	return "Codex Official (`" + id + "`)"
}

// selectorLabel is v0.1.20's modelSnapshotSelectorLabel.
func selectorLabel(sel string) string {
	if sel == "" {
		return "default (dynamic Codex account default)"
	}
	return "official:" + sel
}

func modelSourceLabel(src string) string {
	switch strings.ToLower(strings.TrimSpace(src)) {
	case modelSrcGlobalDefault, "default":
		return "global default"
	case modelSrcChatOverride, "user":
		return "chat override"
	case modelSrcLegacy, "":
		return "legacy session (selection source unavailable)"
	}
	return "selection source unavailable"
}

func effortSourceLabel(src string) string {
	switch strings.ToLower(strings.TrimSpace(src)) {
	case effortSrcExplicit, "user":
		return "chat override"
	case effortSrcGlobalDefault:
		return "global default"
	case effortSrcModelDefault, "default":
		return "model-advertised default"
	case effortSrcExecutorDefault:
		return "Codex runtime default"
	case effortSrcHelperDefault:
		return "helper fallback"
	}
	return "runtime fallback"
}

// effortSource normalizes the rebuilt core's earlier source names.
func effortSource(src string) string {
	switch src {
	case "user":
		return effortSrcExplicit
	case "default":
		return effortSrcModelDefault
	}
	return src
}

// effort is the effort the chat's next turn runs with, and its source.
func (c modelChat) effort() (string, string) {
	if v := firstNonEmpty(c.th.PendingEffort, c.th.Effort); v != "" {
		return v, effortSource(c.th.EffortSource)
	}
	if c.control {
		return ctlEffortFallback, effortSrcHelperDefault // v0.1.20 DefaultControlFallbackReasoningEffort
	}
	if m, ok := c.model(); ok && m.Effort != "" {
		return m.Effort, effortSrcExecutorDefault
	}
	return "runtime fallback", ""
}

// summary is v0.1.20's currentChatModelSummaryLines.
func (c modelChat) summary() []string {
	eff, src := c.effort()
	return []string{
		"Current chat model (" + c.kind + ")",
		"Session: `" + c.label + "`",
		"Effective: " + officialLabel(c.models, c.modelID()),
		"Selector: `" + selectorLabel(c.modelID()) + "`",
		"Selection: " + modelSourceLabel(c.th.ModelSource),
		"Effective effort: `" + eff + "` (" + effortSourceLabel(src) + ")",
		"Applies to: new turns in this chat",
		"Global default for new chats: " + officialLabel(c.models, selector(c.defs.Model)) + " (existing chats are unchanged)",
	}
}

// modelStatusText is v0.1.20's formatChatModelStatus (no Codex thread id:
// the rebuilt core shows no internal ids).
func (c modelChat) modelStatusText() string {
	lines := []string{"Chat: " + c.kind}
	lines = append(lines, c.summary()[1:]...)
	lines = append(lines, "Model profile: "+officialLabel(c.models, c.modelID()))
	if c.running {
		lines = append(lines, "Switchable: yes. The running turn keeps its model; a switch applies from the next turn and preserves the Codex thread.")
	} else {
		lines = append(lines, "Switchable: yes. Switching preserves the existing Codex thread and portable context, if present.")
	}
	return strings.Join(lines, "\n")
}

// verifiedModels is v0.1.20's printTeamsVerifiedModels for the official
// models.
func verifiedModels(models []Model, defs globalDefaults) string {
	lines := []string{"Verified models"}
	def, ok := defaultModel(models)
	if !ok {
		return strings.Join(append(lines, "  official default unavailable: Codex listed no models"), "\n")
	}
	if g, ok := resolveModel(models, selector(defs.Model)); ok {
		lines = append(lines, fmt.Sprintf("  global default: %s (`%s`)", displayName(g), g.ID))
	}
	lines = append(lines, fmt.Sprintf("  Codex account default: %s (`%s`)", displayName(def), def.ID), "", "Official Codex models")
	for _, m := range models {
		suffix := ""
		if m.ID == def.ID {
			suffix = " [account default]"
		}
		if m.Effort != "" {
			suffix += " effort=" + m.Effort
		}
		lines = append(lines, fmt.Sprintf("  - %s (`%s`)%s", displayName(m), m.ID, suffix))
	}
	return strings.Join(lines, "\n")
}

func displayName(m Model) string { return firstNonEmpty(m.DisplayName, m.ID) }

func (c modelChat) modelListText() string {
	return strings.Join(append(c.summary(), "", verifiedModels(c.models, c.defs)), "\n")
}

func modelWorkUsage() string {
	return strings.Join([]string{
		"Work chat model commands:",
		"- `model status` - show this chat's pinned profile",
		"- `model switch <name>` - switch this chat while preserving its Codex thread",
		"- `model reset` - switch this chat to the global default",
		"- `model fork <name>` - create a new Work chat with another profile",
		"- `model list` - show this chat's effective model first, then verified profiles",
		"- `effort list|status|set|reset` - inspect or change this chat's effort",
	}, "\n")
}

func modelControlUsage() string {
	return strings.Join([]string{
		"Model commands:",
		"- `model list` - show this chat's effective model first, then verified models (global default is labeled separately)",
		"- `model status` - show this Control chat's effective model, selection source, effort, and any active/queued model",
		"- `model switch <model>` - switch this Control chat",
		"- `model reset` - switch this Control chat to the global default",
		"- `default model set <model>` - set the global default",
		"- `new <directory> --model <model>` - create a chat pinned to a model",
		"- `effort list|status|set|reset` - inspect or change Control chat effort",
	}, "\n")
}

// modelNaturalLanguageListRequest is v0.1.20's: a read-only question for
// the model list.
func modelNaturalLanguageListRequest(arg string) bool {
	value := strings.ToLower(strings.TrimSpace(arg))
	if value == "" {
		return false
	}
	for _, exact := range []string{"list", "ls", "profiles", "status", "current", "providers", "provider", "setup", "doctor"} {
		if value == exact || strings.HasPrefix(value, exact+" ") {
			return false
		}
	}
	for _, signal := range []string{
		"列出", "列出来", "列表", "有哪些", "有什么模型", "哪些模型", "可用模型", "所有模型", "全部模型",
		"what model", "which model", "models are available", "available model", "list model", "show model", "all model",
	} {
		if strings.Contains(value, signal) {
			return true
		}
	}
	return false
}

// commandParts splits `<sub> <rest>`, sub lower-cased (v0.1.20
// modelCommandParts).
func commandParts(arg string) (string, string) {
	arg = strings.TrimSpace(arg)
	sub, rest, _ := strings.Cut(arg, " ")
	return strings.ToLower(strings.TrimSpace(sub)), strings.TrimSpace(rest)
}

// cmdModelChat is `model …` in either chat.
func (e *Engine) cmdModelChat(ctx context.Context, j cmdJob, cs *convState, arg string) {
	c, failed := e.loadModelChat(ctx, cs)
	if failed != "" {
		e.reply(j, failed, nil)
		return
	}
	if modelNaturalLanguageListRequest(arg) {
		e.reply(j, c.modelListText(), nil)
		return
	}
	sub, rest := commandParts(arg)
	usage := modelWorkUsage
	if c.control {
		usage = modelControlUsage
	}
	switch sub {
	case "", "status", "current":
		e.reply(j, c.modelStatusText(), nil)
	case "list", "ls", "profiles":
		e.reply(j, c.modelListText(), nil)
	case "switch", "use", "set":
		if sub == "set" && !c.control {
			e.reply(j, usage(), nil) // v0.1.20: `set` is a Control-chat alias only
			return
		}
		if rest == "" {
			e.reply(j, "usage: `model switch <model>`", nil)
			return
		}
		e.modelSwitch(j, c, rest, modelSrcChatOverride)
	case "reset":
		e.modelSwitch(j, c, "", modelSrcGlobalDefault)
	case "fork", "new":
		if c.control {
			e.reply(j, usage(), nil)
			return
		}
		if rest == "" {
			e.reply(j, "usage: `model fork <profile>`", nil)
			return
		}
		e.modelFork(ctx, j, c, rest)
	case "effort", "reasoning-effort", "thinking-effort":
		e.effortCommand(j, c, rest)
	case "default", "set-default":
		if c.control {
			e.reply(j, "Global defaults moved to the Control-only `default` command. Use `default model set <model>` or `default model reset`.", nil)
			return
		}
		e.reply(j, usage(), nil)
	case "providers", "provider", "catalog", "catalogs", "subscription", "subscriptions", "setup", "guide", "add", "create",
		"key", "api-key", "apikey", "doctor", "check", "delete", "remove", "rm":
		e.reply(j, "Unknown model command `"+sub+"`. "+textModelRemoved+"\n\n"+usage(), nil)
	default:
		e.reply(j, usage(), nil)
	}
}

// targetModel resolves `model switch|fork <ref>`: "default" is the
// account default.
func (c modelChat) targetModel(ref string) (string, Model, string) {
	ref = strings.Trim(strings.TrimSpace(ref), "`")
	if sel := selector(ref); sel == "" {
		m, _ := defaultModel(c.models)
		return "", m, ""
	}
	m, ok := findModel(c.models, ref)
	if !ok {
		return "", Model{}, fmt.Sprintf("Unknown model %q. Send `model list` to see the models of this Codex account.", ref)
	}
	return m.ID, m, ""
}

// effortForSwitch is v0.1.20's reasoningEffortForModelSwitch: an explicit
// effort the target supports stays; otherwise the target's advertised
// default.
func (c modelChat) effortForSwitch(target Model) (string, string, string) {
	cur := firstNonEmpty(c.th.PendingEffort, c.th.Effort)
	src := effortSource(c.th.EffortSource)
	explicit := src == effortSrcExplicit && cur != ""
	_, supported := findEffort(target, cur)
	if explicit && supported {
		return cur, src, ""
	}
	if target.Effort == "" {
		if cur != "" && supported {
			return cur, src, ""
		}
		return "", effortSrcModelDefault, "Reasoning effort will use the target model default."
	}
	notice := ""
	if explicit && !strings.EqualFold(cur, target.Effort) {
		notice = "Your explicit reasoning effort `" + cur + "` is not supported by the target model; it was reset to `" + target.Effort + "`."
	}
	return target.Effort, effortSrcModelDefault, notice
}

// modelSwitch is v0.1.20's switchChatModelProfileWithSource; ref "" is the
// global default (`model reset`).
func (e *Engine) modelSwitch(j cmdJob, c modelChat, ref, source string) {
	sel, target := "", Model{}
	if source == modelSrcGlobalDefault {
		sel = selector(c.defs.Model)
		target, _ = resolveModel(c.models, sel)
	} else {
		var failed string
		if sel, target, failed = c.targetModel(ref); failed != "" {
			e.reply(j, failed, nil)
			return
		}
	}
	if sel == "" && c.modelID() != "" && target.ID != "" {
		// Codex keeps a turn's model for the thread's later turns, so
		// going back to the account default names it.
		sel = target.ID
	}
	prev := officialLabel(c.models, c.modelID())
	next := officialLabel(c.models, sel)
	eff, effSrc, notice := c.effortForSwitch(target)
	var text string
	if strings.EqualFold(sel, c.modelID()) {
		if c.th.ModelSource == source {
			e.reply(j, "Model profile already selected for this "+c.kind+" chat: "+next+"\nSelection: "+modelSourceLabel(source), nil)
			return
		}
		text = "Model selection for this " + c.kind + " chat is now " + modelSourceLabel(source) + ": " + next
	} else {
		threadNotice := "The next message will start a Codex thread with the selected model."
		if c.th.ThreadID != "" {
			threadNotice = "The next message will continue the current Codex thread with its portable context preserved."
		}
		text = "Model profile switched for this " + c.kind + " chat\nPrevious: " + prev + "\nEffective next-turn model: " + next +
			"\nSelection: " + modelSourceLabel(source) + "\n\n" + threadNotice
		if c.running {
			text += "\nThe running turn keeps its model."
		}
	}
	if notice != "" {
		text += "\n\n" + notice
	}
	e.reply(j, text, func(tx *store.Tx) error {
		return e.changeThread(tx, c.cs, func(th *store.Thread) {
			th.Model, th.ModelSource, th.PendingModel = sel, source, ""
			th.Effort, th.EffortSource, th.PendingEffort = eff, effSrc, ""
		})
	})
}

// changeThread applies fn to the conversation's thread row, creating it
// when missing.
func (e *Engine) changeThread(tx *store.Tx, cs *convState, fn func(*store.Thread)) error {
	th, ok, err := tx.Thread(cs.id)
	if err != nil {
		return err
	}
	if !ok {
		e.mu.Lock()
		threadID, cwd := cs.threadID, cs.cwd
		e.mu.Unlock()
		if th, err = e.newChatThread(tx, cs.id, cwd); err != nil {
			return err
		}
		th.ThreadID = threadID
	}
	fn(&th)
	return tx.Bind(th)
}

// effortCommand is v0.1.20's handleReasoningEffortCommand.
func (e *Engine) effortCommand(j cmdJob, c modelChat, arg string) {
	sub, rest := commandParts(arg)
	m, known := c.model()
	switch sub {
	case "", "status", "current":
		e.reply(j, c.effortStatusText(m, known), nil)
	case "list", "ls", "options", "choices":
		if !known || len(m.Efforts) == 0 {
			e.reply(j, fmt.Sprintf("model %q did not advertise any reasoning efforts", firstNonEmpty(m.ID, c.modelID())), nil)
			return
		}
		e.reply(j, c.effortListText(m), nil)
	case "set", "switch", "use":
		if rest == "" {
			e.reply(j, "usage: `effort set <value>`", nil)
			return
		}
		e.effortSet(j, c, m, known, rest, effortSrcExplicit)
	case "reset", "default":
		// The global default effort when it suits the model, else the
		// model's advertised default (v0.1.20).
		if eff, src := globalDefaultEffort(c.defs, m); eff != "" {
			e.effortSet(j, c, m, known, eff, src)
			return
		}
		if m.Effort == "" {
			e.reply(j, fmt.Sprintf("model %q did not advertise a default reasoning effort", firstNonEmpty(m.ID, c.modelID())), nil)
			return
		}
		e.effortSet(j, c, m, known, m.Effort, effortSrcModelDefault)
	case "help", "?":
		e.reply(j, reasoningEffortUsage(), nil)
	default:
		if rest == "" {
			e.effortSet(j, c, m, known, sub, effortSrcExplicit)
			return
		}
		e.reply(j, reasoningEffortUsage(), nil)
	}
}

func reasoningEffortUsage() string {
	return strings.Join([]string{
		"Reasoning effort commands:",
		"- `effort status` - show this chat's current effort",
		"- `effort list` - list model-advertised choices",
		"- `effort set <value>` - switch future turns in this chat",
		"- `effort reset` - use the current model's advertised default",
	}, "\n")
}

func effortNames(m Model) string {
	names := make([]string, 0, len(m.Efforts))
	for _, e := range m.Efforts {
		names = append(names, "`"+e+"`")
	}
	return strings.Join(names, ", ")
}

// effortStatusText is v0.1.20's formatReasoningEffortStatus.
func (c modelChat) effortStatusText(m Model, known bool) string {
	eff, src := c.effort()
	lines := []string{
		"Current chat effort (" + c.kind + ")",
		"Effective for next turn: `" + eff + "`",
		"Source: " + effortSourceLabel(src),
		"Applies to: turns that start after a change in this chat; a running turn keeps its effort.",
	}
	if !known {
		return strings.Join(append(lines, "Dynamic list unavailable: Codex did not list this chat's model"), "\n")
	}
	lines = append(lines, "Model for this chat: `"+m.ID+"`")
	if m.Effort != "" {
		lines = append(lines, "Model-advertised default: `"+m.Effort+"`")
	}
	if len(m.Efforts) > 0 {
		lines = append(lines, "Available: "+effortNames(m))
	}
	return strings.Join(lines, "\n")
}

// effortListText is v0.1.20's formatReasoningEffortCatalog.
func (c modelChat) effortListText(m Model) string {
	lines := []string{"Current chat effort choices for " + displayName(m) + ":"}
	cur, _ := c.effort()
	for _, opt := range m.Efforts {
		var marks []string
		if strings.EqualFold(opt, cur) {
			marks = append(marks, "current")
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
	return strings.Join(append(lines, "", "Use `effort set <value>` to switch this chat."), "\n")
}

// effortSet is v0.1.20's setReasoningEffortFromCatalog.
func (e *Engine) effortSet(j cmdJob, c modelChat, m Model, known bool, requested, source string) {
	if !known || len(m.Efforts) == 0 {
		e.reply(j, fmt.Sprintf("could not list reasoning efforts: model %q did not advertise any reasoning efforts", firstNonEmpty(m.ID, c.modelID())), nil)
		return
	}
	eff, ok := findEffort(m, requested)
	if !ok {
		e.reply(j, fmt.Sprintf("reasoning effort %q is not supported by model %q; available values: %s", strings.TrimSpace(requested), m.ID, effortNames(m)), nil)
		return
	}
	prev := firstNonEmpty(c.th.PendingEffort, c.th.Effort, "unset")
	text := fmt.Sprintf("Reasoning effort updated for this %s chat\nPrevious: `%s`\nEffective for next turn: `%s`\nModel: `%s`\nSource: %s\nApplies to turns that start after this change in this chat; a running turn keeps its effort.",
		c.kind, prev, eff, m.ID, effortSourceLabel(source))
	e.reply(j, text, func(tx *store.Tx) error {
		return e.changeThread(tx, c.cs, func(th *store.Thread) {
			th.Effort, th.EffortSource, th.PendingEffort = eff, source, ""
		})
	})
}

// globalDefaultEffort is v0.1.20's ResolveDefaultReasoningEffort for a
// model: the global default effort when the model supports it, else (when
// one is set, or the global model is explicit) the model's default.
func globalDefaultEffort(d globalDefaults, m Model) (string, string) {
	if d.Effort == "" {
		if d.Model != "" && m.Effort != "" {
			return m.Effort, effortSrcModelDefault
		}
		return "", ""
	}
	if eff, ok := findEffort(m, d.Effort); ok {
		return eff, effortSrcGlobalDefault
	}
	if m.Effort != "" {
		return m.Effort, effortSrcModelDefault
	}
	return "", ""
}

// modelFork is v0.1.20's forkWorkChatWithModelProfile: a new Work chat in
// the same directory, on a new Codex thread, pinned to the model, with the
// global default effort. The source chat hears about it once the chat
// exists (announceModelFork); a restart in between loses nothing (the
// conversation and the announcement are in the store).
func (e *Engine) modelFork(ctx context.Context, j cmdJob, c modelChat, ref string) {
	sel, target, failed := c.targetModel(ref)
	if failed != "" {
		e.reply(j, failed, nil)
		return
	}
	eff, effSrc := globalDefaultEffort(c.defs, target)
	e.mu.Lock()
	cwd, title, srcLabel := c.cs.cwd, c.cs.title, c.cs.label
	e.mu.Unlock()
	machine := e.machineLabel() // reads the store: not inside the transaction
	if cwd == "" {
		cwd = c.th.Cwd
	}
	name := firstNonEmpty(sel, "default")
	var id int64
	e.complete(j, func(tx *store.Tx) error {
		label, err := e.nextLabel(tx)
		if err != nil {
			return err
		}
		// v0.1.20 forkWorkChatWithModelProfile: the source chat's title.
		id, err = tx.CreateConversation(store.NewConversation{Label: label, Kind: store.KindWork, Channel: e.cfg.ChannelName,
			Title: workTitle(machine, "", titleBody(title, srcLabel, machine, e.cfg.Machine), cwd), TitleSource: "auto"})
		if err != nil {
			return err
		}
		if err := tx.Bind(store.Thread{ConvID: id, Cwd: cwd, Model: sel, ModelSource: modelSrcChatOverride,
			Effort: eff, EffortSource: effSrc}); err != nil {
			return err
		}
		b, _ := json.Marshal(modelForkNote{Src: c.cs.id, Model: name})
		if err := tx.SetMeta(modelForkKey(id), string(b)); err != nil {
			return err
		}
		if err := tx.SetMeta(forkChildKey(id), "1"); err != nil {
			return err
		}
		_, err = e.refresh(tx, id)
		return err
	})
	if id != 0 {
		e.mu.Lock()
		if n := e.convs[id]; n != nil {
			n.cwd = cwd
		}
		e.mu.Unlock()
		e.createChatNow(ctx, id)
	}
}

// modelForkNote records, until its chat exists, which chat asked for a
// `model fork` and with which model.
type modelForkNote struct {
	Src   int64  `json:"src"`
	Model string `json:"model"`
}

func modelForkKey(conv int64) string { return fmt.Sprintf("engine_model_fork_%d", conv) }

func modelForkOf(tx *store.Tx, conv int64) (modelForkNote, error) {
	var n modelForkNote
	v, ok, err := tx.Meta(modelForkKey(conv))
	if err != nil || !ok || v == "" {
		return n, err
	}
	_ = json.Unmarshal([]byte(v), &n)
	return n, nil
}

// announceModelFork posts v0.1.20's notices of a `model fork` chat: its
// created notice and ready anchor (naming the model), and the link in the
// chat that asked for it.
func (e *Engine) announceModelFork(tx *store.Tx, id int64, label, cwd, url string, n modelForkNote) error {
	if err := e.enqueueNotice(tx, id, "ready:"+label, textWorkChatCreated(label, cwd)); err != nil {
		return err
	}
	anchor := strings.Replace(textWorkChatReady(label, cwd), "\nCommands: ", "\nExecution target: model:"+n.Model+"\nCommands: ", 1)
	if err := e.enqueueNotice(tx, id, "anchor:"+label, anchor); err != nil {
		return err
	}
	if err := e.enqueueNotice(tx, n.Src, fmt.Sprintf("modelfork:%d", id),
		fmt.Sprintf("Forked Work chat %s with model profile %s:\n%s", label, n.Model, url)); err != nil {
		return err
	}
	return tx.SetMeta(modelForkKey(id), "")
}

// cmdEffortChat is `effort …` in either chat.
func (e *Engine) cmdEffortChat(ctx context.Context, j cmdJob, cs *convState, arg string) {
	c, failed := e.loadModelChat(ctx, cs)
	if failed != "" {
		e.reply(j, failed, nil)
		return
	}
	e.effortCommand(j, c, arg)
}
