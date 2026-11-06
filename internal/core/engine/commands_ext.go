package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Skills runs the `skills` command (internal/skills in production). The
// engine passes the arguments and posts the returned text; nil disables it.
type Skills interface {
	Command(ctx context.Context, args string) string
}

// metaUsagePrefix + conv id holds the session's accumulated turn stats
// (JSON usageStats). Meta is used so stats need no schema change.
const metaUsagePrefix = "engine_usage:"

// usageStats is what `helper stats` reports for one session.
type usageStats struct {
	Turns     int   `json:"turns"`
	Completed int   `json:"completed"`
	Failed    int   `json:"failed"`
	Input     int64 `json:"input"`
	Cached    int64 `json:"cached"`
	Output    int64 `json:"output"`
	Reasoning int64 `json:"reasoning"`
	Total     int64 `json:"total"`
}

func (u *usageStats) add(o usageStats) {
	u.Turns += o.Turns
	u.Completed += o.Completed
	u.Failed += o.Failed
	u.Input += o.Input
	u.Cached += o.Cached
	u.Output += o.Output
	u.Reasoning += o.Reasoning
	u.Total += o.Total
}

// recordUsage adds one finished turn to the session's stats (inside the
// finishing transaction).
func recordUsage(tx *store.Tx, conv int64, state string, u Usage) error {
	key := fmt.Sprintf("%s%d", metaUsagePrefix, conv)
	var cur usageStats
	if v, ok, err := tx.Meta(key); err != nil {
		return err
	} else if ok && v != "" {
		_ = json.Unmarshal([]byte(v), &cur)
	}
	cur.Turns++
	switch state {
	case store.TurnCompleted:
		cur.Completed++
	case store.TurnFailed, store.TurnNotStarted:
		cur.Failed++
	}
	cur.add(usageStats{Input: u.Input, Cached: u.Cached, Output: u.Output, Reasoning: u.Reasoning, Total: u.Total})
	b, _ := json.Marshal(cur)
	return tx.SetMeta(key, string(b))
}

// The stats command (cmdStats) is in stats.go: computed from the Codex
// rollouts, not from these counters (F-STA-1).

// cmdModel handles `helper model …` in a work chat:
//
//	helper model                 current model and effort
//	helper model list            models Codex offers
//	helper model <id> [effort]   switch (from the next request on)
//	helper model reset           back to the Codex default
//	helper effort <effort>       switch the reasoning effort only
func (e *Engine) cmdModel(ctx context.Context, j cmdJob, cs *convState, args []string) {
	models, err := e.ag.Models(ctx)
	if err != nil {
		e.reply(j, "Codex did not list its models ("+userError(err.Error())+"). Try again later.", nil)
		return
	}
	var th store.Thread
	_ = e.update(func(tx *store.Tx) error {
		th, _, err = tx.Thread(cs.id)
		return err
	})
	sub := ""
	if len(args) > 0 {
		sub = strings.ToLower(args[0])
	}
	switch sub {
	case "", "status", "st":
		e.reply(j, modelStatus(models, th), nil)
	case "list", "ls":
		e.reply(j, modelList(models, th), nil)
	case "reset", "default":
		if th.Model == "" && th.Effort == "" {
			e.reply(j, "This session already uses the Codex default ("+defaultModelName(models)+").", nil)
			return
		}
		// Codex keeps a turn's model override for the thread's later turns,
		// so going back means sending the default explicitly.
		d, ok := defaultModel(models)
		if !ok {
			e.setModel(j, cs, "", "", "Model reset: this session uses the Codex default from the next request on.")
			return
		}
		text := "Model reset: this session uses the Codex default (`" + d.ID + "`"
		if d.Effort != "" {
			text += ", effort " + d.Effort
		}
		e.setModelFrom(j, cs, d.ID, d.Effort, sourceDefault, text+") from the next request on.")
	case "effort":
		if len(args) < 2 {
			e.reply(j, "Usage: `helper effort <low|medium|high>`", nil)
			return
		}
		e.cmdEffort(j, cs, models, th, args[1])
	default:
		m, ok := findModel(models, args[0])
		if !ok {
			e.reply(j, "Unknown model `"+args[0]+"`. Send `helper model list` to see the models.", nil)
			return
		}
		effort := th.Effort
		if len(args) > 1 {
			eff, ok := findEffort(m, args[1])
			if !ok {
				e.reply(j, fmt.Sprintf("`%s` does not support effort `%s` (supported: %s).", m.ID, args[1], strings.Join(m.Efforts, ", ")), nil)
				return
			}
			effort = eff
		} else if effort != "" {
			if _, ok := findEffort(m, effort); !ok {
				effort = ""
			}
		}
		text := "Model for this session: `" + m.ID + "`"
		if effort != "" {
			text += ", effort " + effort
		}
		e.setModel(j, cs, m.ID, effort, text+". It applies from the next request on.")
	}
}

func (e *Engine) cmdEffort(j cmdJob, cs *convState, models []Model, th store.Thread, arg string) {
	m, ok := findModel(models, th.Model)
	if !ok {
		m, ok = defaultModel(models)
	}
	eff := strings.ToLower(strings.TrimSpace(arg))
	if ok {
		var found bool
		if eff, found = findEffort(m, arg); !found {
			e.reply(j, fmt.Sprintf("`%s` does not support effort `%s` (supported: %s).", m.ID, arg, strings.Join(m.Efforts, ", ")), nil)
			return
		}
	}
	e.setModel(j, cs, th.Model, eff, "Reasoning effort for this session: "+eff+". It applies from the next request on.")
}

// setModel stores the session's model and effort ("" = Codex default) with
// the reply in one transaction.
func (e *Engine) setModel(j cmdJob, cs *convState, model, effort, text string) {
	e.setModelFrom(j, cs, model, effort, sourceUser, text)
}

// Model/effort sources stored on the thread.
const (
	sourceUser    = "user"    // chosen in the chat
	sourceDefault = "default" // the Codex default, pinned by `helper model reset`
)

func (e *Engine) setModelFrom(j cmdJob, cs *convState, model, effort, source, text string) {
	e.reply(j, text, func(tx *store.Tx) error {
		th, ok, err := tx.Thread(cs.id)
		if err != nil {
			return err
		}
		if !ok {
			e.mu.Lock()
			th = store.Thread{ConvID: cs.id, ThreadID: cs.threadID, Cwd: cs.cwd}
			e.mu.Unlock()
		}
		th.Model, th.Effort, th.PendingModel, th.PendingEffort = model, effort, "", ""
		th.ModelSource, th.EffortSource = "", ""
		if model != "" {
			th.ModelSource = source
		}
		if effort != "" {
			th.EffortSource = source
		}
		return tx.Bind(th)
	})
}

func findModel(models []Model, name string) (Model, bool) {
	name = strings.Trim(strings.TrimSpace(name), "`")
	if name == "" {
		return Model{}, false
	}
	for _, m := range models {
		if strings.EqualFold(m.ID, name) || strings.EqualFold(m.DisplayName, name) {
			return m, true
		}
	}
	return Model{}, false
}

func defaultModel(models []Model) (Model, bool) {
	for _, m := range models {
		if m.Default {
			return m, true
		}
	}
	if len(models) > 0 {
		return models[0], true
	}
	return Model{}, false
}

func defaultModelName(models []Model) string {
	if m, ok := defaultModel(models); ok {
		return "`" + m.ID + "`"
	}
	return "Codex's choice"
}

func findEffort(m Model, effort string) (string, bool) {
	effort = strings.ToLower(strings.TrimSpace(effort))
	if len(m.Efforts) == 0 {
		return effort, effort != ""
	}
	for _, e := range m.Efforts {
		if strings.EqualFold(e, effort) {
			return e, true
		}
	}
	return "", false
}

func modelStatus(models []Model, th store.Thread) string {
	model, src := th.Model, "chosen in this chat"
	if th.ModelSource == sourceDefault {
		src = "Codex default"
	}
	m, known := findModel(models, model)
	if model == "" {
		src = "Codex default"
		m, known = defaultModel(models)
		model = m.ID
	}
	effort, esrc := th.Effort, "chosen in this chat"
	if th.EffortSource == sourceDefault {
		esrc = "default"
	}
	if effort == "" {
		esrc = "default"
		if known {
			effort = m.Effort
		}
	}
	if model == "" {
		model = "Codex's choice"
	} else {
		model = "`" + model + "`"
	}
	if effort == "" {
		effort = "Codex's choice"
	}
	return fmt.Sprintf("Model: %s (%s). Reasoning effort: %s (%s).\n\nSwitch with `helper model <model> [effort]`, list with `helper model list`, go back with `helper model reset`.",
		model, src, effort, esrc)
}

func modelList(models []Model, th store.Thread) string {
	if len(models) == 0 {
		return "Codex listed no models."
	}
	var b strings.Builder
	b.WriteString("Models:\n")
	for _, m := range models {
		fmt.Fprintf(&b, "\n- `%s`", m.ID)
		if m.DisplayName != "" && m.DisplayName != m.ID {
			b.WriteString(" — " + m.DisplayName)
		}
		var tags []string
		if m.Default {
			tags = append(tags, "default")
		}
		if th.Model != "" && strings.EqualFold(th.Model, m.ID) {
			tags = append(tags, "this session")
		}
		if len(m.Efforts) > 0 {
			tags = append(tags, "efforts: "+strings.Join(m.Efforts, ", "))
		}
		if len(tags) > 0 {
			b.WriteString(" (" + strings.Join(tags, "; ") + ")")
		}
	}
	return b.String()
}

// cmdSkills runs the skills backend outside any transaction.
func (e *Engine) cmdSkills(ctx context.Context, j cmdJob, args string) {
	if e.cfg.Skills == nil {
		e.reply(j, "Skills are not available in this helper.", nil)
		return
	}
	text := strings.TrimSpace(e.cfg.Skills.Command(ctx, strings.TrimSpace(args)))
	if text == "" {
		text = "Done."
	}
	e.reply(j, text, nil)
}

// cmdFork forks the session's Codex thread into a new work chat. The
// original chat and thread are unchanged.
func (e *Engine) cmdFork(ctx context.Context, j cmdJob, cs *convState) {
	e.mu.Lock()
	label, threadID, cwd, running := cs.label, cs.threadID, cs.cwd, cs.running != nil
	e.mu.Unlock()
	if threadID == "" {
		e.reply(j, "Nothing to fork yet: "+label+" has no Codex conversation. Send a request first.", nil)
		return
	}
	// A fork while a request runs (F-WRK-10, batch E; v0.1.20's `model
	// fork` did not wait either) is cut at the last completed turn, so
	// the child never holds a half-finished one.
	cutoff := ""
	if running {
		cutoff = e.lastCompletedCodexTurn(cs.id)
	}
	child, err := e.ag.Fork(ctx, cs.id, threadID, cutoff, cwd)
	if err != nil {
		e.reply(j, "Fork failed: "+userError(err.Error())+". Nothing was changed.", nil)
		return
	}
	// The fork's rollout copies the parent's history: mirror it from where
	// it is when first seen (forkThreads), never from the start.
	e.mu.Lock()
	e.forkThreads[child] = true
	e.mu.Unlock()
	var id int64
	var newLabel string
	// HOOK(batch A, F-WRK-10 step 3): the child's history import; the
	// rc.6 text then promises the link only after the history is
	// confirmed ("…I will send its link only after the visible history is
	// confirmed.").
	e.reply(j, textForkRequested, func(tx *store.Tx) error {
		var err error
		newLabel, err = e.nextLabel(tx)
		if err != nil {
			return err
		}
		id, err = tx.CreateConversation(store.NewConversation{Label: newLabel, Kind: store.KindWork, Channel: e.cfg.ChannelName,
			Title: chatTitle(newLabel, "fork of "+label), TitleSource: "auto"})
		if err != nil {
			return err
		}
		parent, _, err := tx.Thread(cs.id)
		if err != nil {
			return err
		}
		if err := tx.Bind(store.Thread{ConvID: id, ThreadID: child, Cwd: cwd, Model: parent.Model, ModelSource: parent.ModelSource,
			Effort: parent.Effort, EffortSource: parent.EffortSource, ForkParent: cs.id}); err != nil {
			return err
		}
		_, err = e.refresh(tx, id)
		return err
	})
	if id == 0 {
		return
	}
	e.mu.Lock()
	if c := e.convs[id]; c != nil {
		c.cwd, c.threadID, c.forkParent = cwd, child, cs.id
	}
	e.byThread[child] = id
	e.intro[id] = "Forked from " + label + ": this chat continues a copy of that session; " + label + " itself is unchanged. Send `helper help` for commands."
	e.mu.Unlock()
	e.createChatNow(ctx, id)
}

// Fork texts (0.1.21-rc.6 1a210e6, SPEC F-WRK-10 steps 1 and 4).
const textForkRequested = "⏳ Fork requested. I fixed the parent cutoff and am preparing the new chat; I will send its link when it is ready."

func textForkComplete(url string) string {
	return "✅ Fork complete. Open the new Codex chat:\n" + url
}

// lastCompletedCodexTurn is the Codex turn id of the newest completed turn
// of a conversation ("" if none is known).
func (e *Engine) lastCompletedCodexTurn(conv int64) string {
	var id string
	_ = e.update(func(tx *store.Tx) error {
		id = ""
		turns, err := tx.RecentTurns(conv, retryHorizon)
		for _, tr := range turns {
			if tr.State == store.TurnCompleted && tr.CodexTurnID != "" {
				id = tr.CodexTurnID
				break
			}
		}
		return err
	})
	return id
}
