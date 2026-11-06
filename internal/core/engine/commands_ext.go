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

// ChatSkills is a Skills whose commands depend on the chat: a `skills
// push` review belongs to the chat it was made in (gap batch B, F-CTL-17).
type ChatSkills interface {
	ChatCommand(ctx context.Context, chat, args string) string
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

// The model and effort commands are in modelcmds.go (gap batch B).

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

// cmdSkills runs the skills backend outside any transaction.
func (e *Engine) cmdSkills(ctx context.Context, j cmdJob, cs *convState, args string) {
	if e.cfg.Skills == nil {
		e.reply(j, "Skills are not available in this helper.", nil)
		return
	}
	var text string
	if cx, ok := e.cfg.Skills.(ChatSkills); ok && cs != nil { // batch B
		e.mu.Lock()
		chat := cs.chat
		e.mu.Unlock()
		text = cx.ChatCommand(ctx, chat, strings.TrimSpace(args))
	} else {
		text = e.cfg.Skills.Command(ctx, strings.TrimSpace(args))
	}
	text = strings.TrimSpace(text)
	if text == "" {
		text = "Done."
	}
	e.reply(j, text, nil)
}
