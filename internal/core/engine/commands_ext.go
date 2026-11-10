package engine

import (
	"context"
	"strings"
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

// Session statistics are computed from Codex rollouts (stats.go).

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
