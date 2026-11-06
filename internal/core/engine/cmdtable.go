package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// The command table: one handler per canonical command name (parse.go)
// and chat kind. A later batch implements a command by replacing its hook
// here; the grammar stays in parse.go.

// cmdHandler runs one parsed command. cs is the conversation the command
// came from (the control chat for control commands).
type cmdHandler func(e *Engine, ctx context.Context, j cmdJob, cs *convState, p parsed)

// workCommands maps a work-chat command to its handler.
var workCommands = map[string]cmdHandler{
	cmdHelp: func(e *Engine, _ context.Context, j cmdJob, _ *convState, p parsed) {
		if isAdvancedHelpArg(p.arg) {
			e.reply(j, sessionAdvancedHelpText, nil)
			return
		}
		e.reply(j, sessionHelpText, nil)
	},
	cmdStatus: func(e *Engine, _ context.Context, j cmdJob, cs *convState, _ parsed) {
		e.reply(j, e.workStatus(cs), nil)
	},
	cmdStats: func(e *Engine, ctx context.Context, j cmdJob, cs *convState, _ parsed) { e.cmdStats(ctx, j, cs) },
	cmdClose: func(e *Engine, _ context.Context, j cmdJob, cs *convState, _ parsed) { e.cmdCloseChat(j, cs) },
	cmdParkName: func(e *Engine, _ context.Context, j cmdJob, cs *convState, p parsed) {
		if p.arg != "" {
			e.cmdParkLabel(j, p.arg) // `helper park <session>` (v0.1.20)
			return
		}
		e.cmdPark(j, cs)
	},
	cmdResume: func(e *Engine, _ context.Context, j cmdJob, cs *convState, p parsed) {
		if p.arg != "" {
			e.cmdWake(j, p.arg) // `helper resume <session>` (v0.1.20)
			return
		}
		// The rebuilt core's `helper resume` lifted the breaker; without a
		// session it still does (`helper unpause`).
		e.cmdUnpause(j, cs)
	},
	cmdUnpause: func(e *Engine, _ context.Context, j cmdJob, cs *convState, _ parsed) { e.cmdUnpause(j, cs) },
	cmdRetryName: func(e *Engine, _ context.Context, j cmdJob, cs *convState, p parsed) {
		e.cmdRetry(j, cs, firstLower(p.arg))
	},
	cmdCancelName: func(e *Engine, ctx context.Context, j cmdJob, cs *convState, p parsed) {
		e.cmdCancel(ctx, j, cs, firstLower(p.arg))
	},
	cmdRenameName: func(e *Engine, ctx context.Context, j cmdJob, cs *convState, p parsed) {
		e.cmdRename(ctx, j, cs, p.arg)
	},
	cmdSkillsName: func(e *Engine, ctx context.Context, j cmdJob, cs *convState, p parsed) {
		e.cmdSkills(ctx, j, cs, p.arg)
	},
	// `model …` / `effort …` (modelcmds.go, batch B).
	cmdModelName: func(e *Engine, ctx context.Context, j cmdJob, cs *convState, p parsed) {
		e.cmdModelChat(ctx, j, cs, p.arg)
	},
	cmdEffortName: func(e *Engine, ctx context.Context, j cmdJob, cs *convState, p parsed) {
		e.cmdEffortChat(ctx, j, cs, p.arg)
	},
	cmdDefault: func(e *Engine, _ context.Context, j cmdJob, _ *convState, _ parsed) {
		e.reply(j, textWorkDefault, nil)
	},
	cmdFork: func(e *Engine, ctx context.Context, j cmdJob, cs *convState, _ parsed) { e.cmdFork(ctx, j, cs) },
	cmdRestartName: func(e *Engine, _ context.Context, j cmdJob, _ *convState, p parsed) {
		e.cmdRestart(j, strings.ToLower(p.arg))
	},
	cmdShutdown: func(e *Engine, _ context.Context, j cmdJob, _ *convState, _ parsed) {
		e.reply(j, textShutdownControlOnly, nil)
	},
	// HOOK(batch E): `helper file <path>` (F-WRK-5).
	cmdSendFile: hook("helper file"),
	// HOOK(batch A): `helper details`, `helper publish-history`.
	cmdDetails:        hook("helper details"),
	cmdPublishHistory: hook("helper publish-history"),
}

// controlCommands maps a control-chat command to its handler.
var controlCommands = map[string]cmdHandler{
	cmdHelp: func(e *Engine, _ context.Context, j cmdJob, _ *convState, p parsed) {
		if isAdvancedHelpArg(p.arg) {
			e.reply(j, controlAdvancedHelpText, nil)
			return
		}
		e.reply(j, controlHelpText, nil)
	},
	cmdSessions: func(e *Engine, _ context.Context, j cmdJob, _ *convState, _ parsed) { e.reply(j, e.listText(), nil) },
	cmdList:     func(e *Engine, _ context.Context, j cmdJob, _ *convState, _ parsed) { e.reply(j, e.listText(), nil) },
	cmdStatus:   func(e *Engine, _ context.Context, j cmdJob, _ *convState, _ parsed) { e.reply(j, e.statusText(), nil) },
	cmdNewName:  func(e *Engine, ctx context.Context, j cmdJob, _ *convState, p parsed) { e.cmdNew(ctx, j, p.arg) },
	cmdParkName: func(e *Engine, _ context.Context, j cmdJob, _ *convState, p parsed) { e.cmdParkLabel(j, p.arg) },
	cmdRenameName: func(e *Engine, ctx context.Context, j cmdJob, _ *convState, p parsed) {
		e.cmdRename(ctx, j, nil, p.arg)
	},
	cmdResume: func(e *Engine, _ context.Context, j cmdJob, _ *convState, p parsed) {
		if p.arg == "" {
			e.reply(j, "usage: `r <session-number>`", nil) // v0.1.20
			return
		}
		e.cmdWake(j, p.arg)
	},
	cmdRestartName: func(e *Engine, _ context.Context, j cmdJob, _ *convState, p parsed) {
		e.cmdRestart(j, strings.ToLower(p.arg))
	},
	cmdShutdown: func(e *Engine, ctx context.Context, j cmdJob, _ *convState, _ parsed) { e.cmdShutdown(ctx, j) },
	cmdUpdate: func(e *Engine, ctx context.Context, j cmdJob, _ *convState, p parsed) {
		e.cmdHelperUpdate(ctx, j, p.arg)
	},
	cmdCodexUpdate: func(e *Engine, _ context.Context, j cmdJob, _ *convState, p parsed) { e.cmdCodexUpdate(j, p.arg) },
	cmdFork: func(e *Engine, ctx context.Context, j cmdJob, _ *convState, p parsed) {
		target := e.findLabel(p.arg)
		if target == nil {
			e.reply(j, "Usage: `fork <session>` (for example `fork s003`). Send `list` to see sessions.", nil)
			return
		}
		e.cmdFork(ctx, j, target)
	},
	cmdStats: func(e *Engine, ctx context.Context, j cmdJob, _ *convState, _ parsed) { e.cmdStats(ctx, j, nil) },
	cmdSkillsName: func(e *Engine, ctx context.Context, j cmdJob, cs *convState, p parsed) {
		e.cmdSkills(ctx, j, cs, p.arg)
	},
	// `model …` / `effort …` (modelcmds.go, batch B).
	cmdModelName: func(e *Engine, ctx context.Context, j cmdJob, cs *convState, p parsed) {
		e.cmdModelChat(ctx, j, cs, p.arg)
	},
	cmdEffortName: func(e *Engine, ctx context.Context, j cmdJob, cs *convState, p parsed) {
		e.cmdEffortChat(ctx, j, cs, p.arg)
	},
	cmdCancelName: func(e *Engine, ctx context.Context, j cmdJob, cs *convState, p parsed) {
		e.cmdControlCancel(ctx, j, cs, p.arg) // the control chat's Codex questions (control_qa.go)
	},
	cmdDefault: func(e *Engine, ctx context.Context, j cmdJob, _ *convState, p parsed) {
		e.cmdDefaultControl(ctx, j, p.arg) // global defaults (defaults.go, batch B)
	},
	cmdSelect: func(e *Engine, _ context.Context, j cmdJob, _ *convState, _ parsed) {
		// HOOK(batch A): the workspace/session dashboard (F-CTL-1..5).
		e.reply(j, "I do not have a current list yet. Send `projects` or `sessions` first, then choose a number.", nil)
	},
	// HOOK(batch A): the workspace / history dashboard.
	cmdWorkspaces: hook("projects"),
	cmdWorkspace:  hook("project"),
	cmdOpen:       hook("open"),
	cmdPublish:    hook("continue"),
	cmdMkdir:      hook("mkdir"),
	cmdDetails:    hook("details"),
	// `ask <question>` is a question for Codex (control_qa.go); a bare
	// `ask` gets here.
	cmdAsk: func(e *Engine, _ context.Context, j cmdJob, _ *convState, _ parsed) {
		e.reply(j, "usage: `ask <question>`", nil)
	},
	cmdWebhook: func(e *Engine, ctx context.Context, j cmdJob, cs *convState, p parsed) {
		e.cmdWebhook(ctx, j, cs, p.arg) // Workflow webhooks (webhook.go, batch B)
	},
}

// hook answers a command a later batch implements. It is never sent to
// Codex.
func hook(display string) cmdHandler {
	return func(e *Engine, _ context.Context, j cmdJob, _ *convState, _ parsed) {
		e.reply(j, textNotAvailable(display), nil)
	}
}

func textNotAvailable(display string) string {
	return "`" + display + "` is not available in this helper version yet."
}

func firstLower(arg string) string {
	f := strings.Fields(strings.ToLower(arg))
	if len(f) == 0 {
		return ""
	}
	return f[0]
}

// runControl runs a control-chat command.
func (e *Engine) runControl(ctx context.Context, j cmdJob, cs *convState) {
	p := parseControl(j.text)
	switch p.form {
	case formCommand:
		if h := controlCommands[p.name]; h != nil {
			h(e, ctx, j, cs, p)
			return
		}
	case formText:
		// Questions for Codex never get here (control_qa.go); empty text
		// does (v0.1.20 runControlFallback: the help), and so does a path
		// (HOOK(batch A): v0.1.20's controlPathHintMessage).
		e.reply(j, controlHelpText, nil)
		return
	}
	e.reply(j, unknownControlText(p), nil)
}

// runWork runs a work-chat command. The text is parsed as admission did
// (classify), with the @codex mention removed.
func (e *Engine) runWork(ctx context.Context, j cmdJob, cs *convState) {
	text, _ := stripCodexMention(j.text)
	p := parseWork(text)
	if p.form == formCommand {
		if h := workCommands[p.name]; h != nil {
			h(e, ctx, j, cs, p)
			return
		}
	}
	e.reply(j, unknownWorkText(p), nil)
}

// unknownControlText is v0.1.20's unknownControlCommandMessage (B:5271).
func unknownControlText(p parsed) string {
	switch {
	case p.name == "":
		return controlHelpText
	case p.syntax == synHelper && workOnlyName(p.name):
		return textWrongChat
	}
	// HOOK(batch A): a name with "/" or a leading "." gets v0.1.20's path
	// hint (controlPathHintMessage).
	return fmt.Sprintf("unknown control command: `%s`\n\n%s", p.name, controlHelpText)
}

// unknownWorkText is v0.1.20's unknownWorkCommandMessage (B:7387).
func unknownWorkText(p parsed) string {
	if p.name == "" {
		return sessionHelpText
	}
	return fmt.Sprintf("unknown work chat command: `%s`\n\n%s", p.name, sessionHelpText)
}

// cmdCloseChat closes a work chat (`helper close`, v0.1.20 B:8007): it is
// no longer read and starts no request. A running request finishes and its
// answer is posted; queued requests are cancelled without a notice (the
// chat said it no longer responds).
func (e *Engine) cmdCloseChat(j cmdJob, cs *convState) {
	e.mu.Lock()
	var runningID int64
	if cs.running != nil {
		runningID = cs.running.turnID
	}
	e.mu.Unlock()
	e.reply(j, textSessionClosed, func(tx *store.Tx) error {
		open, err := tx.OpenTurnsOf(cs.id)
		if err != nil {
			return err
		}
		for _, tr := range open {
			if tr.State == store.TurnQueued && tr.ID != runningID {
				if err := tx.FinishTurn(tr.ID, store.TurnCancelled, "chat closed", nil); err != nil {
					return err
				}
			}
		}
		if err := tx.Close(cs.id); err != nil {
			return err
		}
		_, err = e.refresh(tx, cs.id)
		return err
	})
}

const (
	textSessionClosed = "Session closed. The helper will no longer read or respond in this Teams chat.\n\n" +
		"This chat remains visible in Teams. Use the 🏠 control chat to open or create another work chat."
	textWrongChat = "⚠️ Wrong chat\n\nThis is the 🏠 control chat. `helper ...` commands like `helper file`, `helper retry`, and `helper close` work inside a 💬 Work chat.\n\n" +
		"To start project work, send `new <directory>` here, then open the new Work chat and send the task there."
	textWorkDefault         = "`default` changes global settings and can only be used in the Control chat. Use `model` or `effort` to change only this Work chat."
	textShutdownControlOnly = "`shutdown` stops the helper on all machines and can only be used in the 🏠 control chat."
	textBareMention         = "Mention received, but there is no Codex request text. Add the task after `@codex`."
	textCoworkerCommand     = "Only the helper owner can run helper commands in this Work chat. Send a normal question or task here and I will pass it to Codex."
	textCoworkerAccepted    = "⏳ Codex received your question. Request accepted."
)

// The help texts are v0.1.20's (bridge.go B:5123, B:5150, B:5216,
// B:5241), adapted only for the owner's decisions: restore-thread,
// `helper reload`, beacon and third-party model setup are removed;
// `shutdown`, `cxp teams stop/start`, `fork` and `helper unpause` are the
// rebuilt core's own commands.

var controlHelpText = strings.Join([]string{
	"## 🏠 Control chat",
	"Use this chat to choose a workspace and create or continue 💬 Work chats.",
	"",
	"Start here:",
	"- `p` / `projects` - choose a workspace",
	"- `n <directory>` / `new <directory>` - create a new Work chat for a directory",
	"- `new <directory> --model <model>` - create a Work chat pinned to a model",
	"- `model status|list|switch|reset` - inspect or change only this Control chat's model",
	"- `effort status|list|set|reset` - inspect or change this Control chat's reasoning effort",
	"- `default status` / `default model ...` / `default effort ...` - manage global defaults for future launches and chats",
	"- `s` / `sessions` - show sessions in the selected workspace",
	"- `c <number>` / `continue <number>` - continue an old local Codex session in Teams",
	"- `st` / `status` - show active Work chats",
	"- `helper rename <title>` - rename this Control chat",
	"- `helper rename hostname <name>` - rename this machine in all related chat titles",
	"- `skills` - list installed skill subscriptions",
	"- `shutdown` - stop the helper on all machines until `cxp teams start` is run on a machine",
	"",
	"After `p`, reply with a number such as `1` to open that workspace. On a workspace page, send `new` to create a Work chat in that workspace.",
	"",
	"For quick helper questions, type the question here. For project work, use a 💬 Work chat.",
	"",
	"Send `help advanced` for all commands.",
}, "\n")

var controlAdvancedHelpText = strings.Join([]string{
	"## 🏠 Control chat advanced help",
	"",
	"Workspace flow:",
	"- `p` / `projects` - list directories with local Codex history",
	"- `p 1` / `project 1` / `1` - open workspace 1 from the `projects` list",
	"- `new` / `n` - create a Work chat in the currently opened workspace",
	"- `n <directory>` / `new <directory>` - create the directory if missing, then create a Work chat there",
	"",
	"History flow:",
	"- `s` / `sessions` / `history` - list local sessions in the selected workspace",
	"- `c 1` / `continue 1` / `1` on a sessions page - create/open a Work chat and import that session history",
	"",
	"Other control commands:",
	"- `st` / `status` - list active Teams work chats",
	"- `d <number>` / `details <number>` - show technical IDs and details",
	"- `park <session-number-or-id>` / `resume <session-number-or-id>` / `unpark <session-number-or-id>` - manually pause or resume polling for a Work chat",
	"- `fork <session>` - copy a session into a new Work chat",
	"- `m <directory>` / `mkdir <directory>` - create a directory only",
	"- `ask <question>` - ask a quick helper question in this control chat",
	"- `helper cancel last` / `helper cancel queued` / `helper cancel running` / `helper cancel all` - stop queued/running Codex question(s) in this Control chat",
	"- `model status|list|switch|reset` and `effort status|list|set|reset` - inspect or change only this Control chat (`model use` aliases switch)",
	"- Control-only `default model ...` / `default effort ...` manage future global defaults",
	"- `helper rename <title>` - rename this Control chat",
	"- `helper rename hostname <name>` - rename this machine in the Control chat and all linked Work chats",
	"- `h` / `help` / `menu` - show short help",
	"- `helper restart` - show the safe restart confirmation",
	"- `helper restart now` - restart the local Teams helper after sending a confirmation",
	"- `shutdown` - stop the helper on all machines until `cxp teams start` is run on a machine",
	"- `helper update now` - update to the latest stable helper release",
	"- `helper update prerelease` - update to the newest helper release or pre-release",
	"- `codex update now` - wait for active work, then update the local Codex CLI",
	"- `helper webhook setup` - show a guided Workflow webhook setup flow",
	"- `helper webhook <url>` - enable Workflow notification cards with a Teams Workflow webhook URL",
	"- `helper webhook off` - disable Workflow notification cards",
	"- `helper skills list|add|sync|push` - manage skill subscriptions",
	"",
	"Local CLI:",
	"- `cxp teams stop` / `cxp teams start` - stop the helper on all machines / start it again",
	"- `cxp skills install-builtin` - install or repair bundled local skills in `$HOME/.agents/skills`",
	"- `cxp skills migrate` - migrate managed legacy skills from `~/.codex/skills` to `$HOME/.agents/skills`",
	"",
	"work chat commands:",
	"Inside a 💬 Work chat, send your task as a regular Teams message. Use `helper help`, `helper status`, `helper stats`, `helper retry last`, `helper file <relative-path>`, or `helper close` for helper actions.",
	"Status words: `queued`/`running` means wait, `completed` means done, `failed` or `interrupted` means check recent messages and changed files before `helper retry last`.",
	"If this chat stops replying for about a minute, send `helper status`. From the control chat, `helper restart now` restarts the installed helper.",
	"",
	"copy-ready examples:",
	"`p`",
	"`n /home/baka/project/codex-helper`",
	"`m ~/tmp/mobile-fix`",
}, "\n")

var sessionHelpText = strings.Join([]string{
	"💬 Work chat",
	"Send your task as a regular Teams message. Messages starting with `helper` or `!` are helper commands.",
	"",
	"Common commands:",
	"`helper status` or `!status` - check progress",
	"`helper stats` or `!stats` - show Codex token usage from the linked local history, grouped by model and service tier",
	"`helper file <relative-path>` or `!file <relative-path>` - upload a file prepared in the helper's Teams upload folder",
	"`helper close` or `!close` - close this Codex session in Teams",
	"`helper details` or `!details` - show debug IDs and links",
	"`model status`, `model switch <model>`, `model reset`, or `model fork <model>` - inspect or change this Work chat model",
	"`effort status`, `effort list`, `effort set <value>`, or `effort reset` - inspect or change this Work chat's reasoning effort",
	"`default ...` - global defaults are Control-chat-only",
	"`helper publish-history` - import a paused local Codex history backlog",
	"`helper publish-history full` - publish the complete local Codex history once to this chat",
	"`helper skills list` / `helper skills add <url>` / `helper skills push` / `helper skills push confirm` - inspect and push skill subscriptions",
	"",
	"Send `helper help advanced` for retry, cancel, and rename commands.",
}, "\n")

var sessionAdvancedHelpText = strings.Join([]string{
	"💬 Work chat advanced help",
	"`helper status` or `!status` - check progress",
	"`helper stats` or `!stats` - show Codex token usage by model/service-tier combination, cache/context analysis, and rate-limit metadata when Codex recorded it",
	"`helper details` or `!details` - show IDs and debug details",
	"`helper rename <title>` or `!rename <title>` - rename this Teams chat",
	"`helper rename hostname <name>` - rename this machine in all related chat titles",
	"`helper file <relative-path>` or `!file <relative-path>` - upload a file prepared in the helper's Teams upload folder",
	"`helper close` or `!close` - close this Codex session in Teams",
	"`helper park <session-number-or-id>` / `helper resume <session-number-or-id>` / `helper unpark <session-number-or-id>` - manually pause or resume polling for a Work chat",
	"`helper unpause` - resume auto-start after a pause (requests sent while paused are not run)",
	"`helper fork` - copy this session into a new Work chat",
	"`helper publish-history` or `!ph` - import a paused local Codex history backlog",
	"`helper publish-history full` - publish the complete local Codex history once to this chat",
	"advanced commands: `helper retry last`, `helper retry <turn-id>` / `!retry <turn-id>`, or `helper cancel last`, `helper cancel queued`, `helper cancel running`, `helper cancel all`, `helper cancel <turn-id>` / `!cancel <turn-id>`",
	"Status words: `queued`/`running` means wait, `completed` means done, `failed` or `interrupted` means check recent messages and changed files before `helper retry last`.",
	"Other text, including unknown slash-prefixed text, is sent to Codex.",
}, "\n")
