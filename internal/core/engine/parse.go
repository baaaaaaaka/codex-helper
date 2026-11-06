package engine

import (
	"regexp"
	"strconv"
	"strings"
)

// The chat command grammar of v0.1.20 (dashboard_commands.go), in one
// place. parseControl and parseWork turn a message text into a parsed
// command; commands.go maps the canonical name to its handler
// (controlCommands, workCommands). Later batches add a name to the alias
// tables here and a handler there.
//
// Forms, as in v0.1.20:
//   - Prefixes (splitDashboardCommand :603): `/x`, `!x`, `cx x`, `codex x`,
//     `helper x` (also `assistant x`, `codex-helper x`); a trailing ":" on
//     the word prefixes is allowed.
//   - `/`, `!` and `cx` accept the short names too (st cl rt rs x f img rn
//     d ph sync; workLegacyCommandName :571, controlLegacyCommandName).
//   - Work chats: bare `help` / `model` / `models` / `effort` / `efforts` /
//     `default` (workBareCommandName :221). Any other text without a prefix
//     is a prompt.
//   - Control chats: bare natural names (`p`, `s`, `new`, `st`, …) and a bare
//     number (controlNaturalCommandName :421). Other text is a question for
//     Codex (control Q&A).
//   - Unknown names: `/`, `codex` and `helper` forward the text to Codex
//     (work chat prompt, control Q&A); `!` and `cx` get the unknown-command
//     reply. The commands the owner removed (DECISIONS 第一级 4) always get
//     the unknown-command reply and never reach Codex.

// syntax is the prefix a command was written with.
type syntax int

const (
	synNone   syntax = iota
	synSlash         // /x
	synBang          // !x
	synCX            // cx x
	synCodex         // codex x
	synHelper        // helper x, assistant x, codex-helper x
)

// cmdForm is what a message is to the command dispatcher.
type cmdForm int

const (
	// formText is not a command: a prompt in a work chat, a question for
	// Codex in the control chat.
	formText cmdForm = iota
	// formCommand is a known command (name is its canonical name).
	formCommand
	// formUnknown is a command form with an unknown or removed name (name
	// is the name as typed): answered with the unknown-command text.
	formUnknown
)

// parsed is a parsed message.
type parsed struct {
	form   cmdForm
	name   string // canonical name (formCommand) or the typed name (formUnknown)
	arg    string // the argument, trimmed, original case
	syntax syntax
}

// isCommand reports a message the dispatcher answers (v0.1.20
// ParsedDashboardCommand.HelperCommand).
func (p parsed) isCommand() bool { return p.form != formText }

// Canonical command names. Handlers are keyed by these (commands.go).
const (
	cmdHelp           = "help"
	cmdStatus         = "status"
	cmdStats          = "stats"
	cmdClose          = "close"
	cmdParkName       = "park"
	cmdResume         = "resume" // wake a parked chat (`r <id>`)
	cmdRetryName      = "retry"
	cmdCancelName     = "cancel"
	cmdSendFile       = "send-file"
	cmdRenameName     = "rename"
	cmdDetails        = "details"
	cmdPublishHistory = "publish-history"
	cmdSkillsName     = "skills"
	cmdModelName      = "model"
	cmdEffortName     = "effort"
	cmdDefault        = "default"
	cmdSelect         = "select"
	cmdWorkspaces     = "workspaces"
	cmdWorkspace      = "workspace"
	cmdSessions       = "sessions"
	cmdOpen           = "open"
	cmdPublish        = "publish"
	cmdNewName        = "new"
	cmdAsk            = "ask"
	cmdMkdir          = "mkdir"
	cmdRestartName    = "restart"
	cmdUpdate         = "update"
	cmdCodexUpdate    = "codex-update"
	cmdWebhook        = "webhook"
	// Commands of the rebuilt core that v0.1.20 did not have.
	cmdUnpause  = "unpause"  // lift the breaker (owner 第一级 8)
	cmdFork     = "fork"     // SPEC F-WRK-10 / F-CTL-13
	cmdShutdown = "shutdown" // owner 2026-11-05 Q4
	cmdList     = "list"     // the core's session list
)

// removedNames are the commands the owner removed (DECISIONS 第一级 4 /
// 定稿 删除: `helper restore-thread`, `helper reload`, beacon). With any
// prefix, and bare, they get the unknown-command reply; they never reach
// Codex (scenario DELETED-CMDS).
var removedNames = map[string]bool{
	"restore-thread": true, "restore": true, "rs": true,
	"reload": true,
	"beacon": true,
}

// controlNatural: the control chat's names, bare or after a prefix
// (v0.1.20 controlNaturalCommandName, plus the core's own commands).
var controlNatural = map[string]string{
	"p": cmdWorkspaces, "proj": cmdWorkspaces, "projects": cmdWorkspaces, "project": cmdWorkspaces,
	"workspaces": cmdWorkspaces, "workdirs": cmdWorkspaces, "dirs": cmdWorkspaces,
	"w": cmdWorkspace, "ws": cmdWorkspace, "workspace": cmdWorkspace, "workdir": cmdWorkspace, "dir": cmdWorkspace,
	"s": cmdSessions, "sessions": cmdSessions, "session": cmdSessions, "history": cmdSessions,
	"o": cmdOpen, "open": cmdOpen,
	"park": cmdParkName,
	"r":    cmdResume, "resume": cmdResume, "unpark": cmdResume,
	"c": cmdPublish, "continue": cmdPublish, "publish": cmdPublish, "import": cmdPublish,
	"n": cmdNewName, "new": cmdNewName, "create": cmdNewName,
	"q": cmdAsk, "ask": cmdAsk, "question": cmdAsk,
	"m": cmdMkdir, "mkdir": cmdMkdir, "folder": cmdMkdir,
	"rn": cmdRenameName, "rename": cmdRenameName,
	"d": cmdDetails, "details": cmdDetails, "detail": cmdDetails,
	"": cmdHelp, "h": cmdHelp, "help": cmdHelp, "menu": cmdHelp, "?": cmdHelp,
	"st": cmdStatus, "status": cmdStatus,
	"skills": cmdSkillsName, "skill": cmdSkillsName,
	"model": cmdModelName, "models": cmdModelName, "model-profile": cmdModelName, "model-profiles": cmdModelName, "profiles": cmdModelName,
	"effort": cmdEffortName, "efforts": cmdEffortName, "reasoning-effort": cmdEffortName, "thinking-effort": cmdEffortName,
	"default": cmdDefault, "defaults": cmdDefault,
	// The rebuilt core's control commands (not in v0.1.20, kept; see
	// coreControlExtra).
	"list": cmdList, "ls": cmdList,
	"wake":    cmdResume,
	"restart": cmdRestartName, "reboot": cmdRestartName,
	"shutdown": cmdShutdown,
	"update":   cmdUpdate, "upgrade": cmdUpdate,
	"fork":  cmdFork,
	"stats": cmdStats, "usage": cmdStats, "tokens": cmdStats,
}

// coreControlExtra: the control names of the rebuilt core that v0.1.20
// did not have. Bare, `helper x`, `/x` and `!x` reach them; `codex x`
// does not.
var coreControlExtra = map[string]bool{
	"list": true, "ls": true, "wake": true, "restart": true, "reboot": true, "shutdown": true,
	"update": true, "upgrade": true, "fork": true, "stats": true, "usage": true, "tokens": true,
}

// workNatural: the work chat's names after any prefix (v0.1.20
// workNaturalCommandName :528, plus the core's own commands).
var workNatural = map[string]string{
	"": cmdHelp, "help": cmdHelp, "menu": cmdHelp, "?": cmdHelp,
	"status": cmdStatus,
	"stats":  cmdStats, "usage": cmdStats, "tokens": cmdStats,
	"close":  cmdClose,
	"park":   cmdParkName,
	"resume": cmdResume, "unpark": cmdResume,
	"retry":  cmdRetryName,
	"cancel": cmdCancelName, "stop": cmdCancelName,
	"send-file": cmdSendFile, "send-image": cmdSendFile, "file": cmdSendFile, "image": cmdSendFile,
	"rename":  cmdRenameName,
	"details": cmdDetails, "detail": cmdDetails,
	"publish-history": cmdPublishHistory, "sync-history": cmdPublishHistory, "import-history": cmdPublishHistory,
	"skills": cmdSkillsName, "skill": cmdSkillsName,
	"model": cmdModelName, "models": cmdModelName, "model-profile": cmdModelName, "model-profiles": cmdModelName, "profiles": cmdModelName,
	"effort": cmdEffortName, "efforts": cmdEffortName, "reasoning-effort": cmdEffortName, "thinking-effort": cmdEffortName,
	"default": cmdDefault, "defaults": cmdDefault,
	// The rebuilt core's work-chat commands (not in v0.1.20, kept).
	"unpause":  cmdUnpause,
	"fork":     cmdFork,
	"restart":  cmdRestartName,
	"shutdown": cmdShutdown, // control-only: answered with a pointer
}

// workShort: the short names `/`, `!` and `cx` accept in a work chat
// (v0.1.20 workLegacyCommandName :571).
var workShort = map[string]string{
	"h": cmdHelp, "st": cmdStatus, "cl": cmdClose, "rt": cmdRetryName,
	"x": cmdCancelName, "img": cmdSendFile, "f": cmdSendFile, "rn": cmdRenameName,
	"d": cmdDetails, "ph": cmdPublishHistory, "sync": cmdPublishHistory,
}

// workBare: the names a work chat accepts without a prefix (v0.1.20
// workBareCommandName :221). `help` only with no argument or an advanced
// one; `beacon` was bare in v0.1.20 and is removed.
var workBare = map[string]string{
	"help": cmdHelp, "menu": cmdHelp, "?": cmdHelp, "h": cmdHelp,
	"model": cmdModelName, "models": cmdModelName,
	"effort": cmdEffortName, "reasoning-effort": cmdEffortName, "thinking-effort": cmdEffortName, "efforts": cmdEffortName,
	"default": cmdDefault, "defaults": cmdDefault,
}

// splitBody splits "name rest" into the lower-case name and the trimmed
// rest (v0.1.20 splitDashboardCommandBody).
func splitBody(text string) (string, string) {
	text = strings.TrimSpace(text)
	if text == "" {
		return "", ""
	}
	name, arg, ok := strings.Cut(text, " ")
	name = strings.ToLower(strings.TrimSpace(name))
	if !ok {
		return name, ""
	}
	return name, strings.TrimSpace(arg)
}

// splitPrefix finds the command prefix (v0.1.20 splitDashboardCommand).
func splitPrefix(text string) (name, arg string, syn syntax, ok bool) {
	text = strings.TrimSpace(text)
	switch {
	case strings.HasPrefix(text, "/"):
		name, arg = splitBody(strings.TrimPrefix(text, "/"))
		return name, arg, synSlash, true
	case strings.HasPrefix(text, "!"):
		name, arg = splitBody(strings.TrimPrefix(text, "!"))
		return name, arg, synBang, true
	}
	word, rest, _ := strings.Cut(text, " ")
	switch strings.TrimSuffix(strings.ToLower(strings.TrimSpace(word)), ":") {
	case "cx":
		syn = synCX
	case "codex":
		syn = synCodex
	case "helper", "assistant", "codex-helper":
		syn = synHelper
	default:
		return "", "", synNone, false
	}
	name, arg = splitBody(rest)
	return name, arg, syn, true
}

// isAdvancedHelpArg is v0.1.20's isAdvancedHelpArg.
func isAdvancedHelpArg(arg string) bool {
	switch strings.ToLower(strings.TrimSpace(arg)) {
	case "advanced", "all", "more", "full":
		return true
	}
	return false
}

// listAlias turns bare `models` / `efforts` into their list form (v0.1.20
// dashboardCommandArgumentAlias).
func listAlias(name, canon, arg string) string {
	if strings.TrimSpace(arg) == "" && (name == "models" || name == "efforts") && (canon == cmdModelName || canon == cmdEffortName) {
		return "list"
	}
	return arg
}

// parseWork parses a work-chat message (v0.1.20 parseWorkChatCommand).
// In a chat with other people the caller strips the @codex mention first.
func parseWork(text string) parsed {
	t := strings.TrimSpace(text)
	if t == "" {
		return parsed{}
	}
	if name, arg := splitBody(t); workBare[name] != "" {
		canon := workBare[name]
		if canon != cmdHelp || arg == "" || isAdvancedHelpArg(arg) {
			return parsed{form: formCommand, name: canon, arg: listAlias(name, canon, arg)}
		}
	} else if name == "beacon" {
		return parsed{form: formUnknown, name: name, arg: arg} // bare in v0.1.20, removed
	}
	name, arg, syn, ok := splitPrefix(t)
	if !ok {
		return parsed{}
	}
	if removedNames[name] {
		return parsed{form: formUnknown, name: name, arg: arg, syntax: syn}
	}
	canon := workNatural[name]
	if canon == "" && (syn == synSlash || syn == synBang || syn == synCX) {
		canon = workShort[name]
	}
	if canon == "" {
		if syn == synBang || syn == synCX {
			return parsed{form: formUnknown, name: name, arg: arg, syntax: syn}
		}
		return parsed{syntax: syn} // `/x`, `codex x`, `helper x`: a prompt
	}
	return parsed{form: formCommand, name: canon, arg: listAlias(name, canon, arg), syntax: syn}
}

// parseControl parses a control-chat message (v0.1.20
// ParseDashboardCommand, ChatScopeControl).
func parseControl(text string) parsed {
	t := strings.TrimSpace(text)
	if t == "" {
		return parsed{}
	}
	if n, err := strconv.Atoi(t); err == nil && n > 0 && t[0] >= '0' && t[0] <= '9' {
		return parsed{form: formCommand, name: cmdSelect, arg: t}
	}
	if name, arg := splitBody(t); controlNatural[name] != "" {
		canon := controlNatural[name]
		if canon == cmdWorkspaces && arg != "" && !pluralListName(name) {
			canon = cmdWorkspace // `p 1`, `project <path>`
		}
		return parsed{form: formCommand, name: canon, arg: listAlias(name, canon, arg)}
	} else if removedNames[name] {
		return parsed{form: formUnknown, name: name, arg: arg}
	}
	name, arg, syn, ok := splitPrefix(t)
	if !ok {
		return parsed{} // a question for Codex
	}
	if removedNames[name] {
		return parsed{form: formUnknown, name: name, arg: arg, syntax: syn}
	}
	cmd := func(canon string) parsed {
		return parsed{form: formCommand, name: canon, arg: listAlias(name, canon, arg), syntax: syn}
	}
	if canon := controlAdmin(syn, name, arg); canon != "" {
		p := cmd(canon)
		if canon == cmdCodexUpdate {
			p.arg = strings.TrimSpace(name + " " + arg) // cmdCodexUpdate reads "update now"
		}
		if name == "service" {
			_, p.arg = splitBody(arg) // `helper service restart now`
		}
		return p
	}
	switch syn {
	case synSlash, synBang, synCX:
		// v0.1.20 controlLegacyCommandName: `p` is continue, then the
		// natural names, then `pub` / `wd`.
		canon := controlNatural[name]
		if name == "p" || name == "pub" {
			canon = cmdPublish
		} else if name == "wd" {
			canon = cmdWorkspace
		} else if canon == cmdWorkspaces && arg != "" && !pluralListName(name) {
			canon = cmdWorkspace
		}
		if canon == "" {
			return parsed{form: formUnknown, name: name, arg: arg, syntax: syn}
		}
		return cmd(canon)
	}
	// `codex x` / `helper x` (v0.1.20 prefixedControlNaturalCommandName):
	// the natural names, only with an explicit argument. `codex` does not
	// reach the rebuilt core's own commands (`codex restart` is a question).
	canon := controlNatural[name]
	if syn == synCodex && coreControlExtra[name] {
		canon = ""
	}
	if canon == cmdWorkspaces && arg != "" && !pluralListName(name) {
		canon = cmdWorkspace
	}
	if canon != "" && controlArgExplicit(canon, arg) {
		return cmd(canon)
	}
	if syn == synHelper && workOnlyName(name) && !((name == "cancel" || name == "stop") && !cancelArgExplicit(arg)) {
		return parsed{form: formUnknown, name: name, arg: arg, syntax: syn} // ⚠️ Wrong chat
	}
	return parsed{syntax: syn} // a question for Codex
}

// controlAdmin is v0.1.20's codex-update / controlAdminCommandName /
// controlCancelCommandName: the prefixed admin forms, checked before the
// natural names.
func controlAdmin(syn syntax, name, arg string) string {
	switch {
	case syn == synCodex && (name == "update" || name == "upgrade"):
		if arg == "" || strings.EqualFold(arg, "now") {
			return cmdCodexUpdate
		}
	case (name == "cancel" || name == "stop") && (syn == synSlash || syn == synBang || syn == synHelper && cancelArgExplicit(arg)):
		return cmdCancelName
	case syn != synHelper:
	case name == "restart" || name == "reboot":
		if arg == "" || restartConfirmed(arg) {
			return cmdRestartName
		}
	case name == "update" || name == "upgrade":
		if _, _, ok := parseUpdateArg(arg); arg == "" || ok {
			return cmdUpdate
		}
	case name == "webhook" || name == "workflow":
		if webhookArgExplicit(arg) {
			return cmdWebhook
		}
	case name == "service":
		sub, rest := splitBody(arg)
		switch sub {
		case "restart", "reboot":
			if rest == "" || restartConfirmed(arg) {
				return cmdRestartName
			}
		case "update", "upgrade":
			if _, _, ok := parseUpdateArg(arg); rest == "" || ok {
				return cmdUpdate
			}
		}
	}
	return ""
}

// restartConfirmed is v0.1.20's helperRestartConfirmed.
func restartConfirmed(arg string) bool {
	switch strings.ToLower(strings.TrimSpace(arg)) {
	case "now", "--now", "restart now", "restart --now", "force", "--force", "restart force", "restart --force":
		return true
	}
	return false
}

func pluralListName(name string) bool {
	switch name {
	case "projects", "workspaces", "workdirs", "dirs":
		return true
	}
	return false
}

// controlArgExplicit is v0.1.20's prefixedControlCommandArgLooksExplicit.
func controlArgExplicit(canon, arg string) bool {
	arg = strings.TrimSpace(arg)
	switch canon {
	case cmdHelp:
		return arg == "" || isAdvancedHelpArg(arg)
	case cmdSkillsName, cmdModelName, cmdEffortName, cmdDefault,
		cmdWorkspace, cmdOpen, cmdParkName, cmdResume, cmdPublish, cmdNewName, cmdAsk, cmdMkdir, cmdRenameName, cmdDetails,
		cmdFork:
		return true
	}
	return arg == ""
}

// cancelArgExplicit is v0.1.20's helperCancelCommandArgLooksExplicit.
func cancelArgExplicit(arg string) bool {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return true
	}
	first, rest := splitBody(arg)
	switch {
	case first == "last" || first == "all":
		return true
	case (first == "queued" || first == "running") && rest == "":
		return true
	}
	return strings.HasPrefix(first, "turn:") || strings.HasPrefix(first, "turn-")
}

// webhookArgExplicit is v0.1.20's helperWebhookCommandArgLooksExplicit.
func webhookArgExplicit(arg string) bool {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return true
	}
	name, rest := splitBody(arg)
	switch name {
	case "help", "?", "setup", "guide", "start", "status", "st", "off", "disable", "disabled", "test":
		return rest == ""
	case "set", "url", "enable":
		return strings.Contains(rest, "://")
	}
	return strings.Contains(arg, "://")
}

// workOnlyName: `helper …` names that only work in a work chat (v0.1.20
// helperPrefixedControlNameIsWorkOnly :154 / isWorkOnlyHelperCommand
// B:7330; the removed restore-thread is not). A bare `helper stats` is the
// rebuilt core's control stats (F-STA-1) and never gets here; `helper
// stats <arg>` does, as in v0.1.20.
func workOnlyName(name string) bool {
	switch name {
	case "close", "retry", "cancel", "stop", "send-file", "send-image", "file", "image",
		"publish-history", "sync-history", "import-history", "stats", "usage", "tokens":
		return true
	}
	return false
}

// codexMention is v0.1.20's codexPlainMentionPattern.
var codexMention = regexp.MustCompile(`(?i)(^|[^[:alnum:]_])@codex\b`)

// stripCodexMention removes the @codex mentions of a message (v0.1.20
// stripCodexMentionFromText) and reports whether there was one.
func stripCodexMention(text string) (string, bool) {
	if !codexMention.MatchString(text) {
		return text, false
	}
	return strings.Join(strings.Fields(codexMention.ReplaceAllString(text, "$1")), " "), true
}

// looksLikeControlPath is v0.1.20's looksLikeControlPath (B:7343).
func looksLikeControlPath(text string) bool {
	text = strings.TrimSpace(text)
	if text == "" || strings.ContainsAny(text, "\r\n") {
		return false
	}
	switch {
	case strings.HasPrefix(text, "./"), strings.HasPrefix(text, "../"), strings.HasPrefix(text, "~/"):
		return true
	case len(text) >= 3 && (text[0] >= 'A' && text[0] <= 'Z' || text[0] >= 'a' && text[0] <= 'z') && text[1] == ':' && (text[2] == '\\' || text[2] == '/'):
		return true
	}
	return false
}
