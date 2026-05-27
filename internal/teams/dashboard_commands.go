package teams

import "strings"

type dashboardCommandSyntax string

const (
	dashboardCommandSyntaxNone  dashboardCommandSyntax = ""
	dashboardCommandSyntaxSlash dashboardCommandSyntax = "slash"
	dashboardCommandSyntaxBang  dashboardCommandSyntax = "bang"
	dashboardCommandSyntaxCX    dashboardCommandSyntax = "cx"
	dashboardCommandSyntaxCodex dashboardCommandSyntax = "codex"
	dashboardCommandSyntaxHelp  dashboardCommandSyntax = "helper"
)

type DashboardCommandName string

const (
	DashboardCommandNone           DashboardCommandName = ""
	DashboardCommandUnknown        DashboardCommandName = "unknown"
	DashboardCommandSelect         DashboardCommandName = "select"
	DashboardCommandWorkspaces     DashboardCommandName = "workspaces"
	DashboardCommandWorkspace      DashboardCommandName = "workspace"
	DashboardCommandSessions       DashboardCommandName = "sessions"
	DashboardCommandOpen           DashboardCommandName = "open"
	DashboardCommandResume         DashboardCommandName = "resume"
	DashboardCommandPublish        DashboardCommandName = "publish"
	DashboardCommandNew            DashboardCommandName = "new"
	DashboardCommandAsk            DashboardCommandName = "ask"
	DashboardCommandMkdir          DashboardCommandName = "mkdir"
	DashboardCommandRename         DashboardCommandName = "rename"
	DashboardCommandDetails        DashboardCommandName = "details"
	DashboardCommandHelp           DashboardCommandName = "help"
	DashboardCommandStatus         DashboardCommandName = "status"
	DashboardCommandStats          DashboardCommandName = "stats"
	DashboardCommandRestart        DashboardCommandName = "restart"
	DashboardCommandReload         DashboardCommandName = "reload"
	DashboardCommandUpdate         DashboardCommandName = "update"
	DashboardCommandWebhook        DashboardCommandName = "webhook"
	DashboardCommandPublishHistory DashboardCommandName = "publish-history"
	DashboardCommandClose          DashboardCommandName = "close"
	DashboardCommandRetry          DashboardCommandName = "retry"
	DashboardCommandRestoreThread  DashboardCommandName = "restore-thread"
	DashboardCommandCancel         DashboardCommandName = "cancel"
	DashboardCommandSendFile       DashboardCommandName = "send-file"
	DashboardCommandSkills         DashboardCommandName = "skills"
	DashboardCommandBeacon         DashboardCommandName = "beacon"
)

type ParsedDashboardCommand struct {
	Scope          ChatScope
	Name           DashboardCommandName
	Argument       string
	Target         DashboardCommandTarget
	HelperCommand  bool
	ForwardToCodex bool
	RequiresCodex  bool
}

type DashboardCommandTarget struct {
	Raw      string
	Number   int
	IsNumber bool
}

func ParseDashboardCommand(scope ChatScope, text string) ParsedDashboardCommand {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return ParsedDashboardCommand{Scope: scope}
	}
	if scope == ChatScopeWork {
		return parseWorkChatCommand(trimmed)
	}
	if number, ok := parseBarePositiveInt(trimmed); ok {
		return ParsedDashboardCommand{
			Scope:         ChatScopeControl,
			Name:          DashboardCommandSelect,
			Target:        DashboardCommandTarget{Raw: trimmed, Number: number, IsNumber: true},
			HelperCommand: true,
		}
	}
	if name, arg, ok := splitNaturalControlCommand(trimmed); ok {
		commandName, _ := controlNaturalCommandName(name, arg)
		return ParsedDashboardCommand{
			Scope:         ChatScopeControl,
			Name:          commandName,
			Argument:      arg,
			Target:        parseDashboardCommandTarget(arg),
			HelperCommand: true,
		}
	}
	name, arg, syntax, hasCommandPrefix := splitDashboardCommand(trimmed)
	if !hasCommandPrefix {
		return ParsedDashboardCommand{
			Scope:          ChatScopeControl,
			ForwardToCodex: true,
			RequiresCodex:  true,
		}
	}
	commandName, ok := controlDashboardCommandName(syntax, name, arg)
	if !ok {
		if syntax == dashboardCommandSyntaxHelp && helperPrefixedControlTextShouldStayHelperUnknown(name, arg) {
			commandName = DashboardCommandUnknown
		} else if syntax == dashboardCommandSyntaxCodex || syntax == dashboardCommandSyntaxHelp {
			return ParsedDashboardCommand{
				Scope:          ChatScopeControl,
				Argument:       strings.TrimSpace(trimmed),
				Target:         parseDashboardCommandTarget(trimmed),
				ForwardToCodex: true,
				RequiresCodex:  true,
			}
		} else {
			commandName = DashboardCommandUnknown
		}
	}
	return ParsedDashboardCommand{
		Scope:          ChatScopeControl,
		Name:           commandName,
		Argument:       arg,
		Target:         parseDashboardCommandTarget(arg),
		HelperCommand:  true,
		ForwardToCodex: false,
		RequiresCodex:  false,
	}
}

func helperPrefixedControlTextShouldStayHelperUnknown(name string, arg string) bool {
	if helperPrefixedControlNameIsWorkOnly(name) {
		return true
	}
	return false
}

func helperPrefixedControlNameIsWorkOnly(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "close", "retry", "restore-thread", "restore", "cancel", "stop", "send-file", "send-image", "file", "image", "publish-history", "sync-history", "import-history", "stats", "usage", "tokens":
		return true
	default:
		return false
	}
}

func parseWorkChatCommand(trimmed string) ParsedDashboardCommand {
	if commandName, arg, ok := workBareCommandName(trimmed); ok {
		return ParsedDashboardCommand{
			Scope:         ChatScopeWork,
			Name:          commandName,
			Argument:      arg,
			HelperCommand: true,
		}
	}
	name, arg, syntax, hasCommandPrefix := splitDashboardCommand(trimmed)
	if !hasCommandPrefix {
		return ParsedDashboardCommand{
			Scope:          ChatScopeWork,
			ForwardToCodex: true,
			RequiresCodex:  true,
		}
	}
	commandName, ok := workChatCommandName(syntax, name, arg)
	if !ok {
		if syntax == dashboardCommandSyntaxSlash || syntax == dashboardCommandSyntaxCodex || syntax == dashboardCommandSyntaxHelp {
			return ParsedDashboardCommand{
				Scope:          ChatScopeWork,
				Name:           DashboardCommandUnknown,
				Argument:       strings.TrimSpace(trimmed),
				Target:         parseDashboardCommandTarget(trimmed),
				ForwardToCodex: true,
				RequiresCodex:  true,
			}
		}
		return ParsedDashboardCommand{
			Scope:          ChatScopeWork,
			Name:           DashboardCommandUnknown,
			Argument:       arg,
			Target:         parseDashboardCommandTarget(arg),
			HelperCommand:  true,
			ForwardToCodex: false,
			RequiresCodex:  false,
		}
	}
	return ParsedDashboardCommand{
		Scope:         ChatScopeWork,
		Name:          commandName,
		Argument:      arg,
		Target:        parseDashboardCommandTarget(arg),
		HelperCommand: true,
	}
}

func workBareCommandName(text string) (DashboardCommandName, string, bool) {
	name, arg := splitDashboardCommandBody(text)
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "help", "menu", "?":
		if strings.TrimSpace(arg) == "" || isAdvancedHelpArg(arg) {
			return DashboardCommandHelp, arg, true
		}
	case "h":
		if strings.TrimSpace(arg) == "" || isAdvancedHelpArg(arg) {
			return DashboardCommandHelp, arg, true
		}
	case "beacon":
		return DashboardCommandBeacon, arg, true
	default:
	}
	return DashboardCommandNone, "", false
}

func splitNaturalControlCommand(text string) (string, string, bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return "", "", false
	}
	name, arg := splitDashboardCommandBody(text)
	if _, ok := controlNaturalCommandName(name, arg); ok {
		return name, arg, true
	}
	return "", "", false
}

func controlDashboardCommandName(syntax dashboardCommandSyntax, name string, arg string) (DashboardCommandName, bool) {
	if commandName, ok := controlAdminCommandName(syntax, name, arg); ok {
		return commandName, true
	}
	switch syntax {
	case dashboardCommandSyntaxSlash, dashboardCommandSyntaxBang, dashboardCommandSyntaxCX:
		return controlLegacyCommandName(name, arg)
	case dashboardCommandSyntaxCodex, dashboardCommandSyntaxHelp:
		return prefixedControlNaturalCommandName(name, arg)
	default:
		return DashboardCommandNone, false
	}
}

func controlAdminCommandName(syntax dashboardCommandSyntax, name string, arg string) (DashboardCommandName, bool) {
	if syntax != dashboardCommandSyntaxHelp {
		return DashboardCommandNone, false
	}
	name = strings.ToLower(strings.TrimSpace(name))
	argName, argRest := splitDashboardCommandBody(arg)
	argName = strings.ToLower(strings.TrimSpace(argName))
	arg = strings.TrimSpace(arg)
	switch name {
	case "restart", "reboot":
		if arg == "" || helperRestartConfirmed(arg) {
			return DashboardCommandRestart, true
		}
	case "reload":
		if arg == "" || helperReloadConfirmed(arg) {
			return DashboardCommandReload, true
		}
	case "update", "upgrade":
		if arg == "" {
			return DashboardCommandUpdate, true
		}
		if _, ok := parseHelperUpdateRequest(arg); ok {
			return DashboardCommandUpdate, true
		}
	case "webhook", "workflow":
		if helperWebhookCommandArgLooksExplicit(arg) {
			return DashboardCommandWebhook, true
		}
	case "service":
		if argName == "restart" || argName == "reboot" {
			if strings.TrimSpace(argRest) == "" || helperRestartConfirmed(arg) {
				return DashboardCommandRestart, true
			}
		}
		if argName == "reload" {
			if strings.TrimSpace(argRest) == "" || helperReloadConfirmed(arg) {
				return DashboardCommandReload, true
			}
		}
		if argName == "update" || argName == "upgrade" {
			if strings.TrimSpace(argRest) == "" {
				return DashboardCommandUpdate, true
			}
			if _, ok := parseHelperUpdateRequest(arg); ok {
				return DashboardCommandUpdate, true
			}
		}
	}
	return DashboardCommandNone, false
}

func helperWebhookCommandArgLooksExplicit(arg string) bool {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return true
	}
	name, rest := splitDashboardCommandBody(arg)
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "help", "?", "setup", "guide", "start", "status", "st", "off", "disable", "disabled", "test":
		return strings.TrimSpace(rest) == ""
	case "set", "url", "enable":
		return strings.Contains(strings.TrimSpace(rest), "://")
	default:
		return strings.Contains(arg, "://")
	}
}

func prefixedControlNaturalCommandName(name string, arg string) (DashboardCommandName, bool) {
	commandName, ok := controlNaturalCommandName(name, arg)
	if !ok {
		return DashboardCommandNone, false
	}
	if prefixedControlCommandArgLooksExplicit(commandName, arg) {
		return commandName, true
	}
	return DashboardCommandNone, false
}

func prefixedControlCommandArgLooksExplicit(commandName DashboardCommandName, arg string) bool {
	arg = strings.TrimSpace(arg)
	switch commandName {
	case DashboardCommandHelp:
		return arg == "" || isAdvancedHelpArg(arg)
	case DashboardCommandWorkspaces, DashboardCommandSessions, DashboardCommandStatus:
		return arg == ""
	case DashboardCommandSkills, DashboardCommandBeacon:
		return true
	case DashboardCommandWorkspace, DashboardCommandOpen, DashboardCommandResume, DashboardCommandPublish, DashboardCommandNew, DashboardCommandAsk, DashboardCommandMkdir, DashboardCommandRename, DashboardCommandDetails:
		return true
	default:
		return arg == ""
	}
}

func controlNaturalCommandName(name string, arg string) (DashboardCommandName, bool) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "p", "proj", "projects", "project", "workspaces", "workdirs", "dirs":
		if strings.TrimSpace(arg) != "" && !isPluralControlListName(name) {
			return DashboardCommandWorkspace, true
		}
		return DashboardCommandWorkspaces, true
	case "w", "ws", "workspace", "workdir", "dir":
		return DashboardCommandWorkspace, true
	case "s", "sessions", "session", "history":
		return DashboardCommandSessions, true
	case "o", "open":
		return DashboardCommandOpen, true
	case "r", "resume":
		return DashboardCommandResume, true
	case "c", "continue", "publish", "import":
		return DashboardCommandPublish, true
	case "n", "new", "create":
		return DashboardCommandNew, true
	case "q", "ask", "question":
		return DashboardCommandAsk, true
	case "m", "mkdir", "folder":
		return DashboardCommandMkdir, true
	case "rn", "rename":
		return DashboardCommandRename, true
	case "d", "details", "detail":
		return DashboardCommandDetails, true
	case "", "h", "help", "menu", "?":
		return DashboardCommandHelp, true
	case "st", "status":
		return DashboardCommandStatus, true
	case "skills", "skill":
		return DashboardCommandSkills, true
	case "beacon":
		return DashboardCommandBeacon, true
	default:
		return DashboardCommandNone, false
	}
}

func isPluralControlListName(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "projects", "workspaces", "workdirs", "dirs":
		return true
	default:
		return false
	}
}

func controlLegacyCommandName(name string, arg string) (DashboardCommandName, bool) {
	if strings.EqualFold(strings.TrimSpace(name), "p") {
		return DashboardCommandPublish, true
	}
	if commandName, ok := controlNaturalCommandName(name, arg); ok {
		return commandName, true
	}
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "w", "ws":
		return DashboardCommandWorkspaces, true
	case "wd":
		return DashboardCommandWorkspace, true
	case "s":
		return DashboardCommandSessions, true
	case "o":
		return DashboardCommandOpen, true
	case "r":
		return DashboardCommandResume, true
	case "pub", "p":
		return DashboardCommandPublish, true
	case "n":
		return DashboardCommandNew, true
	case "q":
		return DashboardCommandAsk, true
	case "m":
		return DashboardCommandMkdir, true
	case "rn":
		return DashboardCommandRename, true
	case "d":
		return DashboardCommandDetails, true
	case "h":
		return DashboardCommandHelp, true
	case "st":
		return DashboardCommandStatus, true
	default:
		return DashboardCommandNone, false
	}
}

func workChatCommandName(syntax dashboardCommandSyntax, name string, arg string) (DashboardCommandName, bool) {
	switch syntax {
	case dashboardCommandSyntaxSlash, dashboardCommandSyntaxBang, dashboardCommandSyntaxCX:
		return workLegacyCommandName(name, arg)
	case dashboardCommandSyntaxCodex, dashboardCommandSyntaxHelp:
		return workNaturalCommandName(name, arg)
	default:
		return DashboardCommandNone, false
	}
}

func workNaturalCommandName(name string, _ string) (DashboardCommandName, bool) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "help", "menu", "?":
		return DashboardCommandHelp, true
	case "status":
		return DashboardCommandStatus, true
	case "stats", "usage", "tokens":
		return DashboardCommandStats, true
	case "close":
		return DashboardCommandClose, true
	case "retry":
		return DashboardCommandRetry, true
	case "restore-thread", "restore":
		return DashboardCommandRestoreThread, true
	case "cancel", "stop":
		return DashboardCommandCancel, true
	case "send-file", "send-image", "file", "image":
		return DashboardCommandSendFile, true
	case "rename":
		return DashboardCommandRename, true
	case "details", "detail":
		return DashboardCommandDetails, true
	case "publish-history", "sync-history", "import-history":
		return DashboardCommandPublishHistory, true
	case "skills", "skill":
		return DashboardCommandSkills, true
	case "beacon":
		return DashboardCommandBeacon, true
	default:
		return DashboardCommandNone, false
	}
}

func workLegacyCommandName(name string, arg string) (DashboardCommandName, bool) {
	if commandName, ok := workNaturalCommandName(name, arg); ok {
		return commandName, true
	}
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "h":
		return DashboardCommandHelp, true
	case "st":
		return DashboardCommandStatus, true
	case "stats", "usage", "tokens":
		return DashboardCommandStats, true
	case "cl":
		return DashboardCommandClose, true
	case "rt":
		return DashboardCommandRetry, true
	case "rs":
		return DashboardCommandRestoreThread, true
	case "x":
		return DashboardCommandCancel, true
	case "img", "f":
		return DashboardCommandSendFile, true
	case "rn":
		return DashboardCommandRename, true
	case "d":
		return DashboardCommandDetails, true
	case "ph", "sync":
		return DashboardCommandPublishHistory, true
	default:
		return DashboardCommandNone, false
	}
}

func splitDashboardCommand(text string) (string, string, dashboardCommandSyntax, bool) {
	text = strings.TrimSpace(text)
	switch {
	case strings.HasPrefix(text, "/"):
		name, arg := splitDashboardCommandBody(strings.TrimSpace(strings.TrimPrefix(text, "/")))
		return name, arg, dashboardCommandSyntaxSlash, true
	case strings.HasPrefix(text, "!"):
		name, arg := splitDashboardCommandBody(strings.TrimSpace(strings.TrimPrefix(text, "!")))
		return name, arg, dashboardCommandSyntaxBang, true
	}
	prefix, rest, ok := strings.Cut(text, " ")
	if !ok {
		token := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(prefix)), ":")
		switch token {
		case "cx":
			return "", "", dashboardCommandSyntaxCX, true
		case "codex":
			return "", "", dashboardCommandSyntaxCodex, true
		case "helper", "assistant", "codex-helper":
			return "", "", dashboardCommandSyntaxHelp, true
		}
		return "", "", dashboardCommandSyntaxNone, false
	}
	token := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(prefix)), ":")
	var syntax dashboardCommandSyntax
	switch token {
	case "cx":
		syntax = dashboardCommandSyntaxCX
	case "codex":
		syntax = dashboardCommandSyntaxCodex
	case "helper", "assistant", "codex-helper":
		syntax = dashboardCommandSyntaxHelp
	default:
		return "", "", dashboardCommandSyntaxNone, false
	}
	name, arg := splitDashboardCommandBody(rest)
	return name, arg, syntax, true
}

func splitDashboardCommandBody(text string) (string, string) {
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

func parseDashboardCommandTarget(arg string) DashboardCommandTarget {
	arg = strings.TrimSpace(arg)
	target := DashboardCommandTarget{Raw: arg}
	if number, ok := parseBarePositiveInt(arg); ok {
		target.Number = number
		target.IsNumber = true
	}
	return target
}
