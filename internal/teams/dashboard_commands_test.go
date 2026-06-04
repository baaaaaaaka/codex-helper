package teams

import (
	"strings"
	"testing"
)

func TestParseControlDashboardCommandsDoNotRequireCodex(t *testing.T) {
	tests := []struct {
		text     string
		name     DashboardCommandName
		raw      string
		number   int
		isNumber bool
	}{
		{text: "/workspaces", name: DashboardCommandWorkspaces},
		{text: "/workspace 2", name: DashboardCommandWorkspace, raw: "2", number: 2, isNumber: true},
		{text: "/sessions", name: DashboardCommandSessions},
		{text: "/open 3", name: DashboardCommandOpen, raw: "3", number: 3, isNumber: true},
		{text: "/open session-abc", name: DashboardCommandOpen, raw: "session-abc"},
		{text: "/park session-abc", name: DashboardCommandPark, raw: "session-abc"},
		{text: "park s113", name: DashboardCommandPark, raw: "s113"},
		{text: "helper park s113", name: DashboardCommandPark, raw: "s113"},
		{text: "unpark s113", name: DashboardCommandResume, raw: "s113"},
		{text: "/publish session-def", name: DashboardCommandPublish, raw: "session-def"},
		{text: "/new build the thing", name: DashboardCommandNew, raw: "build the thing"},
		{text: "/ask what can this control chat do", name: DashboardCommandAsk, raw: "what can this control chat do"},
		{text: "/mkdir /tmp/new-work", name: DashboardCommandMkdir, raw: "/tmp/new-work"},
		{text: "/rename Better Title", name: DashboardCommandRename, raw: "Better Title"},
		{text: "helper rename Better Title", name: DashboardCommandRename, raw: "Better Title"},
		{text: "/details session-abc", name: DashboardCommandDetails, raw: "session-abc"},
		{text: "/help", name: DashboardCommandHelp},
		{text: "/status", name: DashboardCommandStatus},
		{text: "help", name: DashboardCommandHelp},
		{text: "h", name: DashboardCommandHelp},
		{text: "menu", name: DashboardCommandHelp},
		{text: "projects", name: DashboardCommandWorkspaces},
		{text: "p", name: DashboardCommandWorkspaces},
		{text: "p 2", name: DashboardCommandWorkspace, raw: "2", number: 2, isNumber: true},
		{text: "project 2", name: DashboardCommandWorkspace, raw: "2", number: 2, isNumber: true},
		{text: "s", name: DashboardCommandSessions},
		{text: "history", name: DashboardCommandSessions},
		{text: "c 3", name: DashboardCommandPublish, raw: "3", number: 3, isNumber: true},
		{text: "continue 3", name: DashboardCommandPublish, raw: "3", number: 3, isNumber: true},
		{text: "open 3", name: DashboardCommandOpen, raw: "3", number: 3, isNumber: true},
		{text: "d 3", name: DashboardCommandDetails, raw: "3", number: 3, isNumber: true},
		{text: "details 3", name: DashboardCommandDetails, raw: "3", number: 3, isNumber: true},
		{text: "n /tmp/new-work", name: DashboardCommandNew, raw: "/tmp/new-work"},
		{text: "new build the thing", name: DashboardCommandNew, raw: "build the thing"},
		{text: "new /tmp/new-work -- build the thing", name: DashboardCommandNew, raw: "/tmp/new-work -- build the thing"},
		{text: "ask what can this control chat do", name: DashboardCommandAsk, raw: "what can this control chat do"},
		{text: "m /tmp/new-work", name: DashboardCommandMkdir, raw: "/tmp/new-work"},
		{text: "mkdir /tmp/new-work", name: DashboardCommandMkdir, raw: "/tmp/new-work"},
		{text: "st", name: DashboardCommandStatus},
		{text: "status", name: DashboardCommandStatus},
		{text: "!continue 3", name: DashboardCommandPublish, raw: "3", number: 3, isNumber: true},
		{text: "!p 3", name: DashboardCommandPublish, raw: "3", number: 3, isNumber: true},
		{text: "codex continue 3", name: DashboardCommandPublish, raw: "3", number: 3, isNumber: true},
		{text: "codex help", name: DashboardCommandHelp},
		{text: "helper restart", name: DashboardCommandRestart},
		{text: "helper restart now", name: DashboardCommandRestart, raw: "now"},
		{text: "helper restart force", name: DashboardCommandRestart, raw: "force"},
		{text: "codex-helper service restart now", name: DashboardCommandRestart, raw: "restart now"},
		{text: "helper reload", name: DashboardCommandReload},
		{text: "helper reload now", name: DashboardCommandReload, raw: "now"},
		{text: "helper reload force", name: DashboardCommandReload, raw: "force"},
		{text: "codex-helper service reload now", name: DashboardCommandReload, raw: "reload now"},
		{text: "helper update now", name: DashboardCommandUpdate, raw: "now"},
		{text: "helper update prerelease", name: DashboardCommandUpdate, raw: "prerelease"},
		{text: "helper upgrade pre", name: DashboardCommandUpdate, raw: "pre"},
		{text: "codex-helper service update now", name: DashboardCommandUpdate, raw: "update now"},
		{text: "helper asr status", name: DashboardCommandASR, raw: "status"},
		{text: "helper asr warmup", name: DashboardCommandASR, raw: "warmup"},
		{text: "asr status", name: DashboardCommandASR, raw: "status"},
		{text: "helper webhook https://workflow.example.test/hook", name: DashboardCommandWebhook, raw: "https://workflow.example.test/hook"},
		{text: "helper workflow off", name: DashboardCommandWebhook, raw: "off"},
		{text: "helper status", name: DashboardCommandStatus},
		{text: "helper skills", name: DashboardCommandSkills},
		{text: "helper skills sync acme", name: DashboardCommandSkills, raw: "sync acme"},
		{text: "model list", name: DashboardCommandModel, raw: "list"},
		{text: "model key confirm ABCD2345", name: DashboardCommandModel, raw: "key confirm ABCD2345"},
		{text: "models", name: DashboardCommandModel},
		{text: "helper model default mimo25", name: DashboardCommandModel, raw: "default mimo25"},
		{text: "/model doctor deepseek", name: DashboardCommandModel, raw: "doctor deepseek"},
		{text: "beacon list", name: DashboardCommandBeacon, raw: "list"},
		{text: "/beacon machine list", name: DashboardCommandBeacon, raw: "machine list"},
		{text: "helper beacon profile list", name: DashboardCommandBeacon, raw: "profile list"},
		{text: "helper help advanced", name: DashboardCommandHelp, raw: "advanced"},
		{text: "cx p 3", name: DashboardCommandPublish, raw: "3", number: 3, isNumber: true},
		{text: "cx h", name: DashboardCommandHelp},
		{text: "4", name: DashboardCommandSelect, raw: "4", number: 4, isNumber: true},
	}

	for _, tt := range tests {
		t.Run(tt.text, func(t *testing.T) {
			cmd := ParseDashboardCommand(ChatScopeControl, tt.text)
			if cmd.Scope != ChatScopeControl || !cmd.HelperCommand {
				t.Fatalf("command scope/helper = %q/%v, want control/helper: %#v", cmd.Scope, cmd.HelperCommand, cmd)
			}
			if cmd.Name != tt.name {
				t.Fatalf("command name = %q, want %q", cmd.Name, tt.name)
			}
			if cmd.ForwardToCodex || cmd.RequiresCodex {
				t.Fatalf("dashboard command should not require Codex: %#v", cmd)
			}
			if cmd.Target.Raw != tt.raw || cmd.Target.Number != tt.number || cmd.Target.IsNumber != tt.isNumber {
				t.Fatalf("target = %#v, want raw=%q number=%d isNumber=%v", cmd.Target, tt.raw, tt.number, tt.isNumber)
			}
		})
	}
}

func TestParseControlRestartRequiresHelperPrefix(t *testing.T) {
	for _, text := range []string{"restart", "restart now", "codex restart"} {
		t.Run(text, func(t *testing.T) {
			cmd := ParseDashboardCommand(ChatScopeControl, text)
			if cmd.HelperCommand || !cmd.ForwardToCodex || !cmd.RequiresCodex {
				t.Fatalf("restart text without helper prefix should go to Codex, got %#v", cmd)
			}
		})
	}
}

func TestParseControlUpdateRequiresHelperPrefix(t *testing.T) {
	for _, text := range []string{"update", "update now", "upgrade", "upgrade prerelease", "codex update"} {
		t.Run(text, func(t *testing.T) {
			cmd := ParseDashboardCommand(ChatScopeControl, text)
			if cmd.HelperCommand || !cmd.ForwardToCodex || !cmd.RequiresCodex {
				t.Fatalf("update text without helper prefix should go to Codex, got %#v", cmd)
			}
		})
	}
}

func TestParseControlHelperAdminNaturalLanguageFallsBackToCodex(t *testing.T) {
	for _, text := range []string{
		"helper upgrade 能够更新成避免 api 访问过于频繁的报错吗",
		"helper update can this avoid GitHub API rate limits",
		"helper restart 为什么会卡住",
		"helper reload should we use the newest code",
		"helper webhook 能不能一键配置",
		"helper status 为什么没有更新",
		"helper help 怎么用",
		"helper sessions 为什么这么慢",
		"codex-helper service update 这个机制安全吗",
	} {
		t.Run(text, func(t *testing.T) {
			cmd := ParseDashboardCommand(ChatScopeControl, text)
			if cmd.HelperCommand || !cmd.ForwardToCodex || !cmd.RequiresCodex {
				t.Fatalf("natural helper-prefixed text should go to Codex, got %#v", cmd)
			}
		})
	}
}

func TestParseControlReloadRequiresHelperPrefix(t *testing.T) {
	for _, text := range []string{"reload", "reload now", "/reload", "codex reload"} {
		t.Run(text, func(t *testing.T) {
			cmd := ParseDashboardCommand(ChatScopeControl, text)
			if strings.HasPrefix(text, "/") {
				if !cmd.HelperCommand || cmd.Name != DashboardCommandUnknown || cmd.ForwardToCodex || cmd.RequiresCodex {
					t.Fatalf("unknown slash reload parse mismatch: %#v", cmd)
				}
				return
			}
			if cmd.HelperCommand || !cmd.ForwardToCodex || !cmd.RequiresCodex {
				t.Fatalf("reload text without helper prefix should go to Codex, got %#v", cmd)
			}
		})
	}
}

func TestParseControlUnknownInputForwardsOnlyPlainTextToCodex(t *testing.T) {
	for _, text := range []string{"帮我看看现在该怎么操作", "codex explain this error", "/not-a-helper-command please"} {
		t.Run(text, func(t *testing.T) {
			cmd := ParseDashboardCommand(ChatScopeControl, text)
			if strings.HasPrefix(text, "/") {
				if !cmd.HelperCommand || cmd.Name != DashboardCommandUnknown || cmd.ForwardToCodex || cmd.RequiresCodex {
					t.Fatalf("unknown slash command parse mismatch: %#v", cmd)
				}
				return
			}
			if !cmd.ForwardToCodex || !cmd.RequiresCodex || cmd.HelperCommand {
				t.Fatalf("plain control text should not be a helper command: %#v", cmd)
			}
		})
	}
}

func TestParseControlCancelTargetsControlFallback(t *testing.T) {
	for _, text := range []string{"helper cancel", "helper cancel last", "helper cancel all", "helper cancel turn:inbound:control:message", "helper stop turn-123", "helper stop last", "!cancel last", "/cancel last"} {
		t.Run(text, func(t *testing.T) {
			cmd := ParseDashboardCommand(ChatScopeControl, text)
			if !cmd.HelperCommand || cmd.Name != DashboardCommandCancel {
				t.Fatalf("control cancel parsed as %#v, want helper cancel", cmd)
			}
			if cmd.ForwardToCodex || cmd.RequiresCodex {
				t.Fatalf("control cancel should not be forwarded to Codex: %#v", cmd)
			}
		})
	}
}

func TestParseControlCancelNaturalLanguageFallsBackToCodex(t *testing.T) {
	for _, text := range []string{
		"helper cancel 为什么不能取消",
		"helper stop should explain the stuck request",
		"helper cancel can you tell me how this works",
	} {
		t.Run(text, func(t *testing.T) {
			cmd := ParseDashboardCommand(ChatScopeControl, text)
			if cmd.HelperCommand || !cmd.ForwardToCodex || !cmd.RequiresCodex {
				t.Fatalf("natural control cancel text should go to Codex, got %#v", cmd)
			}
		})
	}
}

func TestParseWorkChatPlainTextIsCodexInput(t *testing.T) {
	for _, text := range []string{"1", "status", "stats", "details", "close", "park s001", "resume s001", "rename this chat", "retry the failed test", "helper upgrade 能够更新成避免 api 访问过于频繁的报错吗", "helper webhook 能不能一键配置"} {
		t.Run(text, func(t *testing.T) {
			cmd := ParseDashboardCommand(ChatScopeWork, text)
			if cmd.HelperCommand {
				t.Fatalf("work plaintext %q parsed as helper command: %#v", text, cmd)
			}
			if !cmd.ForwardToCodex || !cmd.RequiresCodex {
				t.Fatalf("work plaintext %q should be forwarded to Codex: %#v", text, cmd)
			}
		})
	}

	for _, text := range []string{"help", "help advanced", "h advanced", "/status", "/stats", "/close", "/park s001", "/resume s001", "/unpark s001", "/help", "/details", "!status", "!stats", "!file report.txt", "!ph", "helper status", "helper stats", "helper usage", "helper retry turn-1", "helper park s001", "helper resume s001", "helper unpark s001", "helper file report.txt", "helper publish-history", "helper skills push", "model status", "models", "helper model switch mimo25", "codex status", "codex stats", "codex send-file report.txt"} {
		t.Run(text, func(t *testing.T) {
			cmd := ParseDashboardCommand(ChatScopeWork, text)
			if !cmd.HelperCommand {
				t.Fatalf("work slash command %q was not helper command: %#v", text, cmd)
			}
			if cmd.ForwardToCodex || cmd.RequiresCodex {
				t.Fatalf("work slash command %q should not require Codex: %#v", text, cmd)
			}
		})
	}
	if cmd := ParseDashboardCommand(ChatScopeWork, "help advanced"); cmd.Name != DashboardCommandHelp || cmd.Argument != "advanced" {
		t.Fatalf("help advanced parse = %#v, want help with advanced arg", cmd)
	}
	if cmd := ParseDashboardCommand(ChatScopeWork, "helper stats"); cmd.Name != DashboardCommandStats {
		t.Fatalf("helper stats parse = %#v, want stats", cmd)
	}
	if cmd := ParseDashboardCommand(ChatScopeWork, "helper park s001"); cmd.Name != DashboardCommandPark || cmd.Argument != "s001" {
		t.Fatalf("helper park parse = %#v, want park s001", cmd)
	}
	if cmd := ParseDashboardCommand(ChatScopeWork, "helper unpark s001"); cmd.Name != DashboardCommandResume || cmd.Argument != "s001" {
		t.Fatalf("helper unpark parse = %#v, want resume s001", cmd)
	}
}

func TestParseBeaconWorkDashboardCommands(t *testing.T) {
	tests := []struct {
		text string
		arg  string
	}{
		{text: "beacon status", arg: "status"},
		{text: "/beacon list", arg: "list"},
		{text: "!beacon switch gpu", arg: "switch gpu"},
		{text: "helper beacon switch local", arg: "switch local"},
		{text: "codex beacon fork gpu", arg: "fork gpu"},
	}

	for _, tt := range tests {
		t.Run(tt.text, func(t *testing.T) {
			cmd := ParseDashboardCommand(ChatScopeWork, tt.text)
			if !cmd.HelperCommand || cmd.Name != DashboardCommandBeacon {
				t.Fatalf("beacon work command parsed as %#v", cmd)
			}
			if cmd.Argument != tt.arg {
				t.Fatalf("beacon work argument = %q, want %q", cmd.Argument, tt.arg)
			}
			if cmd.ForwardToCodex || cmd.RequiresCodex {
				t.Fatalf("beacon work command should not be forwarded to Codex: %#v", cmd)
			}
		})
	}
}

func TestParseRestoreThreadWorkCommand(t *testing.T) {
	for _, text := range []string{
		"helper restore-thread thread-123",
		"helper restore thread-123",
		"/restore-thread thread-123",
		"!rs thread-123",
	} {
		cmd := ParseDashboardCommand(ChatScopeWork, text)
		if !cmd.HelperCommand || cmd.Name != DashboardCommandRestoreThread || cmd.Argument != "thread-123" {
			t.Fatalf("ParseDashboardCommand(%q) = %#v, want restore-thread thread-123", text, cmd)
		}
	}
}

func TestParseWorkChatUnknownSlashIsCodexInput(t *testing.T) {
	for _, text := range []string{"/tmp/a.log 这个文件是什么", "/tmp/status should be checked", "/usr/bin/env bash -lc pwd", "codex explain this error", "codex h"} {
		t.Run(text, func(t *testing.T) {
			cmd := ParseDashboardCommand(ChatScopeWork, text)
			if cmd.HelperCommand || !cmd.ForwardToCodex || !cmd.RequiresCodex {
				t.Fatalf("work unknown slash should be forwarded to Codex: %#v", cmd)
			}
		})
	}
}
