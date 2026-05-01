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
		{text: "/publish session-def", name: DashboardCommandPublish, raw: "session-def"},
		{text: "/new build the thing", name: DashboardCommandNew, raw: "build the thing"},
		{text: "/ask what can this control chat do", name: DashboardCommandAsk, raw: "what can this control chat do"},
		{text: "/mkdir /tmp/new-work", name: DashboardCommandMkdir, raw: "/tmp/new-work"},
		{text: "/rename Better Title", name: DashboardCommandRename, raw: "Better Title"},
		{text: "/details session-abc", name: DashboardCommandDetails, raw: "session-abc"},
		{text: "/help", name: DashboardCommandHelp},
		{text: "/status", name: DashboardCommandStatus},
		{text: "help", name: DashboardCommandHelp},
		{text: "menu", name: DashboardCommandHelp},
		{text: "projects", name: DashboardCommandWorkspaces},
		{text: "project 2", name: DashboardCommandWorkspace, raw: "2", number: 2, isNumber: true},
		{text: "history", name: DashboardCommandSessions},
		{text: "continue 3", name: DashboardCommandPublish, raw: "3", number: 3, isNumber: true},
		{text: "open 3", name: DashboardCommandOpen, raw: "3", number: 3, isNumber: true},
		{text: "details 3", name: DashboardCommandDetails, raw: "3", number: 3, isNumber: true},
		{text: "new build the thing", name: DashboardCommandNew, raw: "build the thing"},
		{text: "new /tmp/new-work -- build the thing", name: DashboardCommandNew, raw: "/tmp/new-work -- build the thing"},
		{text: "ask what can this control chat do", name: DashboardCommandAsk, raw: "what can this control chat do"},
		{text: "mkdir /tmp/new-work", name: DashboardCommandMkdir, raw: "/tmp/new-work"},
		{text: "status", name: DashboardCommandStatus},
		{text: "!continue 3", name: DashboardCommandPublish, raw: "3", number: 3, isNumber: true},
		{text: "!p 3", name: DashboardCommandPublish, raw: "3", number: 3, isNumber: true},
		{text: "codex continue 3", name: DashboardCommandPublish, raw: "3", number: 3, isNumber: true},
		{text: "codex help", name: DashboardCommandHelp},
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

func TestParseControlUnknownInputForwardsOnlyPlainTextToCodex(t *testing.T) {
	for _, text := range []string{"帮我看看现在该怎么操作", "codex explain this error", "codex h", "codex p 1", "/not-a-helper-command please"} {
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

func TestParseWorkChatPlainTextIsCodexInput(t *testing.T) {
	for _, text := range []string{"1", "status", "details", "close", "rename this chat", "retry the failed test"} {
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

	for _, text := range []string{"help", "/status", "/close", "/help", "/details", "!status", "!file report.txt", "helper status", "helper retry turn-1", "helper file report.txt", "codex status", "codex send-file report.txt"} {
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
