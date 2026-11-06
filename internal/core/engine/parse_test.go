package engine

import "testing"

// The chat command grammar follows v0.1.20 dashboard_commands.go
// (F-TRN-7, F-TRN-8, F-CTL-9).
func TestParseWork(t *testing.T) {
	for _, c := range []struct {
		text string
		form cmdForm
		name string
		arg  string
	}{
		// Prefixes and the trailing colon.
		{"helper status", formCommand, cmdStatus, ""},
		{"Helper: status", formCommand, cmdStatus, ""},
		{"!status", formCommand, cmdStatus, ""},
		{"/status", formCommand, cmdStatus, ""},
		{"cx status", formCommand, cmdStatus, ""},
		{"codex status", formCommand, cmdStatus, ""},
		{"assistant close", formCommand, cmdClose, ""},
		{"helper", formCommand, cmdHelp, ""},
		{"!", formCommand, cmdHelp, ""},
		// Short names: only after `/`, `!`, `cx`.
		{"!st", formCommand, cmdStatus, ""},
		{"/cl", formCommand, cmdClose, ""},
		{"cx rt last", formCommand, cmdRetryName, "last"},
		{"!x all", formCommand, cmdCancelName, "all"},
		{"!f out/report.md", formCommand, cmdSendFile, "out/report.md"},
		{"!rn New Title", formCommand, cmdRenameName, "New Title"},
		{"!d", formCommand, cmdDetails, ""},
		{"!ph", formCommand, cmdPublishHistory, ""},
		{"helper st", formText, "", ""},
		{"codex cl", formText, "", ""},
		// Bare words.
		{"help", formCommand, cmdHelp, ""},
		{"help advanced", formCommand, cmdHelp, "advanced"},
		{"help me fix this", formText, "", ""},
		{"model", formCommand, cmdModelName, ""},
		{"models", formCommand, cmdModelName, "list"},
		{"efforts", formCommand, cmdEffortName, "list"},
		{"effort high", formCommand, cmdEffortName, "high"},
		{"default model x", formCommand, cmdDefault, "model x"},
		{"status", formText, "", ""},
		{"close", formText, "", ""},
		// Unknown names: `/`, `codex`, `helper` go to Codex; `!`, `cx` do not.
		{"/compact", formText, "", ""},
		{"helper frobnicate", formText, "", ""},
		{"codex explain this", formText, "", ""},
		{"!frobnicate", formUnknown, "frobnicate", ""},
		{"cx frobnicate", formUnknown, "frobnicate", ""},
		// Removed commands never reach Codex.
		{"helper restore-thread abc", formUnknown, "restore-thread", "abc"},
		{"helper reload", formUnknown, "reload", ""},
		{"!rs", formUnknown, "rs", ""},
		{"/beacon", formUnknown, "beacon", ""},
		{"beacon status", formUnknown, "beacon", "status"},
		{"reload the page", formText, "", ""},
		// The core's own commands.
		{"helper unpause", formCommand, cmdUnpause, ""},
		{"helper fork", formCommand, cmdFork, ""},
		{"", formText, "", ""},
		{"fix the bug", formText, "", ""},
	} {
		p := parseWork(c.text)
		if p.form != c.form || p.name != c.name || p.arg != c.arg {
			t.Errorf("parseWork(%q) = %+v, want form %d name %q arg %q", c.text, p, c.form, c.name, c.arg)
		}
	}
}

func TestParseControl(t *testing.T) {
	for _, c := range []struct {
		text string
		form cmdForm
		name string
		arg  string
	}{
		{"3", formCommand, cmdSelect, "3"},
		{"0", formText, "", ""},
		{"+3", formText, "", ""},
		{"p", formCommand, cmdWorkspaces, ""},
		{"p 2", formCommand, cmdWorkspace, "2"},
		{"projects 2", formCommand, cmdWorkspaces, "2"},
		{"!p 2", formCommand, cmdPublish, "2"},
		{"!w", formCommand, cmdWorkspace, ""},
		{"!wd x", formCommand, cmdWorkspace, "x"},
		{"st", formCommand, cmdStatus, ""},
		{"!st", formCommand, cmdStatus, ""},
		{"/status", formCommand, cmdStatus, ""},
		{"helper st", formCommand, cmdStatus, ""},
		{"helper status please", formText, "", ""},
		{"r s003", formCommand, cmdResume, "s003"},
		{"new /tmp/x", formCommand, cmdNewName, "/tmp/x"},
		{"help advanced", formCommand, cmdHelp, "advanced"},
		{"helper help", formCommand, cmdHelp, ""},
		{"helper", formCommand, cmdHelp, ""},
		{"shutdown", formCommand, cmdShutdown, ""},
		{"!shutdown", formCommand, cmdShutdown, ""},
		{"/shutdown", formCommand, cmdShutdown, ""},
		{"helper shutdown", formCommand, cmdShutdown, ""},
		{"codex shutdown", formText, "", ""},
		{"codex update now", formCommand, cmdCodexUpdate, "update now"},
		{"codex update", formCommand, cmdCodexUpdate, "update"},
		{"codex update the docs", formText, "", ""},
		{"helper update now", formCommand, cmdUpdate, "now"},
		{"helper update everything", formText, "", ""},
		{"helper restart now", formCommand, cmdRestartName, "now"},
		{"helper service restart now", formCommand, cmdRestartName, "now"},
		{"helper cancel all", formCommand, cmdCancelName, "all"},
		{"helper cancel this please", formText, "", ""},
		{"/cancel", formCommand, cmdCancelName, ""},
		// Work-only `helper …` commands: Wrong chat.
		{"helper close", formUnknown, "close", ""},
		{"helper file out.txt", formUnknown, "file", "out.txt"},
		{"helper retry last", formUnknown, "retry", "last"},
		{"helper stats", formCommand, cmdStats, ""},
		{"helper stats now", formUnknown, "stats", "now"},
		// Unknown and removed names.
		{"!frobnicate", formUnknown, "frobnicate", ""},
		{"cx frobnicate", formUnknown, "frobnicate", ""},
		{"helper frobnicate", formText, "", ""},
		{"reload", formUnknown, "reload", ""},
		{"restore-thread x", formUnknown, "restore-thread", "x"},
		{"how do I start?", formText, "", ""},
	} {
		p := parseControl(c.text)
		if p.form != c.form || p.name != c.name || p.arg != c.arg {
			t.Errorf("parseControl(%q) = %+v, want form %d name %q arg %q", c.text, p, c.form, c.name, c.arg)
		}
	}
}

func TestStripCodexMention(t *testing.T) {
	for _, c := range []struct {
		in, out   string
		mentioned bool
	}{
		{"@codex fix it", "fix it", true},
		{"hey @Codex  !status", "hey !status", true},
		{"@codex", "", true},
		{"mail me at a@codex.io", "mail me at a@codex.io", false},
		{"no mention", "no mention", false},
	} {
		out, m := stripCodexMention(c.in)
		if out != c.out || m != c.mentioned {
			t.Errorf("stripCodexMention(%q) = %q, %v; want %q, %v", c.in, out, m, c.out, c.mentioned)
		}
	}
}
