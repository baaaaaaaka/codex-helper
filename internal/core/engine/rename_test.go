package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTitles(t *testing.T) {
	cases := []struct{ got, want string }{
		{controlTitleFor("box", ""), "🏠 Codex Control - box"},
		{controlTitleFor("", "Ops: desk"), "🏠 Codex Control - machine - Ops- desk"},
		{workTitle("box", "", "Fix /home/me/src/app/main.go now", "/w"), "💬 box - Fix main.go now"},
		{workTitle("box", "Mine", "ignored", "/w"), "💬 box - Mine"},
		{workTitle("box", "", "", "/work/proj/"), "💬 box - proj"},
		{workTitle("box", "", "", ""), "💬 box - untitled"},
		{sanitizeTitle("  a\tb\x00c | d?  "), "abc - d"},
		{sanitizeTitle(strings.Repeat("x", 70)), strings.Repeat("x", 61) + "..."},
		{shortLine("abcdef", 4), "abc…"},
		{stripMachinePrefix("💬 old - Fix it", []string{"new", "old"}), "Fix it"},
		{stripMachinePrefix("💬 old", []string{"old"}), ""},
		{stripMachinePrefix("s001 · dir", []string{"old"}), "s001 · dir"},
	}
	for i, c := range cases {
		if c.got != c.want {
			t.Errorf("%d: %q, want %q", i, c.got, c.want)
		}
	}
	if !autoAllowed("auto", "x", "") || autoAllowed("user", "x", "") || autoAllowed("auto", "x", "mine") ||
		!autoAllowed("", "New message in w", "") || autoAllowed("", "My chat", "") {
		t.Error("autoAllowed")
	}
}

func TestPromptTitle(t *testing.T) {
	cases := map[string]string{
		teamsCodexPrompt("Fix it", "/out"): "Fix it",
		teamsCodexPrompt(withTranscripts("", []promptClip{{name: "v.ogg", text: "list files"}}), "/out"): "list files",
		"Fix it\n\nHelper safety:\n- x":                       "Fix it",
		"Read notes\n\nAttached files (saved locally):\n- /a": "Read notes",
		"Teams helper safety:\n- only the suffix":             "",
		"  plain  ": "plain",
	}
	for in, want := range cases {
		if got := promptTitle(in); got != want {
			t.Errorf("promptTitle(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestThreadName(t *testing.T) {
	p := filepath.Join(t.TempDir(), "session_index.jsonl")
	data := `{"id":"t1","thread_name":"First"}
not json
{"id":"t2","thread_name":"Other"}
{"id":"t1","thread_name":"Renamed"}
{"id":"t1","thread_name":"  "}
`
	if err := os.WriteFile(p, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := threadName(p, "t1"); got != "Renamed" {
		t.Fatalf("t1: %q", got)
	}
	if got := threadName(p, "t3"); got != "" {
		t.Fatalf("t3: %q", got)
	}
	if got := threadName(filepath.Join(t.TempDir(), "none"), "t1"); got != "" {
		t.Fatalf("missing file: %q", got)
	}
}
