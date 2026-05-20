package teams

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
)

func TestControlFallbackPromptUsesCodexhistoryMarker(t *testing.T) {
	got := ControlFallbackCodexPrompt("what can I do here")
	if !strings.HasPrefix(got, codexhistory.HelperControlSessionTitleKeyword+"\n") {
		t.Fatalf("ControlFallbackCodexPrompt prefix = %q, want codexhistory marker", got)
	}
}

func TestControlFallbackPromptWithContextIncludesHelpAndRedactsSecrets(t *testing.T) {
	got := ControlFallbackCodexPromptWithContext("helper upgrade 怎么用", ControlFallbackPromptContext{
		HelperVersion:    "v-test",
		ControlChatTitle: "Codex Control",
		ControlChatID:    "control-chat",
		ActiveWorkChats: []string{
			"`s001` - repo - cwd=`/home/baka/project/repo` - chat=`chat-1`",
		},
		CurrentDashboard: "- view: `workspaces`\n- visible_items:\n  - `1` workspace `repo`",
		HelperHelpContext: strings.Join([]string{
			"cxp teams service bootstrap",
			"Webhook URL: https://workflow.example.test/hook?sig=super-secret",
			"Authorization: Bearer token-secret",
			"https://workflow.example.test/hook?sig=bare-secret",
		}, "\n"),
	})
	for _, want := range []string{
		`<codex-helper-control-context version="1">`,
		"helper_version: `v-test`",
		"control_chat_title: `Codex Control`",
		"active_work_chats:",
		"last_control_dashboard:",
		"Relevant cxp / Teams helper help digest:",
		"cxp teams service bootstrap",
		"User message:\nhelper upgrade 怎么用",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("ControlFallbackCodexPromptWithContext missing %q:\n%s", want, got)
		}
	}
	for _, forbidden := range []string{"super-secret", "token-secret", "bare-secret"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("ControlFallbackCodexPromptWithContext leaked %q:\n%s", forbidden, got)
		}
	}
}

func TestControlFallbackPromptDefaultHelpHasCommandDigest(t *testing.T) {
	got := ControlFallbackCodexPrompt("help")
	for _, want := range []string{
		"Relevant cxp / Teams helper help digest:",
		"Control chat quick help:",
		"Beacon CLI quick help:",
		"cxp beacon profile create",
		"cxp beacon profile confirm",
		"--after-current-turn",
		"Beacon execution profiles are separate from SSH proxy profiles",
		"Work chat quick help:",
		"helper skills add",
		"helper skills sync",
		"helper cancel last",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("default control fallback prompt missing %q:\n%s", want, got)
		}
	}
}

func TestControlFallbackPromptTruncatesUTF8ContextSafely(t *testing.T) {
	got := ControlFallbackCodexPromptWithContext("help", ControlFallbackPromptContext{
		HelperHelpContext: strings.Repeat("说明\n", maxControlFallbackHelpContextChars),
	})
	if !utf8.ValidString(got) {
		t.Fatalf("ControlFallbackCodexPromptWithContext produced invalid UTF-8")
	}
	if !strings.Contains(got, "truncated for prompt size") {
		t.Fatalf("ControlFallbackCodexPromptWithContext did not truncate large help context")
	}
}

func TestTeamsCodexPromptIncludesSelfManagementGuard(t *testing.T) {
	got := TeamsCodexPrompt("fix the helper")
	if !strings.HasPrefix(got, "User message:\nfix the helper\n\nTeams helper safety:") {
		t.Fatalf("TeamsCodexPrompt did not prefix the user message:\n%s", got)
	}
	for _, want := range []string{
		"Teams helper safety:",
		"Do not restart, reload, kill, replace, or background the Teams helper",
		"`helper reload now`",
		"`helper restart now`",
		artifactHandoffInstructionLead,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("TeamsCodexPrompt missing %q:\n%s", want, got)
		}
	}
}

func TestStripHelperPromptEchoesRemovesGeneratedControlFallbackContext(t *testing.T) {
	prompt := ControlFallbackCodexPromptWithContext("what can I do here", ControlFallbackPromptContext{
		HelperVersion:     "v-test",
		HelperHelpContext: "cxp teams service bootstrap",
	})
	got := StripHelperPromptEchoes("echo: " + prompt)
	if got != "echo: what can I do here" {
		t.Fatalf("StripHelperPromptEchoes() = %q, want generated control context removed", got)
	}
}

func TestStripHelperPromptEchoesRemovesHelperHistoryMarkers(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "marker only",
			in:   "[codex-helper-control]\nUse projects",
			want: "Use projects",
		},
		{
			name: "uppercase marker",
			in:   "[CODEX-HELPER-CONTROL]\nUse projects",
			want: "Use projects",
		},
		{
			name: "partial hidden instruction",
			in:   "[codex-helper-control]\nControl chat commands the helper understands:\n- projects\nUser message:\nhello",
			want: "hello",
		},
		{
			name: "lowercase user message delimiter",
			in:   "[codex-helper-control]\nyou are handling an unrecognized message from the user's microsoft teams control chat for codex-helper.\nuser message:\nhello",
			want: "hello",
		},
		{
			name: "structured control context echo with prefix",
			in:   "echo: [codex-helper-control]\n<codex-helper-control-context version=\"1\">\nYou are handling an unrecognized message from the user's Microsoft Teams control chat for codex-helper.\nControl chat commands the helper understands:\n- projects\n</codex-helper-control-context>\n\nUser message:\nhello",
			want: "echo: hello",
		},
		{
			name: "teams helper safety echo",
			in:   "use the control chat\nTeams helper safety:\n- You are running inside a Codex turn launched by the Teams helper.",
			want: "use the control chat",
		},
		{
			name: "teams helper safety echo with user message envelope",
			in:   "User message:\nuse the control chat\n\nTeams helper safety:\n- You are running inside a Codex turn launched by the Teams helper.",
			want: "use the control chat",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StripHelperPromptEchoes(tt.in); got != tt.want {
				t.Fatalf("StripHelperPromptEchoes() = %q, want %q", got, tt.want)
			}
		})
	}
}
