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
		"Model/profile quick help:",
		"cxp run --yolo -- codex",
		"cxp run --model-profile <name> -- codex",
		"cxp model list",
		"cxp model-profile setup",
		"cxp responses serve",
		"new <dir> --model <profile>",
		"model status|switch|fork",
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
		"The current Teams request is the `User message` section above.",
		"Treat earlier Teams requests in resumed Codex history as completed context",
		"Do not restart, reload, kill, replace, or background the Teams helper",
		"`helper restart now`",
		"`helper update now`",
		"`helper reload now` only for source-checkout development reloads",
		artifactHandoffInstructionLead,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("TeamsCodexPrompt missing %q:\n%s", want, got)
		}
	}
}

func TestStripOAIMemoryCitationBlocksRemovesInlineClosedBlock(t *testing.T) {
	text := strings.Join([]string{
		"visible answer<oai-mem-citation>",
		"<citation_entries>",
		"MEMORY.md:1-3|note=[confirmed codex-helper repo context]",
		"</citation_entries>",
		"<rollout_ids>",
		"019d4393-5109-7b10-b5c2-05b2fe8635ba",
		"</rollout_ids>",
		"</oai-mem-citation>",
	}, "\n")
	if got := StripOAIMemoryCitationBlocks(text); got != "visible answer" {
		t.Fatalf("StripOAIMemoryCitationBlocks() = %q, want visible answer", got)
	}
	literal := "visible <oai-mem-citation> literal text without closing tag"
	if got := StripOAIMemoryCitationBlocks(literal); got != literal {
		t.Fatalf("StripOAIMemoryCitationBlocks() = %q, want literal text preserved", got)
	}
	withLiteralMention := strings.Join([]string{
		"visible `<oai-mem-citation>` mention before metadata.",
		"",
		"<oai-mem-citation>",
		"<citation_entries>",
		"MEMORY.md:1-3|note=[confirmed codex-helper repo context]",
		"</citation_entries>",
		"</oai-mem-citation>",
	}, "\n")
	if got := StripOAIMemoryCitationBlocks(withLiteralMention); got != "visible `<oai-mem-citation>` mention before metadata." {
		t.Fatalf("StripOAIMemoryCitationBlocks() = %q, want inline mention preserved", got)
	}
}

func TestStripOAIMemoryCitationBlocksPreservesFencedExamples(t *testing.T) {
	fenced := strings.Join([]string{
		"visible before",
		"```xml",
		"<oai-mem-citation>",
		"<citation_entries>",
		"MEMORY.md:1-3|note=[example that must remain visible]",
		"</citation_entries>",
		"</oai-mem-citation>",
		"```",
		"visible after",
		"<oai-mem-citation>",
		"<citation_entries>",
		"MEMORY.md:4-5|note=[hidden metadata]",
		"</citation_entries>",
		"</oai-mem-citation>",
	}, "\n")
	got := StripOAIMemoryCitationBlocks(fenced)
	for _, want := range []string{
		"visible before",
		"```xml",
		"MEMORY.md:1-3|note=[example that must remain visible]",
		"</oai-mem-citation>",
		"```",
		"visible after",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("StripOAIMemoryCitationBlocks() missing fenced content %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "hidden metadata") || strings.Contains(got, "MEMORY.md:4-5") {
		t.Fatalf("StripOAIMemoryCitationBlocks() leaked hidden metadata:\n%s", got)
	}
}

func TestStripOAIMemoryCitationBlocksDoesNotTreatCitationStartAsFenceInfo(t *testing.T) {
	text := strings.Join([]string{
		"visible before",
		"~~~<oai-mem-citation>",
		"<citation_entries>",
		"MEMORY.md:1-3|note=[hidden metadata]",
		"</citation_entries>",
		"</oai-mem-citation>",
		"visible after",
	}, "\n")
	got := StripOAIMemoryCitationBlocks(text)
	if !strings.Contains(got, "visible before") || !strings.Contains(got, "visible after") {
		t.Fatalf("StripOAIMemoryCitationBlocks() lost visible text:\n%s", got)
	}
	if strings.Contains(got, "hidden metadata") || strings.Contains(got, "MEMORY.md") || strings.Contains(got, "citation_entries") {
		t.Fatalf("StripOAIMemoryCitationBlocks() leaked hidden metadata:\n%s", got)
	}
}

func TestStripOAIMemoryCitationBlocksCleansAfterUnclosedFence(t *testing.T) {
	text := strings.Join([]string{
		"visible before",
		"~~~",
		"<oai-mem-citation>",
		"<citation_entries>",
		"MEMORY.md:1-3|note=[hidden metadata]",
		"</citation_entries>",
		"</oai-mem-citation>",
	}, "\n")
	got := StripOAIMemoryCitationBlocks(text)
	if !strings.Contains(got, "visible before") {
		t.Fatalf("StripOAIMemoryCitationBlocks() lost visible text:\n%s", got)
	}
	if strings.Contains(got, "hidden metadata") || strings.Contains(got, "MEMORY.md") || strings.Contains(got, "citation_entries") {
		t.Fatalf("StripOAIMemoryCitationBlocks() leaked hidden metadata:\n%s", got)
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
