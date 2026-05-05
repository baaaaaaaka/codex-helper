package teams

import (
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
)

func TestControlFallbackPromptUsesCodexhistoryMarker(t *testing.T) {
	got := ControlFallbackCodexPrompt("what can I do here")
	if !strings.HasPrefix(got, codexhistory.HelperControlSessionTitleKeyword+"\n") {
		t.Fatalf("ControlFallbackCodexPrompt prefix = %q, want codexhistory marker", got)
	}
}

func TestTeamsCodexPromptIncludesSelfManagementGuard(t *testing.T) {
	got := TeamsCodexPrompt("fix the helper")
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
			name: "teams helper safety echo",
			in:   "use the control chat\nTeams helper safety:\n- You are running inside a Codex turn launched by the Teams helper.",
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
