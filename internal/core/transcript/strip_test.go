package transcript

import (
	"strings"
	"testing"
)

// Ported from v0.1.20 internal/teams/artifact_handoff_test.go (F-MIR-8).

func TestStripMemCitationsRemovesInlineClosedBlock(t *testing.T) {
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
	if got := StripMemCitations(text); got != "visible answer" {
		t.Fatalf("StripMemCitations() = %q, want visible answer", got)
	}
	literal := "visible <oai-mem-citation> literal text without closing tag"
	if got := StripMemCitations(literal); got != literal {
		t.Fatalf("StripMemCitations() = %q, want literal text preserved", got)
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
	if got := StripMemCitations(withLiteralMention); got != "visible `<oai-mem-citation>` mention before metadata." {
		t.Fatalf("StripMemCitations() = %q, want inline mention preserved", got)
	}
}

func TestStripMemCitationsPreservesFencedExamples(t *testing.T) {
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
	got := StripMemCitations(fenced)
	for _, want := range []string{
		"visible before",
		"```xml",
		"MEMORY.md:1-3|note=[example that must remain visible]",
		"</oai-mem-citation>",
		"```",
		"visible after",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("StripMemCitations() missing fenced content %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "hidden metadata") || strings.Contains(got, "MEMORY.md:4-5") {
		t.Fatalf("StripMemCitations() leaked hidden metadata:\n%s", got)
	}
}

func TestStripMemCitationsDoesNotTreatCitationStartAsFenceInfo(t *testing.T) {
	text := strings.Join([]string{
		"visible before",
		"~~~<oai-mem-citation>",
		"<citation_entries>",
		"MEMORY.md:1-3|note=[hidden metadata]",
		"</citation_entries>",
		"</oai-mem-citation>",
		"visible after",
	}, "\n")
	got := StripMemCitations(text)
	if !strings.Contains(got, "visible before") || !strings.Contains(got, "visible after") {
		t.Fatalf("StripMemCitations() lost visible text:\n%s", got)
	}
	if strings.Contains(got, "hidden metadata") || strings.Contains(got, "MEMORY.md") || strings.Contains(got, "citation_entries") {
		t.Fatalf("StripMemCitations() leaked hidden metadata:\n%s", got)
	}
}

func TestStripMemCitationsCleansAfterUnclosedFence(t *testing.T) {
	text := strings.Join([]string{
		"visible before",
		"~~~",
		"<oai-mem-citation>",
		"<citation_entries>",
		"MEMORY.md:1-3|note=[hidden metadata]",
		"</citation_entries>",
		"</oai-mem-citation>",
	}, "\n")
	got := StripMemCitations(text)
	if !strings.Contains(got, "visible before") {
		t.Fatalf("StripMemCitations() lost visible text:\n%s", got)
	}
	if strings.Contains(got, "hidden metadata") || strings.Contains(got, "MEMORY.md") || strings.Contains(got, "citation_entries") {
		t.Fatalf("StripMemCitations() leaked hidden metadata:\n%s", got)
	}
}

func TestStripMemCitationsLeavesOtherTextAlone(t *testing.T) {
	for _, text := range []string{"", "  plain answer\n", "no tag here\n\n```go\nx\n```"} {
		if got := StripMemCitations(text); got != text {
			t.Fatalf("StripMemCitations(%q) = %q", text, got)
		}
	}
}

func TestStripHelperPrompt(t *testing.T) {
	for in, want := range map[string]string{
		"User message:\nfix the bug\n\nTeams helper safety:\n- You are running inside a Codex turn launched by the Teams helper.":              "fix the bug",
		"Teams helper safety:\n- You are running inside a Codex turn launched by the Teams helper.":                                            "",
		"User message:\nlook\n\nIf you need to return generated files or images to the Teams user, write them under this local directory:\n/x": "look",
		"a local prompt": "a local prompt",
	} {
		if got := StripHelperPrompt(in); got != want {
			t.Fatalf("StripHelperPrompt(%q) = %q, want %q", in, got, want)
		}
	}
}
