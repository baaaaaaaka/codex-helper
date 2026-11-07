package engine

import (
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
)

// `helper stats` has the layout of 285784d3^ (owner request 2026-11-07):
// the total, the main agent and the subagents as scoped sections, each a
// table or a list.
func TestStatsLayoutAsBeforeTheRebuild(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"turn_context","payload":{"turn_id":"turn-1","model":"model-a","effort":"high"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":100,"cached_input_tokens":80,"output_tokens":10,"total_tokens":110},"last_token_usage":{"input_tokens":100,"cached_input_tokens":80,"output_tokens":10,"total_tokens":110}}}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","message":"done"}`,
	}, "\n")
	got, err := transcript.ParseTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	c := chatUsage{label: "s001", linked: true, main: got}
	lines := []string{statsTitle, "", "Session: s001", ""}
	lines = append(lines, totalLines(c)...)
	lines = append(lines, "")
	lines = append(lines, mainAgentLines(c.main, []string{"one rollout | unreadable"})...)
	lines = append(lines, "")
	lines = append(lines, subagentLines(c)...)
	out := renderStats(strings.Join(lines, "\n"))

	for _, want := range []string{
		"**" + statsTitle + "**\n\n**Session:** s001\n\n**🧮 TOTAL (MAIN + SUBAGENTS) · overall:**\n\n| Metric | Overall |\n| --- | --- |\n| **input** | 100 (cached 80, non-cached 20) |\n| **Cache hit rate** | 80.0% |\n| **output** | 10 |\n| **total** | 110 |",
		"**🧮 TOTAL (MAIN + SUBAGENTS) · model/effort detail:**\n\n| Model | Effort | input | Cache hit rate | output | total |\n| --- | --- | --- | --- | --- | --- |\n| model-a | overall | 100 (cached 80, non-cached 20) | 80.0% | 10 | 110 |\n| model-a | high | 100 (cached 80, non-cached 20) | 80.0% | 10 | 110 |",
		"**🧠 MAIN AGENT · metadata:**\n\n**Source:**",
		"**🧠 MAIN AGENT · snapshots:**\n\n| Metric | Last recorded model usage | Conversation total |\n| --- | --- | --- |\n| **input** | 100 (cached 80, non-cached 20) | 100 (cached 80, non-cached 20) |",
		"**🧠 MAIN AGENT · analysis:**\n\n- **",
		"**🧠 MAIN AGENT · diagnostics:**\n\n- **Diagnostics:** skipped",
		"**🧠 MAIN AGENT · source diagnostics:**\n\n- one rollout | unreadable",
		"**🧩 SUBAGENTS · NOT USED:**\n\n| Status | Value |\n| --- | --- |\n| **Status** | no user-visible subagent transcript was found |\n| **Transcript count** | 0 |\n| **Token usage** | 0 |\n| **Cache hit rate** | N/A |\n| **Model/effort breakdown** | not applicable |",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("stats missing\n%s\n--- in ---\n%s", want, out)
		}
	}

	// A chat with subagents, and a main agent without usage.
	c = chatUsage{label: "s002", linked: true, subs: 2, subTotal: transcript.TokenUsage{Input: 5, Output: 1, Total: 6}}
	out = renderStats(strings.Join(append(append(mainAgentLines(c.main, nil), ""), subagentLines(c)...), "\n"))
	for _, want := range []string{
		"**🧠 MAIN AGENT · UNAVAILABLE:**\n\n| Status | Value |\n| --- | --- |\n| **Status** | no Codex usage event was found |",
		"**🧩 SUBAGENTS (2) · overall:**\n\n| Metric | Overall |\n| --- | --- |\n| **input** | 5 |\n| **Cache hit rate** | N/A |",
		"**🧩 SUBAGENTS (2) · model/effort detail:**\n\n**Model/effort breakdown:** unavailable",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("stats missing\n%s\n--- in ---\n%s", want, out)
		}
	}
}

// The control chat's `stats` lists each chat's usage as a row.
func TestAllStatsPerChatTable(t *testing.T) {
	text := strings.Join([]string{statsTitle, "", "Scope: all Work chats", "", allChatsScope + "per chat:", "Chat: s002"}, "\n") + "\n" +
		strings.Join(usageLines(transcript.TokenUsage{Input: 10, Output: 2, Total: 12}), "\n") + "\nChat: s001\n" +
		strings.Join(usageLines(transcript.TokenUsage{Input: 20, Output: 3, Total: 23}), "\n")
	want := "| Chat | input | Cache hit rate | output | total |\n| --- | --- | --- | --- | --- |\n| s002 | 10 | N/A | 2 | 12 |\n| s001 | 20 | N/A | 3 | 23 |"
	if out := renderStats(text); !strings.Contains(out, want) {
		t.Fatalf("stats missing\n%s\n--- in ---\n%s", want, out)
	}
}
