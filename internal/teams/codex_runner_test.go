package teams

import (
	"strings"
	"testing"
)

func TestParseCodexJSONLExtractsThreadAndFinalAgentMessage(t *testing.T) {
	output := strings.Join([]string{
		"Reading additional input from stdin...",
		`{"type":"thread.started","thread_id":"019ddc51-618d-75c1-b508-8150cd20fb96"}`,
		`{"type":"turn.started"}`,
		`{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"first"}}`,
		`{"type":"item.completed","item":{"id":"item_1","type":"agent_message","content":[{"type":"output_text","text":"final"}]}}`,
		`{"type":"turn.completed"}`,
	}, "\n")

	got := parseCodexJSONL(output)
	if got.CodexThreadID != "019ddc51-618d-75c1-b508-8150cd20fb96" {
		t.Fatalf("unexpected thread id %q", got.CodexThreadID)
	}
	if got.Text != "final" {
		t.Fatalf("unexpected final text %q", got.Text)
	}
	if got.CodexTurnID != "" {
		t.Fatalf("unexpected turn id %q for official exec JSON", got.CodexTurnID)
	}
}

func TestSplitTextChunks(t *testing.T) {
	got := splitTextChunks("one two three four", 8)
	if len(got) != 3 {
		t.Fatalf("expected 3 chunks, got %#v", got)
	}
	if strings.Join(got, " ") != "one two three four" {
		t.Fatalf("unexpected chunks %#v", got)
	}
}

func TestSplitTextChunksForHTMLMessageUsesRenderedHTMLBytes(t *testing.T) {
	text := strings.Repeat("<&>", 200)
	chunks := splitTextChunksForHTMLMessage("Codex", text, 512)
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %#v", chunks)
	}
	for _, chunk := range chunks {
		if got := len(HTMLMessage("Codex", chunk)); got > 512 {
			t.Fatalf("chunk rendered to %d HTML bytes, want <= 512", got)
		}
	}
	if strings.Join(chunks, "") != text {
		t.Fatalf("chunks did not preserve text")
	}
}

func TestTeamsChunkLimitLeavesRoomForPartLabels(t *testing.T) {
	text := strings.Repeat("&", 30000)
	chunks := splitTextChunksForHTMLMessage("Codex", text, teamsChunkHTMLContentBytes)
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}
	for i, chunk := range chunks {
		body := chunk
		if len(chunks) > 1 {
			body = "part label headroom\n" + body
		}
		if got := len(HTMLMessage("Codex", body)); got > safeTeamsHTMLContentBytes {
			t.Fatalf("chunk %d rendered to %d HTML bytes, want <= %d", i, got, safeTeamsHTMLContentBytes)
		}
	}
}
