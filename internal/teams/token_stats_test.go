package teams

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestParseCodexTokenStatsPrefersTokenCountAndReportsDiagnostics(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-1"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":1370490,"cached_input_tokens":1197952,"output_tokens":4893,"reasoning_output_tokens":1127,"total_tokens":1375383},"last_token_usage":{"input_tokens":168707,"cached_input_tokens":154496,"output_tokens":674,"reasoning_output_tokens":231,"total_tokens":169381},"model_context_window":258400},"rate_limits":{"limit_id":"codex","limit_name":"Codex","plan_type":"business","primary":{"used_percent":42.5,"window_minutes":10,"resets_at":1704069000},"credits":{"has_credits":true,"unlimited":false,"balance":"42"}}}}`,
		`{"type":"turn.completed","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","message":"done"}`,
	}, "\n")

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Source != "token_count" || got.SourceLine != 2 || got.UsedFallbackOnly {
		t.Fatalf("source = %#v", got)
	}
	if got.Info.Last.InputTokens != 168707 || got.Info.Last.CachedInputTokens != 154496 || got.Info.Last.ReasoningOutputTokens != 231 || got.Info.Last.TotalTokens != 169381 {
		t.Fatalf("last usage = %#v", got.Info.Last)
	}
	if got.Info.Total.InputTokens != 1370490 || got.Info.Total.CachedInputTokens != 1197952 || got.Info.Total.ReasoningOutputTokens != 1127 || got.Info.Total.TotalTokens != 1375383 {
		t.Fatalf("total usage = %#v", got.Info.Total)
	}
	if got.Info.ModelContextWindow != 258400 {
		t.Fatalf("context window = %d", got.Info.ModelContextWindow)
	}
	if !got.RateLimits.Present || got.RateLimits.LimitID != "codex" || got.RateLimits.PlanType != "business" || len(got.RateLimits.Windows) != 1 {
		t.Fatalf("rate limits = %#v", got.RateLimits)
	}
	if got.RateLimits.LimitName != "Codex" || !got.RateLimits.Credits.Present || !got.RateLimits.Credits.HasCredits || got.RateLimits.Credits.Balance != "42" {
		t.Fatalf("rate limit details = %#v", got.RateLimits)
	}
	if len(got.Diagnostics) != 1 || got.Diagnostics[0].Kind != "invalid_json" {
		t.Fatalf("diagnostics = %#v", got.Diagnostics)
	}

	rendered := strings.Join(formatCodexTokenStatsLines(got), "\n")
	for _, want := range []string{
		"Last recorded model usage:",
		"Conversation total:",
		"Cache hit rate: 91.6%",
		"model context window: 258,400",
		"current context uses",
		"Codex baseline-adjusted context remaining:",
		"Rate limits:",
		"limit name: Codex",
		"reset 2024-01-01T00:30:00Z",
		"Diagnostics: skipped",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("formatted stats missing %q:\n%s", want, rendered)
		}
	}
}

func TestParseCodexTokenStatsMergesRateOnlyTokenCount(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":100,"cached_input_tokens":20,"output_tokens":10,"reasoning_output_tokens":2,"total_tokens":110},"last_token_usage":{"input_tokens":100,"cached_input_tokens":20,"output_tokens":10,"reasoning_output_tokens":2,"total_tokens":110},"model_context_window":1000},"rate_limits":null}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":null,"rate_limits":{"limit_id":"codex","primary":{"used_percent":80,"window_minutes":300,"resets_at":1704074400}}}}`,
	}, "\n")

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.SourceLine != 1 || got.Info.Last.TotalTokens != 110 {
		t.Fatalf("merged stats = %#v", got)
	}
	if !got.RateLimits.Present || len(got.RateLimits.Windows) != 1 || got.RateLimits.Windows[0].UsedPercent != 80 {
		t.Fatalf("merged rate limits = %#v", got.RateLimits)
	}
}

func TestFormatCodexTokenStatsHidesMetadataOnlyRateLimits(t *testing.T) {
	input := `{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":100,"cached_input_tokens":80,"output_tokens":10,"total_tokens":110},"last_token_usage":{"input_tokens":100,"cached_input_tokens":80,"output_tokens":10,"total_tokens":110}},"rate_limits":{"limit_id":"codex","plan_type":"business","credits":{"has_credits":true}}}}`

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if !got.RateLimits.Present || !got.RateLimits.Credits.Present {
		t.Fatalf("rate limits = %#v", got.RateLimits)
	}
	rendered := strings.Join(formatCodexTokenStatsLines(got), "\n")
	if strings.Contains(rendered, "Rate limits:") || strings.Contains(rendered, "credits: available") {
		t.Fatalf("metadata-only rate limits should be hidden:\n%s", rendered)
	}
}

func TestParseCodexTokenStatsAllowsLargeTranscriptLines(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":100,"cached_input_tokens":80,"output_tokens":10,"total_tokens":110},"last_token_usage":{"input_tokens":100,"cached_input_tokens":80,"output_tokens":10,"total_tokens":110}}}}`,
		strings.Repeat("x", 17<<20),
	}, "\n")

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Last.TotalTokens != 110 || got.SourceLine != 1 {
		t.Fatalf("stats = %#v", got)
	}
}

func TestParseCodexTokenStatsFallsBackToUsageFields(t *testing.T) {
	input := `{"type":"turn.completed","usage":{"input_tokens":100,"output_tokens":12,"reasoning_output_tokens":5,"total_tokens":112,"input_tokens_details":{"cached_tokens":34}}}`

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if !got.UsedFallbackOnly || got.Source != "event usage" {
		t.Fatalf("source = %#v", got)
	}
	if got.Info.Last.InputTokens != 100 || got.Info.Last.CachedInputTokens != 34 || got.Info.Last.ReasoningOutputTokens != 5 || got.Info.Last.TotalTokens != 112 {
		t.Fatalf("fallback usage = %#v", got.Info.Last)
	}
	rendered := strings.Join(formatCodexTokenStatsLines(got), "\n")
	if !strings.Contains(rendered, "runner usage fallback") {
		t.Fatalf("fallback formatted stats = %q", rendered)
	}
}

func TestRenderCodexTokenStatsHTMLEscapesMetadataAndTableValues(t *testing.T) {
	rendered := renderCodexTokenStatsHTML(strings.Join([]string{
		"STATS: Codex tokens",
		"Session: s<001>",
		"Codex thread: thread&1",
		"Source: token_count at transcript line 2 (/tmp/<session>&.jsonl)",
		"Reliability: using Codex `token_count` event from local history.",
		"",
		"Last recorded model usage:",
		"",
		"input: 100 < 200 (cached 80, non-cached 20)",
		"Cache hit rate: 80.0%",
		"output: 12 (reasoning 5)",
		"total: 112",
		"",
		"Conversation total:",
		"",
		"input: 300 (cached 120, non-cached 180)",
		"Cache hit rate: 40.0%",
		"output: 30 (reasoning 10)",
		"total: 330",
	}, "\n"))

	for _, want := range []string{
		"<strong>Session:</strong> s&lt;001&gt;",
		"<strong>Codex thread:</strong> thread&amp;1",
		"(/tmp/&lt;session&gt;&amp;.jsonl)",
		"<table><tr><th>Metric</th><th>Last recorded model usage</th><th>Conversation total</th></tr>",
		"<tr><td><strong>input</strong></td><td>100 &lt; 200 (cached 80, non-cached 20)</td><td>300 (cached 120, non-cached 180)</td></tr>",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered stats HTML missing %q:\n%s", want, rendered)
		}
	}
	for _, forbidden := range []string{"s<001>", "thread&1", "/tmp/<session>&.jsonl", "100 < 200"} {
		if strings.Contains(rendered, forbidden) {
			t.Fatalf("rendered stats HTML did not escape %q:\n%s", forbidden, rendered)
		}
	}
}

func TestBridgeWorkHelperStatsReadsLinkedTranscript(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	transcript := `{"type":"session_meta","payload":{"id":"thread-1"}}` + "\n" +
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":300,"cached_input_tokens":120,"output_tokens":30,"reasoning_output_tokens":10,"total_tokens":330},"last_token_usage":{"input_tokens":200,"cached_input_tokens":100,"output_tokens":20,"reasoning_output_tokens":5,"total_tokens":220},"model_context_window":1000},"rate_limits":{"limit_id":"codex","plan_type":"business"}}}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(transcript), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}

	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{result: ExecutionResult{Text: "should not run"}})
	session := bridge.reg.SessionByChatID("chat-1")
	session.CodexThreadID = "thread-1"
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		state.ImportCheckpoints[transcriptCheckpointID(session.ID)] = teamstore.ImportCheckpoint{
			ID:         transcriptCheckpointID(session.ID),
			SessionID:  session.ID,
			SourcePath: transcriptPath,
			Status:     importCheckpointStatusComplete,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessageWithText("helper-stats", "helper stats"), "helper stats"); err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if len(*sent) != 1 {
		t.Fatalf("sent = %d, want 1", len(*sent))
	}
	got := PlainTextFromTeamsHTML((*sent)[0].Content)
	for _, want := range []string{"STATS: Codex tokens", "Codex thread: thread-1", "Model usage:", "Last recorded model usage", "Conversation total", "input", "Cache hit rate", "total"} {
		if !strings.Contains(got, want) {
			t.Fatalf("helper stats response missing %q:\n%s", want, got)
		}
	}
	html := (*sent)[0].Content
	for _, want := range []string{
		"<p><strong>🔧 Helper:</strong></p>",
		"<p><strong>STATS: Codex tokens</strong></p>",
		"<strong>Session:</strong> s001<br><strong>Codex thread:</strong> thread-1",
		"<p><strong>Model usage:</strong></p>",
		"<table><tr><th>Metric</th><th>Last recorded model usage</th><th>Conversation total</th></tr>",
		"<tr><td><strong>input</strong></td><td>200 (cached 100, non-cached 100)</td><td>300 (cached 120, non-cached 180)</td></tr>",
		"<tr><td><strong>Cache hit rate</strong></td><td>50.0%</td><td>40.0%</td></tr>",
		"<tr><td><strong>output</strong></td><td>20 (reasoning 5)</td><td>30 (reasoning 10)</td></tr>",
		"<tr><td><strong>total</strong></td><td>220</td><td>330</td></tr>",
		"<p><strong>Analysis:</strong></p>",
		"<ul><li><strong>model context window:</strong> 1,000; current context uses 22.0%; approx remaining: 780</li>",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("helper stats HTML missing paragraph %q:\n%s", want, html)
		}
	}
	for _, forbidden := range []string{
		"STATS: Codex tokens<br>Session:",
		"Session: s001<br>Codex thread:",
		"Codex thread: thread-1<br>Source:",
		"Last recorded model usage:<br>input:",
		"<p><strong>Last recorded model usage:</strong></p>",
		"<p><strong>Conversation total:</strong></p>",
		"input: 200 (cached 100, non-cached 100)<br>Cache hit rate:",
		"<p>&nbsp;</p><p><strong>Session:",
		"<strong>🤖 ✅ Codex answer:",
	} {
		if strings.Contains(html, forbidden) {
			t.Fatalf("helper stats HTML flattened paragraph %q:\n%s", forbidden, html)
		}
	}
	for _, forbidden := range []string{"Rate limits:", "Cost:"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("helper stats response unexpectedly included %q:\n%s", forbidden, got)
		}
	}
}
