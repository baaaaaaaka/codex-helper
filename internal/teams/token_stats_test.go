package teams

import (
	"context"
	"fmt"
	"math"
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
	if got.NativeLatestTotal != got.Info.Total || got.UsageEventCount != 1 || got.NonAdvancingUsageEvents != 0 || got.NativeCounterResets != 0 {
		t.Fatalf("aggregation metadata = %#v", got)
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

func TestParseCodexTokenStatsReconstructsMonotonicCumulativeUsage(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":90,"cached_input_tokens":60,"output_tokens":10,"total_tokens":100},"last_token_usage":{"input_tokens":35,"cached_input_tokens":20,"output_tokens":5,"total_tokens":40},"model_context_window":1000}}}`,
		`{"type":"turn_context","payload":{"turn_id":"healthy-next-turn"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":140,"cached_input_tokens":90,"output_tokens":20,"total_tokens":160},"last_token_usage":{"input_tokens":50,"cached_input_tokens":30,"output_tokens":10,"total_tokens":60},"model_context_window":2000}}}`,
	}, "\n")

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total != (CodexTokenUsage{InputTokens: 140, CachedInputTokens: 90, OutputTokens: 20, TotalTokens: 160}) {
		t.Fatalf("reconstructed total = %#v", got.Info.Total)
	}
	if got.Info.Last.TotalTokens != 60 || got.Info.ModelContextWindow != 2000 {
		t.Fatalf("latest usage info = %#v", got.Info)
	}
	if got.NativeLatestTotal != got.Info.Total || got.UsageEventCount != 2 || got.NonAdvancingUsageEvents != 0 || got.NativeCounterResets != 0 {
		t.Fatalf("aggregation metadata = %#v", got)
	}
}

func TestParseCodexTokenStatsReconstructsAcrossNativeResetsAndNonAdvancingSnapshots(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":90,"cached_input_tokens":60,"output_tokens":10,"total_tokens":100},"last_token_usage":{"input_tokens":90,"cached_input_tokens":60,"output_tokens":10,"total_tokens":100}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":140,"cached_input_tokens":90,"output_tokens":20,"total_tokens":160},"last_token_usage":{"input_tokens":50,"cached_input_tokens":30,"output_tokens":10,"total_tokens":60}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":30,"cached_input_tokens":20,"output_tokens":10,"total_tokens":40},"last_token_usage":{"input_tokens":30,"cached_input_tokens":20,"output_tokens":10,"total_tokens":40}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":30,"cached_input_tokens":20,"output_tokens":10,"total_tokens":40},"last_token_usage":{"input_tokens":30,"cached_input_tokens":20,"output_tokens":10,"total_tokens":40}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":30,"cached_input_tokens":20,"output_tokens":10,"total_tokens":40},"last_token_usage":{"total_tokens":7}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":null,"rate_limits":{"limit_id":"codex","primary":{"used_percent":80,"window_minutes":300}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":70,"cached_input_tokens":40,"output_tokens":20,"total_tokens":90},"last_token_usage":{"input_tokens":40,"cached_input_tokens":20,"output_tokens":10,"total_tokens":50}}}}`,
	}, "\n")

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	want := CodexTokenUsage{InputTokens: 210, CachedInputTokens: 130, OutputTokens: 40, TotalTokens: 250}
	if got.Info.Total != want {
		t.Fatalf("reconstructed total = %#v, want %#v", got.Info.Total, want)
	}
	if got.NativeLatestTotal.TotalTokens != 90 || got.UsageEventCount != 4 || got.NonAdvancingUsageEvents != 2 || got.NativeCounterResets != 1 {
		t.Fatalf("aggregation metadata = %#v", got)
	}
	if !got.RateLimits.Present || len(got.RateLimits.Windows) != 1 || got.RateLimits.Windows[0].UsedPercent != 80 {
		t.Fatalf("rate limits = %#v", got.RateLimits)
	}
	rendered := strings.Join(formatCodexTokenStatsLines(got), "\n")
	for _, wantText := range []string{
		"Conversation total:",
		"total: 250",
		"ignored 2 non-advancing usage snapshot(s); observed 1 native cumulative counter reset(s) and 0 recovery event(s)",
		"native latest cumulative total: 90; reconstructed conversation total: 250",
	} {
		if !strings.Contains(rendered, wantText) {
			t.Fatalf("formatted stats missing %q:\n%s", wantText, rendered)
		}
	}
}

func TestParseCodexTokenStatsDoesNotDoubleCountWhenNativeTotalRecovers(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":100},"last_token_usage":{"total_tokens":100}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":40},"last_token_usage":{"total_tokens":40}}}}`,
		`{"type":"turn_context","payload":{"turn_id":"fixed-codex-turn"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":170},"last_token_usage":{"total_tokens":30}}}}`,
	}, "\n")

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total.TotalTokens != 170 {
		t.Fatalf("reconstructed total = %d, want 170", got.Info.Total.TotalTokens)
	}
	if got.NativeLatestTotal.TotalTokens != 170 || got.NativeCounterResets != 1 || got.NativeCounterRecoveries != 1 || got.UsageEventCount != 3 {
		t.Fatalf("aggregation metadata = %#v", got)
	}
}

func TestParseCodexTokenStatsRetainsTurnBoundaryAcrossNonAdvancingSnapshot(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":100},"last_token_usage":{"total_tokens":100}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":40},"last_token_usage":{"total_tokens":40}}}}`,
		`{"type":"turn_context","payload":{"turn_id":"fixed-codex-turn"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":40},"last_token_usage":{"total_tokens":40}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":170},"last_token_usage":{"total_tokens":30}}}}`,
	}, "\n")

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total.TotalTokens != 170 || got.NativeCounterRecoveries != 1 || got.NonAdvancingUsageEvents != 1 {
		t.Fatalf("stats = %#v", got)
	}
}

func TestParseCodexTokenStatsDoesNotRecoverAfterFallbackUsageConsumesTurnBoundary(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":100},"last_token_usage":{"total_tokens":100}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":40},"last_token_usage":{"total_tokens":40}}}}`,
		`{"type":"turn_context","payload":{"turn_id":"fallback-first"}}`,
		`{"type":"turn.completed","usage":{"total_tokens":5}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":170},"last_token_usage":{"total_tokens":30}}}}`,
	}, "\n")

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total.TotalTokens != 270 || got.NativeCounterRecoveries != 0 {
		t.Fatalf("stats = %#v", got)
	}
}

func TestParseCodexTokenStatsUsesHealthyNativeTotalToBridgeMissingUpdate(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":90,"output_tokens":10,"total_tokens":100},"last_token_usage":{"input_tokens":90,"output_tokens":10,"total_tokens":100}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":180,"output_tokens":20,"total_tokens":200},"last_token_usage":{"input_tokens":45,"output_tokens":5,"total_tokens":50}}}}`,
	}, "\n")

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total != (CodexTokenUsage{InputTokens: 180, OutputTokens: 20, TotalTokens: 200}) {
		t.Fatalf("reconstructed total = %#v", got.Info.Total)
	}
	if got.NativeCounterResets != 0 || got.NativeCounterRecoveries != 0 || got.UsageEventCount != 2 {
		t.Fatalf("aggregation metadata = %#v", got)
	}
}

func TestParseCodexTokenStatsAddsCompleteFirstSnapshotAfterReset(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":100},"last_token_usage":{"total_tokens":100}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":80},"last_token_usage":{"total_tokens":30}}}}`,
	}, "\n")

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total.TotalTokens != 180 {
		t.Fatalf("reconstructed total = %d, want 180", got.Info.Total.TotalTokens)
	}
	if got.NativeCounterResets != 1 || got.UsageEventCount != 2 {
		t.Fatalf("aggregation metadata = %#v", got)
	}
}

func TestParseCodexTokenStatsDetectsTurnResetThatStartsAbovePreviousTotal(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":90,"output_tokens":10,"total_tokens":100},"last_token_usage":{"input_tokens":90,"output_tokens":10,"total_tokens":100}}}}`,
		`{"type":"turn_context","payload":{"turn_id":"large-reset-call"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":180,"output_tokens":20,"total_tokens":200},"last_token_usage":{"input_tokens":180,"output_tokens":20,"total_tokens":200}}}}`,
	}, "\n")

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	want := CodexTokenUsage{InputTokens: 270, OutputTokens: 30, TotalTokens: 300}
	if got.Info.Total != want || got.NativeCounterResets != 1 || got.NativeCounterRecoveries != 0 {
		t.Fatalf("stats = %#v, want total %#v", got, want)
	}
}

func TestParseCodexTokenStatsDetectsTurnResetFromDecreasingComponent(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":900,"cached_input_tokens":800,"output_tokens":100,"total_tokens":1000},"last_token_usage":{"input_tokens":90,"cached_input_tokens":80,"output_tokens":10,"total_tokens":100}}}}`,
		`{"type":"turn_context","payload":{"turn_id":"component-reset"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":1050,"cached_input_tokens":700,"output_tokens":150,"total_tokens":1200},"last_token_usage":{"input_tokens":150,"cached_input_tokens":100,"output_tokens":50,"total_tokens":200}}}}`,
	}, "\n")

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	want := CodexTokenUsage{InputTokens: 1950, CachedInputTokens: 1500, OutputTokens: 250, TotalTokens: 2200}
	if got.Info.Total != want || got.NativeCounterResets != 1 || got.NativeCounterRecoveries != 0 {
		t.Fatalf("stats = %#v, want total %#v", got, want)
	}
}

func TestParseCodexTokenStatsDetectsEqualTotalTurnResetFromDecreasingComponent(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":900,"cached_input_tokens":800,"output_tokens":100,"total_tokens":1000},"last_token_usage":{"input_tokens":90,"cached_input_tokens":80,"output_tokens":10,"total_tokens":100}}}}`,
		`{"type":"turn_context","payload":{"turn_id":"equal-total-component-reset"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":950,"cached_input_tokens":700,"output_tokens":50,"total_tokens":1000},"last_token_usage":{"input_tokens":950,"cached_input_tokens":700,"output_tokens":50,"total_tokens":1000}}}}`,
	}, "\n")

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	want := CodexTokenUsage{InputTokens: 1850, CachedInputTokens: 1500, OutputTokens: 150, TotalTokens: 2000}
	if got.Info.Total != want || got.NativeCounterResets != 1 || got.NonAdvancingUsageEvents != 0 {
		t.Fatalf("stats = %#v, want total %#v", got, want)
	}
}

func TestParseCodexTokenStatsUsesEpochCumulativeTotalAfterReset(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":100},"last_token_usage":{"total_tokens":100}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":40},"last_token_usage":{"total_tokens":40}}}}`,
		// The native epoch advanced by 130 while only the latest 30-token call is
		// present. The epoch cumulative value preserves the omitted 100 tokens.
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":170},"last_token_usage":{"total_tokens":30}}}}`,
	}, "\n")

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total.TotalTokens != 270 {
		t.Fatalf("reconstructed total = %d, want 270", got.Info.Total.TotalTokens)
	}
	if got.NativeCounterResets != 1 || got.NativeCounterRecoveries != 0 || got.UsageEventCount != 3 {
		t.Fatalf("aggregation metadata = %#v", got)
	}
}

func TestParseCodexTokenStatsReportsMissingLastWithoutLosingEpochUsage(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":100},"last_token_usage":{"total_tokens":100}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":40},"last_token_usage":{"total_tokens":40}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":70}}}}`,
	}, "\n")

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total.TotalTokens != 170 || got.MissingLastUsageEvents != 1 {
		t.Fatalf("stats = %#v", got)
	}
	if rendered := strings.Join(formatCodexTokenStatsLines(got), "\n"); !strings.Contains(rendered, "totals remain reconstructed as reset epochs") {
		t.Fatalf("missing recovery warning:\n%s", rendered)
	}
}

func TestParseCodexTokenStatsSaturatesAggregationOverflow(t *testing.T) {
	input := fmt.Sprintf("%s\n%s",
		fmt.Sprintf(`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":%d},"last_token_usage":{"total_tokens":%d}}}}`, math.MaxInt64-5, math.MaxInt64-5),
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":10},"last_token_usage":{"total_tokens":10}}}}`,
	)

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total.TotalTokens != math.MaxInt64 || !got.UsageAggregationOverflow {
		t.Fatalf("stats = %#v", got)
	}
}

func TestParseCodexTokenStatsProductionScaleResetHistory(t *testing.T) {
	const (
		epochs          = 35
		updatesPerEpoch = 103
		nonAdvancing    = 27
	)
	var input strings.Builder
	var want CodexTokenUsage
	var latestNative CodexTokenUsage
	for epoch := 0; epoch < epochs; epoch++ {
		var native CodexTokenUsage
		for update := 0; update < updatesPerEpoch; update++ {
			last := CodexTokenUsage{
				InputTokens:           int64(900 + epoch*3 + update),
				CachedInputTokens:     int64(700 + epoch*2 + update/2),
				OutputTokens:          int64(30 + update%11),
				ReasoningOutputTokens: int64(7 + update%5),
			}
			last.TotalTokens = last.InputTokens + last.OutputTokens
			native.InputTokens += last.InputTokens
			native.CachedInputTokens += last.CachedInputTokens
			native.OutputTokens += last.OutputTokens
			native.ReasoningOutputTokens += last.ReasoningOutputTokens
			native.TotalTokens += last.TotalTokens
			want.InputTokens += last.InputTokens
			want.CachedInputTokens += last.CachedInputTokens
			want.OutputTokens += last.OutputTokens
			want.ReasoningOutputTokens += last.ReasoningOutputTokens
			want.TotalTokens += last.TotalTokens
			fmt.Fprintf(&input, `{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":%d,"cached_input_tokens":%d,"output_tokens":%d,"reasoning_output_tokens":%d,"total_tokens":%d},"last_token_usage":{"input_tokens":%d,"cached_input_tokens":%d,"output_tokens":%d,"reasoning_output_tokens":%d,"total_tokens":%d}}}}`+"\n",
				native.InputTokens, native.CachedInputTokens, native.OutputTokens, native.ReasoningOutputTokens, native.TotalTokens,
				last.InputTokens, last.CachedInputTokens, last.OutputTokens, last.ReasoningOutputTokens, last.TotalTokens)
		}
		latestNative = native
		if epoch < nonAdvancing {
			fmt.Fprintf(&input, `{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":%d,"cached_input_tokens":%d,"output_tokens":%d,"reasoning_output_tokens":%d,"total_tokens":%d},"last_token_usage":{"total_tokens":1}}}}`+"\n",
				native.InputTokens, native.CachedInputTokens, native.OutputTokens, native.ReasoningOutputTokens, native.TotalTokens)
		}
	}

	got, err := ParseCodexTokenStats(strings.NewReader(input.String()))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total != want {
		t.Fatalf("reconstructed total = %#v, want %#v", got.Info.Total, want)
	}
	if got.NativeLatestTotal != latestNative {
		t.Fatalf("native latest = %#v, want %#v", got.NativeLatestTotal, latestNative)
	}
	if got.UsageEventCount != epochs*updatesPerEpoch || got.NonAdvancingUsageEvents != nonAdvancing || got.NativeCounterResets != epochs-1 || got.NativeCounterRecoveries != 0 {
		t.Fatalf("aggregation metadata = %#v", got)
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

func TestFormatCodexTokenStatsRateOnlyDoesNotClaimUsageReconstruction(t *testing.T) {
	input := `{"type":"event_msg","payload":{"type":"token_count","info":null,"rate_limits":{"limit_id":"codex","primary":{"used_percent":80,"window_minutes":300}}}}`

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	rendered := strings.Join(formatCodexTokenStatsLines(got), "\n")
	if !strings.Contains(rendered, "metadata was found, but it did not contain a usage snapshot") || !strings.Contains(rendered, "Token usage unavailable") {
		t.Fatalf("rate-only stats = %q", rendered)
	}
	if strings.Contains(rendered, "reconstructed conversation usage") {
		t.Fatalf("rate-only stats falsely claimed usage reconstruction: %q", rendered)
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
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":300,"cached_input_tokens":120,"output_tokens":30,"reasoning_output_tokens":10,"total_tokens":330},"last_token_usage":{"input_tokens":200,"cached_input_tokens":100,"output_tokens":20,"reasoning_output_tokens":5,"total_tokens":220},"model_context_window":1000},"rate_limits":{"limit_id":"codex","plan_type":"business"}}}` + "\n" +
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":100,"cached_input_tokens":50,"output_tokens":10,"reasoning_output_tokens":3,"total_tokens":110},"last_token_usage":{"input_tokens":100,"cached_input_tokens":50,"output_tokens":10,"reasoning_output_tokens":3,"total_tokens":110},"model_context_window":1000}}}` + "\n" +
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":180,"cached_input_tokens":90,"output_tokens":20,"reasoning_output_tokens":5,"total_tokens":200},"last_token_usage":{"input_tokens":80,"cached_input_tokens":40,"output_tokens":10,"reasoning_output_tokens":2,"total_tokens":90},"model_context_window":1000}}}` + "\n"
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
		"<tr><td><strong>input</strong></td><td>80 (cached 40, non-cached 40)</td><td>480 (cached 210, non-cached 270)</td></tr>",
		"<tr><td><strong>Cache hit rate</strong></td><td>50.0%</td><td>43.8%</td></tr>",
		"<tr><td><strong>output</strong></td><td>10 (reasoning 2)</td><td>50 (reasoning 15)</td></tr>",
		"<tr><td><strong>total</strong></td><td>90</td><td>530</td></tr>",
		"<p><strong>Analysis:</strong></p>",
		"<ul><li><strong>model context window:</strong> 1,000; current context uses 9.0%; approx remaining: 910</li>",
		"<li><strong>native latest cumulative total:</strong> 200; reconstructed conversation total: 530</li>",
		"<strong>Aggregation:</strong> ignored 0 non-advancing usage snapshot(s); observed 1 native cumulative counter reset(s) and 0 recovery event(s).",
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
