package engine

// The parser and formatting tests of v0.1.20 (internal/teams/
// token_stats_test.go @ 2ae5e748, lines 16-570), ported mechanically:
// the core's types and names, the same inputs and expectations.

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
)

func TestV0120ParseCodexTokenStatsPrefersTokenCountAndReportsDiagnostics(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-1"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":1370490,"cached_input_tokens":1197952,"output_tokens":4893,"reasoning_output_tokens":1127,"total_tokens":1375383},"last_token_usage":{"input_tokens":168707,"cached_input_tokens":154496,"output_tokens":674,"reasoning_output_tokens":231,"total_tokens":169381},"model_context_window":258400},"rate_limits":{"limit_id":"codex","limit_name":"Codex","plan_type":"business","primary":{"used_percent":42.5,"window_minutes":10,"resets_at":1704069000},"credits":{"has_credits":true,"unlimited":false,"balance":"42"}}}}`,
		`{"type":"turn.completed","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`,
		`{"type":"event_msg","payload":{"type":"agent_message","message":"done"}`,
	}, "\n")

	got, err := transcript.ParseTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Source != "token_count" || got.SourceLine != 2 || got.FallbackOnly {
		t.Fatalf("source = %#v", got)
	}
	if got.Info.Last.Input != 168707 || got.Info.Last.Cached != 154496 || got.Info.Last.Reasoning != 231 || got.Info.Last.Total != 169381 {
		t.Fatalf("last usage = %#v", got.Info.Last)
	}
	if got.Info.Total.Input != 1370490 || got.Info.Total.Cached != 1197952 || got.Info.Total.Reasoning != 1127 || got.Info.Total.Total != 1375383 {
		t.Fatalf("total usage = %#v", got.Info.Total)
	}
	if got.NativeLatestTotal != got.Info.Total || got.UsageEvents != 1 || got.NonAdvancing != 0 || got.Resets != 0 {
		t.Fatalf("aggregation metadata = %#v", got)
	}
	if got.Info.ContextWindow != 258400 {
		t.Fatalf("context window = %d", got.Info.ContextWindow)
	}
	if !got.RateLimits.Present || got.RateLimits.LimitID != "codex" || got.RateLimits.PlanType != "business" || len(got.RateLimits.Windows) != 1 {
		t.Fatalf("rate limits = %#v", got.RateLimits)
	}
	if got.RateLimits.LimitName != "Codex" || !got.RateLimits.Credits.Present || !got.RateLimits.Credits.HasCredits || got.RateLimits.Credits.Balance != "42" {
		t.Fatalf("rate limit details = %#v", got.RateLimits)
	}
	if len(got.Diagnostics) != 1 || got.Diagnostics["invalid_json"] != 1 {
		t.Fatalf("diagnostics = %#v", got.Diagnostics)
	}

	rendered := strings.Join(mainAgentLines(got, nil), "\n")
	for _, want := range []string{
		"Last recorded model usage:",
		"Conversation total:",
		"Cache hit rate: 91.6%",
		"model context window: 258,400",
		"current context uses",
		"Codex baseline-adjusted context remaining:",
		"🧠 MAIN AGENT · rate limits:",
		"limit name: Codex",
		"reset 2024-01-01T00:30:00Z",
		"Diagnostics: skipped",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("formatted stats missing %q:\n%s", want, rendered)
		}
	}
}

func TestV0120ParseCodexTokenStatsReconstructsMonotonicCumulativeUsage(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":90,"cached_input_tokens":60,"output_tokens":10,"total_tokens":100},"last_token_usage":{"input_tokens":35,"cached_input_tokens":20,"output_tokens":5,"total_tokens":40},"model_context_window":1000}}}`,
		`{"type":"turn_context","payload":{"turn_id":"healthy-next-turn"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":140,"cached_input_tokens":90,"output_tokens":20,"total_tokens":160},"last_token_usage":{"input_tokens":50,"cached_input_tokens":30,"output_tokens":10,"total_tokens":60},"model_context_window":2000}}}`,
	}, "\n")

	got, err := transcript.ParseTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total != (transcript.TokenUsage{Input: 140, Cached: 90, Output: 20, Total: 160}) {
		t.Fatalf("reconstructed total = %#v", got.Info.Total)
	}
	if got.Info.Last.Total != 60 || got.Info.ContextWindow != 2000 {
		t.Fatalf("latest usage info = %#v", got.Info)
	}
	if got.NativeLatestTotal != got.Info.Total || got.UsageEvents != 2 || got.NonAdvancing != 0 || got.Resets != 0 {
		t.Fatalf("aggregation metadata = %#v", got)
	}
}

func TestV0120ParseCodexTokenStatsReconstructsAcrossNativeResetsAndNonAdvancingSnapshots(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":90,"cached_input_tokens":60,"output_tokens":10,"total_tokens":100},"last_token_usage":{"input_tokens":90,"cached_input_tokens":60,"output_tokens":10,"total_tokens":100}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":140,"cached_input_tokens":90,"output_tokens":20,"total_tokens":160},"last_token_usage":{"input_tokens":50,"cached_input_tokens":30,"output_tokens":10,"total_tokens":60}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":30,"cached_input_tokens":20,"output_tokens":10,"total_tokens":40},"last_token_usage":{"input_tokens":30,"cached_input_tokens":20,"output_tokens":10,"total_tokens":40}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":30,"cached_input_tokens":20,"output_tokens":10,"total_tokens":40},"last_token_usage":{"input_tokens":30,"cached_input_tokens":20,"output_tokens":10,"total_tokens":40}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":30,"cached_input_tokens":20,"output_tokens":10,"total_tokens":40},"last_token_usage":{"total_tokens":7}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":null,"rate_limits":{"limit_id":"codex","primary":{"used_percent":80,"window_minutes":300}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":70,"cached_input_tokens":40,"output_tokens":20,"total_tokens":90},"last_token_usage":{"input_tokens":40,"cached_input_tokens":20,"output_tokens":10,"total_tokens":50}}}}`,
	}, "\n")

	got, err := transcript.ParseTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	want := transcript.TokenUsage{Input: 210, Cached: 130, Output: 40, Total: 250}
	if got.Info.Total != want {
		t.Fatalf("reconstructed total = %#v, want %#v", got.Info.Total, want)
	}
	if got.NativeLatestTotal.Total != 90 || got.UsageEvents != 4 || got.NonAdvancing != 2 || got.Resets != 1 {
		t.Fatalf("aggregation metadata = %#v", got)
	}
	if !got.RateLimits.Present || len(got.RateLimits.Windows) != 1 || got.RateLimits.Windows[0].UsedPercent != 80 {
		t.Fatalf("rate limits = %#v", got.RateLimits)
	}
	rendered := strings.Join(mainAgentLines(got, nil), "\n")
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

func TestV0120ParseCodexTokenStatsDoesNotDoubleCountWhenNativeTotalRecovers(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":100},"last_token_usage":{"total_tokens":100}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":40},"last_token_usage":{"total_tokens":40}}}}`,
		`{"type":"turn_context","payload":{"turn_id":"fixed-codex-turn"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":170},"last_token_usage":{"total_tokens":30}}}}`,
	}, "\n")

	got, err := transcript.ParseTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total.Total != 170 {
		t.Fatalf("reconstructed total = %d, want 170", got.Info.Total.Total)
	}
	if got.NativeLatestTotal.Total != 170 || got.Resets != 1 || got.Recoveries != 1 || got.UsageEvents != 3 {
		t.Fatalf("aggregation metadata = %#v", got)
	}
}

func TestV0120ParseCodexTokenStatsRetainsTurnBoundaryAcrossNonAdvancingSnapshot(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":100},"last_token_usage":{"total_tokens":100}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":40},"last_token_usage":{"total_tokens":40}}}}`,
		`{"type":"turn_context","payload":{"turn_id":"fixed-codex-turn"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":40},"last_token_usage":{"total_tokens":40}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":170},"last_token_usage":{"total_tokens":30}}}}`,
	}, "\n")

	got, err := transcript.ParseTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total.Total != 170 || got.Recoveries != 1 || got.NonAdvancing != 1 {
		t.Fatalf("stats = %#v", got)
	}
}

func TestV0120ParseCodexTokenStatsDoesNotRecoverAfterFallbackUsageConsumesTurnBoundary(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":100},"last_token_usage":{"total_tokens":100}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":40},"last_token_usage":{"total_tokens":40}}}}`,
		`{"type":"turn_context","payload":{"turn_id":"fallback-first"}}`,
		`{"type":"turn.completed","usage":{"total_tokens":5}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":170},"last_token_usage":{"total_tokens":30}}}}`,
	}, "\n")

	got, err := transcript.ParseTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total.Total != 270 || got.Recoveries != 0 {
		t.Fatalf("stats = %#v", got)
	}
}

func TestV0120ParseCodexTokenStatsUsesHealthyNativeTotalToBridgeMissingUpdate(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":90,"output_tokens":10,"total_tokens":100},"last_token_usage":{"input_tokens":90,"output_tokens":10,"total_tokens":100}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":180,"output_tokens":20,"total_tokens":200},"last_token_usage":{"input_tokens":45,"output_tokens":5,"total_tokens":50}}}}`,
	}, "\n")

	got, err := transcript.ParseTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total != (transcript.TokenUsage{Input: 180, Output: 20, Total: 200}) {
		t.Fatalf("reconstructed total = %#v", got.Info.Total)
	}
	if got.Resets != 0 || got.Recoveries != 0 || got.UsageEvents != 2 {
		t.Fatalf("aggregation metadata = %#v", got)
	}
}

func TestV0120ParseCodexTokenStatsAddsCompleteFirstSnapshotAfterReset(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":100},"last_token_usage":{"total_tokens":100}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":80},"last_token_usage":{"total_tokens":30}}}}`,
	}, "\n")

	got, err := transcript.ParseTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total.Total != 180 {
		t.Fatalf("reconstructed total = %d, want 180", got.Info.Total.Total)
	}
	if got.Resets != 1 || got.UsageEvents != 2 {
		t.Fatalf("aggregation metadata = %#v", got)
	}
}

func TestV0120ParseCodexTokenStatsDetectsTurnResetThatStartsAbovePreviousTotal(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":90,"output_tokens":10,"total_tokens":100},"last_token_usage":{"input_tokens":90,"output_tokens":10,"total_tokens":100}}}}`,
		`{"type":"turn_context","payload":{"turn_id":"large-reset-call"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":180,"output_tokens":20,"total_tokens":200},"last_token_usage":{"input_tokens":180,"output_tokens":20,"total_tokens":200}}}}`,
	}, "\n")

	got, err := transcript.ParseTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	want := transcript.TokenUsage{Input: 270, Output: 30, Total: 300}
	if got.Info.Total != want || got.Resets != 1 || got.Recoveries != 0 {
		t.Fatalf("stats = %#v, want total %#v", got, want)
	}
}

func TestV0120ParseCodexTokenStatsDetectsTurnResetFromDecreasingComponent(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":900,"cached_input_tokens":800,"output_tokens":100,"total_tokens":1000},"last_token_usage":{"input_tokens":90,"cached_input_tokens":80,"output_tokens":10,"total_tokens":100}}}}`,
		`{"type":"turn_context","payload":{"turn_id":"component-reset"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":1050,"cached_input_tokens":700,"output_tokens":150,"total_tokens":1200},"last_token_usage":{"input_tokens":150,"cached_input_tokens":100,"output_tokens":50,"total_tokens":200}}}}`,
	}, "\n")

	got, err := transcript.ParseTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	want := transcript.TokenUsage{Input: 1950, Cached: 1500, Output: 250, Total: 2200}
	if got.Info.Total != want || got.Resets != 1 || got.Recoveries != 0 {
		t.Fatalf("stats = %#v, want total %#v", got, want)
	}
}

func TestV0120ParseCodexTokenStatsDetectsEqualTotalTurnResetFromDecreasingComponent(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":900,"cached_input_tokens":800,"output_tokens":100,"total_tokens":1000},"last_token_usage":{"input_tokens":90,"cached_input_tokens":80,"output_tokens":10,"total_tokens":100}}}}`,
		`{"type":"turn_context","payload":{"turn_id":"equal-total-component-reset"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":950,"cached_input_tokens":700,"output_tokens":50,"total_tokens":1000},"last_token_usage":{"input_tokens":950,"cached_input_tokens":700,"output_tokens":50,"total_tokens":1000}}}}`,
	}, "\n")

	got, err := transcript.ParseTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	want := transcript.TokenUsage{Input: 1850, Cached: 1500, Output: 150, Total: 2000}
	if got.Info.Total != want || got.Resets != 1 || got.NonAdvancing != 0 {
		t.Fatalf("stats = %#v, want total %#v", got, want)
	}
}

func TestV0120ParseCodexTokenStatsUsesEpochCumulativeTotalAfterReset(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":100},"last_token_usage":{"total_tokens":100}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":40},"last_token_usage":{"total_tokens":40}}}}`,
		// The native epoch advanced by 130 while only the latest 30-token call is
		// present. The epoch cumulative value preserves the omitted 100 tokens.
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":170},"last_token_usage":{"total_tokens":30}}}}`,
	}, "\n")

	got, err := transcript.ParseTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total.Total != 270 {
		t.Fatalf("reconstructed total = %d, want 270", got.Info.Total.Total)
	}
	if got.Resets != 1 || got.Recoveries != 0 || got.UsageEvents != 3 {
		t.Fatalf("aggregation metadata = %#v", got)
	}
}

func TestV0120ParseCodexTokenStatsReportsMissingLastWithoutLosingEpochUsage(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":100},"last_token_usage":{"total_tokens":100}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":40},"last_token_usage":{"total_tokens":40}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":70}}}}`,
	}, "\n")

	got, err := transcript.ParseTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total.Total != 170 || got.MissingLast != 1 {
		t.Fatalf("stats = %#v", got)
	}
	if rendered := strings.Join(mainAgentLines(got, nil), "\n"); !strings.Contains(rendered, "totals remain reconstructed as reset epochs") {
		t.Fatalf("missing recovery warning:\n%s", rendered)
	}
}

func TestV0120ParseCodexTokenStatsSaturatesAggregationOverflow(t *testing.T) {
	input := fmt.Sprintf("%s\n%s",
		fmt.Sprintf(`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":%d},"last_token_usage":{"total_tokens":%d}}}}`, math.MaxInt64-5, math.MaxInt64-5),
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":10},"last_token_usage":{"total_tokens":10}}}}`,
	)

	got, err := transcript.ParseTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total.Total != math.MaxInt64 || !got.Overflow {
		t.Fatalf("stats = %#v", got)
	}
}

func TestV0120ParseCodexTokenStatsProductionScaleResetHistory(t *testing.T) {
	const (
		epochs          = 35
		updatesPerEpoch = 103
		nonAdvancing    = 27
	)
	var input strings.Builder
	var want transcript.TokenUsage
	var latestNative transcript.TokenUsage
	for epoch := 0; epoch < epochs; epoch++ {
		var native transcript.TokenUsage
		for update := 0; update < updatesPerEpoch; update++ {
			last := transcript.TokenUsage{
				Input:     int64(900 + epoch*3 + update),
				Cached:    int64(700 + epoch*2 + update/2),
				Output:    int64(30 + update%11),
				Reasoning: int64(7 + update%5),
			}
			last.Total = last.Input + last.Output
			native.Input += last.Input
			native.Cached += last.Cached
			native.Output += last.Output
			native.Reasoning += last.Reasoning
			native.Total += last.Total
			want.Input += last.Input
			want.Cached += last.Cached
			want.Output += last.Output
			want.Reasoning += last.Reasoning
			want.Total += last.Total
			fmt.Fprintf(&input, `{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":%d,"cached_input_tokens":%d,"output_tokens":%d,"reasoning_output_tokens":%d,"total_tokens":%d},"last_token_usage":{"input_tokens":%d,"cached_input_tokens":%d,"output_tokens":%d,"reasoning_output_tokens":%d,"total_tokens":%d}}}}`+"\n",
				native.Input, native.Cached, native.Output, native.Reasoning, native.Total,
				last.Input, last.Cached, last.Output, last.Reasoning, last.Total)
		}
		latestNative = native
		if epoch < nonAdvancing {
			fmt.Fprintf(&input, `{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":%d,"cached_input_tokens":%d,"output_tokens":%d,"reasoning_output_tokens":%d,"total_tokens":%d},"last_token_usage":{"total_tokens":1}}}}`+"\n",
				native.Input, native.Cached, native.Output, native.Reasoning, native.Total)
		}
	}

	got, err := transcript.ParseTokenStats(strings.NewReader(input.String()))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total != want {
		t.Fatalf("reconstructed total = %#v, want %#v", got.Info.Total, want)
	}
	if got.NativeLatestTotal != latestNative {
		t.Fatalf("native latest = %#v, want %#v", got.NativeLatestTotal, latestNative)
	}
	if got.UsageEvents != epochs*updatesPerEpoch || got.NonAdvancing != nonAdvancing || got.Resets != epochs-1 || got.Recoveries != 0 {
		t.Fatalf("aggregation metadata = %#v", got)
	}
}

func TestV0120ParseCodexTokenStatsMergesRateOnlyTokenCount(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":100,"cached_input_tokens":20,"output_tokens":10,"reasoning_output_tokens":2,"total_tokens":110},"last_token_usage":{"input_tokens":100,"cached_input_tokens":20,"output_tokens":10,"reasoning_output_tokens":2,"total_tokens":110},"model_context_window":1000},"rate_limits":null}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":null,"rate_limits":{"limit_id":"codex","primary":{"used_percent":80,"window_minutes":300,"resets_at":1704074400}}}}`,
	}, "\n")

	got, err := transcript.ParseTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.SourceLine != 1 || got.Info.Last.Total != 110 {
		t.Fatalf("merged stats = %#v", got)
	}
	if !got.RateLimits.Present || len(got.RateLimits.Windows) != 1 || got.RateLimits.Windows[0].UsedPercent != 80 {
		t.Fatalf("merged rate limits = %#v", got.RateLimits)
	}
}

func TestV0120FormatCodexTokenStatsHidesMetadataOnlyRateLimits(t *testing.T) {
	input := `{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":100,"cached_input_tokens":80,"output_tokens":10,"total_tokens":110},"last_token_usage":{"input_tokens":100,"cached_input_tokens":80,"output_tokens":10,"total_tokens":110}},"rate_limits":{"limit_id":"codex","plan_type":"business","credits":{"has_credits":true}}}}`

	got, err := transcript.ParseTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if !got.RateLimits.Present || !got.RateLimits.Credits.Present {
		t.Fatalf("rate limits = %#v", got.RateLimits)
	}
	rendered := strings.Join(mainAgentLines(got, nil), "\n")
	if strings.Contains(rendered, "rate limits:") || strings.Contains(rendered, "credits: available") {
		t.Fatalf("metadata-only rate limits should be hidden:\n%s", rendered)
	}
}

func TestV0120FormatCodexTokenStatsRateOnlyDoesNotClaimUsageReconstruction(t *testing.T) {
	input := `{"type":"event_msg","payload":{"type":"token_count","info":null,"rate_limits":{"limit_id":"codex","primary":{"used_percent":80,"window_minutes":300}}}}`

	got, err := transcript.ParseTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	rendered := strings.Join(mainAgentLines(got, nil), "\n")
	if !strings.Contains(rendered, "metadata was found, but it did not contain a usage snapshot") || !strings.Contains(rendered, "Token usage unavailable") {
		t.Fatalf("rate-only stats = %q", rendered)
	}
	if strings.Contains(rendered, "reconstructed conversation usage") {
		t.Fatalf("rate-only stats falsely claimed usage reconstruction: %q", rendered)
	}
}

func TestV0120ParseCodexTokenStatsAllowsLargeTranscriptLines(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":100,"cached_input_tokens":80,"output_tokens":10,"total_tokens":110},"last_token_usage":{"input_tokens":100,"cached_input_tokens":80,"output_tokens":10,"total_tokens":110}}}}`,
		strings.Repeat("x", 17<<20),
	}, "\n")

	got, err := transcript.ParseTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Last.Total != 110 || got.SourceLine != 1 {
		t.Fatalf("stats = %#v", got)
	}
}

func TestV0120ParseCodexTokenStatsFallsBackToUsageFields(t *testing.T) {
	input := `{"type":"turn.completed","usage":{"input_tokens":100,"output_tokens":12,"reasoning_output_tokens":5,"total_tokens":112,"input_tokens_details":{"cached_tokens":34}}}`

	got, err := transcript.ParseTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if !got.FallbackOnly || got.Source != "event usage" {
		t.Fatalf("source = %#v", got)
	}
	if got.Info.Last.Input != 100 || got.Info.Last.Cached != 34 || got.Info.Last.Reasoning != 5 || got.Info.Last.Total != 112 {
		t.Fatalf("fallback usage = %#v", got.Info.Last)
	}
	rendered := strings.Join(mainAgentLines(got, nil), "\n")
	if !strings.Contains(rendered, "runner usage fallback") {
		t.Fatalf("fallback formatted stats = %q", rendered)
	}
}

func TestV0120ParseCodexTokenStatsAggregatesUsageByModelAndTier(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"thread_settings_applied","thread_settings":{"model":"gpt-5.6-sol","service_tier":"default"}}}`,
		`{"type":"turn_context","payload":{"turn_id":"turn-1","model":"gpt-5.6-sol"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":80,"cached_input_tokens":60,"output_tokens":20,"total_tokens":100},"last_token_usage":{"input_tokens":80,"cached_input_tokens":60,"output_tokens":20,"total_tokens":100}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":130,"cached_input_tokens":90,"output_tokens":30,"total_tokens":160},"last_token_usage":{"input_tokens":50,"cached_input_tokens":30,"output_tokens":10,"total_tokens":60}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":130,"cached_input_tokens":90,"output_tokens":30,"total_tokens":160},"last_token_usage":{"input_tokens":50,"cached_input_tokens":30,"output_tokens":10,"total_tokens":60}}}}`,
		`{"type":"event_msg","payload":{"type":"thread_settings_applied","thread_settings":{"model":"gpt-5.6-luna","service_tier":"default"}}}`,
		`{"type":"turn_context","payload":{"turn_id":"turn-2","model":"gpt-5.6-luna"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":30,"cached_input_tokens":10,"output_tokens":10,"total_tokens":40},"last_token_usage":{"input_tokens":30,"cached_input_tokens":10,"output_tokens":10,"total_tokens":40}}}}`,
		`{"type":"event_msg","payload":{"type":"thread_settings_applied","thread_settings":{"model":"gpt-5.6-luna","service_tier":"priority"}}}`,
		`{"type":"turn_context","payload":{"turn_id":"turn-3","model":"gpt-5.6-luna"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":50,"cached_input_tokens":20,"output_tokens":20,"total_tokens":70},"last_token_usage":{"input_tokens":20,"cached_input_tokens":10,"output_tokens":10,"total_tokens":30}}}}`,
	}, "\n")

	got, err := transcript.ParseTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total.Total != 230 {
		t.Fatalf("conversation total = %d, want 230", got.Info.Total.Total)
	}
	want := []transcript.ModelTierUsage{
		{Model: "gpt-5.6-luna", Tier: "default", Usage: transcript.TokenUsage{Input: 30, Cached: 10, Output: 10, Total: 40}},
		{Model: "gpt-5.6-luna", Tier: "priority", Usage: transcript.TokenUsage{Input: 20, Cached: 10, Output: 10, Total: 30}},
		{Model: "gpt-5.6-sol", Tier: "default", Usage: transcript.TokenUsage{Input: 130, Cached: 90, Output: 30, Total: 160}},
	}
	if !reflect.DeepEqual(got.ModelTiers, want) {
		t.Fatalf("model/tier usages = %#v, want %#v", got.ModelTiers, want)
	}
	if rendered := strings.Join(mainAgentLines(got, nil), "\n"); strings.Contains(rendered, "Tier:") {
		t.Fatalf("stats show service tiers, which 285784d3^ did not:\n%s", rendered)
	}
}

func TestV0120ParseCodexTokenStatsKeepsMissingModelTierMetadataUnknown(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"turn_context","payload":{"turn_id":"turn-1","model":"model-a"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12},"last_token_usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}}}}`,
		`{"type":"event_msg","payload":{"type":"thread_settings_applied","thread_settings":{"model":"model-a","service_tier":"default"}}}`,
		`{"type":"turn_context","payload":{"turn_id":"turn-2","model":"model-a"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":20,"output_tokens":4,"total_tokens":24},"last_token_usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}}}}`,
	}, "\n")

	got, err := transcript.ParseTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	want := []transcript.ModelTierUsage{
		{Model: "model-a", Tier: "default", Usage: transcript.TokenUsage{Input: 10, Output: 2, Total: 12}},
		{Model: "model-a", Tier: "unknown", Usage: transcript.TokenUsage{Input: 10, Output: 2, Total: 12}},
	}
	if !reflect.DeepEqual(got.ModelTiers, want) {
		t.Fatalf("model/tier usages = %#v, want %#v", got.ModelTiers, want)
	}
}

func TestV0120ParseCodexTokenStatsGroupsFallbackUsageByModelAndTier(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"thread_settings_applied","thread_settings":{"model":"model-a","service_tier":"default"}}}`,
		`{"type":"turn_context","payload":{"turn_id":"turn-1","model":"model-a"}}`,
		`{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}}`,
		`{"type":"event_msg","payload":{"type":"thread_settings_applied","thread_settings":{"model":"model-b","service_tier":"priority"}}}`,
		`{"type":"turn_context","payload":{"turn_id":"turn-2","model":"model-b"}}`,
		`{"type":"turn.completed","usage":{"input_tokens":20,"output_tokens":3,"total_tokens":23}}`,
	}, "\n")

	got, err := transcript.ParseTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	want := []transcript.ModelTierUsage{
		{Model: "model-a", Tier: "default", Usage: transcript.TokenUsage{Input: 10, Output: 2, Total: 12}},
		{Model: "model-b", Tier: "priority", Usage: transcript.TokenUsage{Input: 20, Output: 3, Total: 23}},
	}
	if !reflect.DeepEqual(got.ModelTiers, want) {
		t.Fatalf("model/tier fallback usages = %#v, want %#v", got.ModelTiers, want)
	}
}
