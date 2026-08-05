package teams

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
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

func TestParseCodexTokenStatsAggregatesUsageByModelAndTier(t *testing.T) {
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

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total.TotalTokens != 230 {
		t.Fatalf("conversation total = %d, want 230", got.Info.Total.TotalTokens)
	}
	want := []CodexModelTierUsage{
		{Model: "gpt-5.6-luna", Tier: "default", Usage: CodexTokenUsage{InputTokens: 30, CachedInputTokens: 10, OutputTokens: 10, TotalTokens: 40}},
		{Model: "gpt-5.6-luna", Tier: "priority", Usage: CodexTokenUsage{InputTokens: 20, CachedInputTokens: 10, OutputTokens: 10, TotalTokens: 30}},
		{Model: "gpt-5.6-sol", Tier: "default", Usage: CodexTokenUsage{InputTokens: 130, CachedInputTokens: 90, OutputTokens: 30, TotalTokens: 160}},
	}
	if !reflect.DeepEqual(got.ModelTierUsages, want) {
		t.Fatalf("model/tier usages = %#v, want %#v", got.ModelTierUsages, want)
	}
	rendered := strings.Join(formatCodexTokenStatsLines(got), "\n")
	for _, wantText := range []string{
		"Model/tier usage:",
		"Model: gpt-5.6-luna",
		"Tier: default",
		"Tier: priority",
		"Model: gpt-5.6-sol",
		"input: 130 (cached 90, non-cached 40)",
		"total: 160",
		"model/tier attribution: 3 observed combination(s)",
	} {
		if !strings.Contains(rendered, wantText) {
			t.Fatalf("formatted stats missing %q:\n%s", wantText, rendered)
		}
	}
}

func TestParseCodexTokenStatsAggregatesUsageByModelAndEffort(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"thread_settings_applied","thread_settings":{"model":"model-a","service_tier":"default","reasoning_effort":"high"}}}`,
		`{"type":"turn_context","payload":{"turn_id":"turn-1","model":"model-a","effort":"high"}}`,
		`{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}}`,
		`{"type":"event_msg","payload":{"type":"thread_settings_applied","thread_settings":{"model":"model-a","service_tier":"default","reasoning_effort":"max"}}}`,
		`{"type":"turn_context","payload":{"turn_id":"turn-2","model":"model-a","effort":"max"}}`,
		`{"type":"turn.completed","usage":{"input_tokens":20,"output_tokens":3,"total_tokens":23}}`,
		`{"type":"event_msg","payload":{"type":"thread_settings_applied","thread_settings":{"model":"model-b","service_tier":"priority","reasoning_effort":"max"}}}`,
		`{"type":"turn_context","payload":{"turn_id":"turn-3","model":"model-b","effort":"max"}}`,
		`{"type":"turn.completed","usage":{"input_tokens":30,"output_tokens":4,"total_tokens":34}}`,
	}, "\n")

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total.TotalTokens != 69 {
		t.Fatalf("fallback conversation total = %d, want 69", got.Info.Total.TotalTokens)
	}
	want := []CodexModelUsage{
		{
			Model:   "model-a",
			Overall: CodexTokenUsage{InputTokens: 30, OutputTokens: 5, TotalTokens: 35},
			EffortUsages: []CodexEffortUsage{
				{Effort: "high", Usage: CodexTokenUsage{InputTokens: 10, OutputTokens: 2, TotalTokens: 12}},
				{Effort: "max", Usage: CodexTokenUsage{InputTokens: 20, OutputTokens: 3, TotalTokens: 23}},
			},
		},
		{
			Model:   "model-b",
			Overall: CodexTokenUsage{InputTokens: 30, OutputTokens: 4, TotalTokens: 34},
			EffortUsages: []CodexEffortUsage{
				{Effort: "max", Usage: CodexTokenUsage{InputTokens: 30, OutputTokens: 4, TotalTokens: 34}},
			},
		},
	}
	if !reflect.DeepEqual(got.ModelUsages, want) {
		t.Fatalf("model/effort usages = %#v, want %#v", got.ModelUsages, want)
	}
	rendered := strings.Join(formatCodexTokenStatsLines(got), "\n")
	for _, wantText := range []string{
		"Model/effort usage:",
		"Model: model-a",
		"Overall:",
		"high",
		"max",
		"Model: model-b",
		"model/effort attribution: 2 model(s), 3 per-effort group(s)",
	} {
		if !strings.Contains(rendered, wantText) {
			t.Fatalf("formatted model/effort stats missing %q:\n%s", wantText, rendered)
		}
	}
}

func TestParseCodexTokenStatsKeepsMissingModelAndEffortAsUnknown(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"turn_context","payload":{"turn_id":"turn-1"}}`,
		`{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}}`,
		`{"type":"event_msg","payload":{"type":"thread_settings_applied","thread_settings":{"model":"model-a"}}}`,
		`{"type":"turn_context","payload":{"turn_id":"turn-2","model":"model-a"}}`,
		`{"type":"turn.completed","usage":{"input_tokens":20,"output_tokens":3,"total_tokens":23}}`,
	}, "\n")

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	want := []CodexModelUsage{
		{Model: "model-a", Overall: CodexTokenUsage{InputTokens: 20, OutputTokens: 3, TotalTokens: 23}, EffortUsages: []CodexEffortUsage{{Effort: "unknown", Usage: CodexTokenUsage{InputTokens: 20, OutputTokens: 3, TotalTokens: 23}}}},
		{Model: "unknown", Overall: CodexTokenUsage{InputTokens: 10, OutputTokens: 2, TotalTokens: 12}, EffortUsages: []CodexEffortUsage{{Effort: "unknown", Usage: CodexTokenUsage{InputTokens: 10, OutputTokens: 2, TotalTokens: 12}}}},
	}
	if !reflect.DeepEqual(got.ModelUsages, want) {
		t.Fatalf("unknown model/effort usages = %#v, want %#v", got.ModelUsages, want)
	}
	rendered := strings.Join(formatCodexTokenStatsLines(got), "\n")
	for _, wantText := range []string{"Model: unknown", "Effort: unknown", "remain `unknown` instead of being guessed or merged"} {
		if !strings.Contains(rendered, wantText) {
			t.Fatalf("unknown model/effort diagnostic missing %q:\n%s", wantText, rendered)
		}
	}
}

func TestSummarizeCodexTokenStatsCombinesSubagentsByModelAndEffort(t *testing.T) {
	got := summarizeCodexTokenStats([]CodexTokenStats{
		{
			Info: CodexTokenUsageInfo{Total: CodexTokenUsage{InputTokens: 10, OutputTokens: 2, TotalTokens: 12}},
			ModelUsages: []CodexModelUsage{{
				Model:   "model-a",
				Overall: CodexTokenUsage{InputTokens: 10, OutputTokens: 2, TotalTokens: 12},
				EffortUsages: []CodexEffortUsage{{
					Effort: "max", Usage: CodexTokenUsage{InputTokens: 10, OutputTokens: 2, TotalTokens: 12},
				}},
			}},
		},
		{
			Info: CodexTokenUsageInfo{Total: CodexTokenUsage{InputTokens: 20, OutputTokens: 3, TotalTokens: 23}},
			ModelUsages: []CodexModelUsage{{
				Model:   "model-a",
				Overall: CodexTokenUsage{InputTokens: 20, OutputTokens: 3, TotalTokens: 23},
				EffortUsages: []CodexEffortUsage{{
					Effort: "max", Usage: CodexTokenUsage{InputTokens: 20, OutputTokens: 3, TotalTokens: 23},
				}},
			}},
		},
	})
	if got.Total.TotalTokens != 35 || len(got.ModelUsages) != 1 || got.ModelUsages[0].Overall.TotalTokens != 35 {
		t.Fatalf("summary = %#v", got)
	}
	if len(got.ModelUsages[0].EffortUsages) != 1 || got.ModelUsages[0].EffortUsages[0].Usage.TotalTokens != 35 {
		t.Fatalf("summary effort usage = %#v", got.ModelUsages[0].EffortUsages)
	}
}

func TestAggregateCodexTokenStatsCombinesSourcesAndResetsNativeLatest(t *testing.T) {
	old := CodexTokenStats{
		SourcePath:        "/tmp/old-parent.jsonl",
		Source:            "token_count",
		SourceLine:        4,
		Info:              CodexTokenUsageInfo{Last: CodexTokenUsage{InputTokens: 80, CachedInputTokens: 40, OutputTokens: 10, ReasoningOutputTokens: 5, TotalTokens: 90}, Total: CodexTokenUsage{InputTokens: 80, CachedInputTokens: 40, OutputTokens: 10, ReasoningOutputTokens: 5, TotalTokens: 90}, ModelContextWindow: 1000},
		NativeLatestTotal: CodexTokenUsage{TotalTokens: 90},
		UsageEventCount:   1,
		ModelUsages: []CodexModelUsage{{
			Model:        "gpt-5.5",
			Overall:      CodexTokenUsage{InputTokens: 80, CachedInputTokens: 40, OutputTokens: 10, ReasoningOutputTokens: 5, TotalTokens: 90},
			EffortUsages: []CodexEffortUsage{{Effort: "xhigh", Usage: CodexTokenUsage{InputTokens: 80, CachedInputTokens: 40, OutputTokens: 10, ReasoningOutputTokens: 5, TotalTokens: 90}}},
		}},
		ModelTierUsages: []CodexModelTierUsage{{Model: "gpt-5.5", Tier: "default", Usage: CodexTokenUsage{InputTokens: 80, CachedInputTokens: 40, OutputTokens: 10, TotalTokens: 90}}},
	}
	newer := CodexTokenStats{
		SourcePath:        "/tmp/new-parent.jsonl",
		Source:            "token_count",
		SourceLine:        7,
		Info:              CodexTokenUsageInfo{Last: CodexTokenUsage{InputTokens: 100, CachedInputTokens: 80, OutputTokens: 20, ReasoningOutputTokens: 10, TotalTokens: 120}, Total: CodexTokenUsage{InputTokens: 100, CachedInputTokens: 80, OutputTokens: 20, ReasoningOutputTokens: 10, TotalTokens: 120}, ModelContextWindow: 2000},
		NativeLatestTotal: CodexTokenUsage{TotalTokens: 120},
		UsageEventCount:   2,
		ModelUsages: []CodexModelUsage{{
			Model:        "gpt-5.6-luna",
			Overall:      CodexTokenUsage{InputTokens: 100, CachedInputTokens: 80, OutputTokens: 20, ReasoningOutputTokens: 10, TotalTokens: 120},
			EffortUsages: []CodexEffortUsage{{Effort: "max", Usage: CodexTokenUsage{InputTokens: 100, CachedInputTokens: 80, OutputTokens: 20, ReasoningOutputTokens: 10, TotalTokens: 120}}},
		}},
		ModelTierUsages: []CodexModelTierUsage{{Model: "gpt-5.6-luna", Tier: "priority", Usage: CodexTokenUsage{InputTokens: 100, CachedInputTokens: 80, OutputTokens: 20, TotalTokens: 120}}},
	}

	got := aggregateCodexTokenStats([]CodexTokenStats{old, newer}, []codexStatsParentSource{{FilePath: old.SourcePath}, {FilePath: newer.SourcePath}})
	if got.Source != "aggregated across 2 linked parent transcripts" || got.SourcePath != "" || len(got.SourcePaths) != 2 {
		t.Fatalf("aggregate metadata = %#v", got)
	}
	if got.Info.Total.InputTokens != 180 || got.Info.Total.CachedInputTokens != 120 || got.Info.Total.OutputTokens != 30 || got.Info.Total.TotalTokens != 210 {
		t.Fatalf("aggregate total = %#v", got.Info.Total)
	}
	if got.Info.Last != newer.Info.Last || got.Info.ModelContextWindow != 2000 || got.NativeLatestTotal.hasTokens() {
		t.Fatalf("aggregate latest metadata = %#v", got)
	}
	if got.UsageEventCount != 3 || got.UsedFallbackOnly {
		t.Fatalf("aggregate counters = %#v", got)
	}
	if len(got.ModelUsages) != 2 || len(got.ModelTierUsages) != 2 {
		t.Fatalf("aggregate model details = %#v tiers=%#v", got.ModelUsages, got.ModelTierUsages)
	}
}

func TestBridgeSubagentProjectDiscoveryCachesShortHistoryScan(t *testing.T) {
	previousDiscover := discoverCodexProjectsForTeams
	defer func() { discoverCodexProjectsForTeams = previousDiscover }()
	var calls int
	discoverCodexProjectsForTeams = func(context.Context, string) ([]codexhistory.Project, error) {
		calls++
		return []codexhistory.Project{{Path: "/tmp/project"}}, nil
	}
	bridge := &Bridge{scope: teamstore.ScopeIdentity{CodexHome: "/tmp/codex-home"}}
	first, err := bridge.discoverSubagentProjects(context.Background())
	if err != nil || len(first) != 1 {
		t.Fatalf("first discovery = %#v, err=%v", first, err)
	}
	second, err := bridge.discoverSubagentProjects(context.Background())
	if err != nil || len(second) != 1 {
		t.Fatalf("second discovery = %#v, err=%v", second, err)
	}
	if calls != 1 {
		t.Fatalf("history discovery calls = %d, want one cached scan", calls)
	}

	bridge.scope.CodexHome = "/tmp/other-codex-home"
	if _, err := bridge.discoverSubagentProjects(context.Background()); err != nil {
		t.Fatalf("discovery after root change: %v", err)
	}
	if calls != 2 {
		t.Fatalf("history discovery calls after root change = %d, want 2", calls)
	}
}

func TestParseCodexTokenStatsKeepsMissingModelTierMetadataUnknown(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"turn_context","payload":{"turn_id":"turn-1","model":"model-a"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12},"last_token_usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}}}}`,
		`{"type":"event_msg","payload":{"type":"thread_settings_applied","thread_settings":{"model":"model-a","service_tier":"default"}}}`,
		`{"type":"turn_context","payload":{"turn_id":"turn-2","model":"model-a"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":20,"output_tokens":4,"total_tokens":24},"last_token_usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}}}}`,
	}, "\n")

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	want := []CodexModelTierUsage{
		{Model: "model-a", Tier: "default", Usage: CodexTokenUsage{InputTokens: 10, OutputTokens: 2, TotalTokens: 12}},
		{Model: "model-a", Tier: "unknown", Usage: CodexTokenUsage{InputTokens: 10, OutputTokens: 2, TotalTokens: 12}},
	}
	if !reflect.DeepEqual(got.ModelTierUsages, want) {
		t.Fatalf("model/tier usages = %#v, want %#v", got.ModelTierUsages, want)
	}
	rendered := strings.Join(formatCodexTokenStatsLines(got), "\n")
	for _, wantText := range []string{
		"Model: model-a",
		"Tier: unknown",
		"1 combination(s), 12 total, have missing model or service-tier metadata; they remain `unknown` instead of being guessed or merged",
	} {
		if !strings.Contains(rendered, wantText) {
			t.Fatalf("unknown model/tier diagnostic missing %q:\n%s", wantText, rendered)
		}
	}
}

func TestParseCodexTokenStatsGroupsFallbackUsageByModelAndTier(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"thread_settings_applied","thread_settings":{"model":"model-a","service_tier":"default"}}}`,
		`{"type":"turn_context","payload":{"turn_id":"turn-1","model":"model-a"}}`,
		`{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}}`,
		`{"type":"event_msg","payload":{"type":"thread_settings_applied","thread_settings":{"model":"model-b","service_tier":"priority"}}}`,
		`{"type":"turn_context","payload":{"turn_id":"turn-2","model":"model-b"}}`,
		`{"type":"turn.completed","usage":{"input_tokens":20,"output_tokens":3,"total_tokens":23}}`,
	}, "\n")

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	want := []CodexModelTierUsage{
		{Model: "model-a", Tier: "default", Usage: CodexTokenUsage{InputTokens: 10, OutputTokens: 2, TotalTokens: 12}},
		{Model: "model-b", Tier: "priority", Usage: CodexTokenUsage{InputTokens: 20, OutputTokens: 3, TotalTokens: 23}},
	}
	if !reflect.DeepEqual(got.ModelTierUsages, want) {
		t.Fatalf("model/tier fallback usages = %#v, want %#v", got.ModelTierUsages, want)
	}
}

func TestParseCodexTokenStatsIncludesFallbackOnlyTurnsInModelOverall(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"event_msg","payload":{"type":"thread_settings_applied","thread_settings":{"model":"model-a","reasoning_effort":"high"}}}`,
		`{"type":"turn_context","payload":{"turn_id":"turn-1","model":"model-a","effort":"high"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":90,"output_tokens":10,"total_tokens":100},"last_token_usage":{"input_tokens":90,"output_tokens":10,"total_tokens":100}}}}`,
		`{"type":"turn_context","payload":{"turn_id":"turn-2","model":"model-a","effort":"high"}}`,
		`{"type":"turn.completed","usage":{"input_tokens":4,"output_tokens":1,"total_tokens":5}}`,
	}, "\n")

	got, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseCodexTokenStats error: %v", err)
	}
	if got.Info.Total.TotalTokens != 105 {
		t.Fatalf("conversation total = %d, want fallback-inclusive 105", got.Info.Total.TotalTokens)
	}
	if total := codexModelUsagesTotal(got.ModelUsages); total.TotalTokens != got.Info.Total.TotalTokens {
		t.Fatalf("model overall total = %d, conversation total = %d", total.TotalTokens, got.Info.Total.TotalTokens)
	}
}

func TestRenderCodexTokenStatsHTMLRendersModelTierUsageSafely(t *testing.T) {
	rendered := renderCodexTokenStatsHTML(strings.Join([]string{
		"STATS: Codex tokens",
		"Model/tier usage:",
		"Model: model<one>",
		"Tier: tier&one",
		"input: 100 (cached 80, non-cached 20)",
		"Cache hit rate: 80.0%",
		"output: 12 (reasoning 5)",
		"total: 112",
	}, "\n"))
	for _, want := range []string{
		"<p><strong>Model:</strong> model&lt;one&gt;<br><strong>Tier:</strong> tier&amp;one",
		"<strong>input:</strong> 100 (cached 80, non-cached 20)",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("model/tier HTML missing %q:\n%s", want, rendered)
		}
	}
	for _, forbidden := range []string{"model<one>", "tier&one"} {
		if strings.Contains(rendered, forbidden) {
			t.Fatalf("model/tier HTML did not escape %q:\n%s", forbidden, rendered)
		}
	}
}

func TestRenderCodexTokenStatsHTMLRendersModelEffortTableSafely(t *testing.T) {
	rendered := renderCodexTokenStatsHTML(strings.Join([]string{
		"STATS: Codex tokens",
		"Main agent token usage:",
		"Model/effort usage:",
		"Model: model<one>",
		"Overall:",
		"input: 130 (cached 90, non-cached 40)",
		"Cache hit rate: 69.2%",
		"output: 30 (reasoning 10)",
		"total: 160",
		"Effort: max&safe",
		"input: 50",
		"output: 10",
		"total: 60",
	}, "\n"))
	for _, want := range []string{
		"<p><strong>Main agent token usage:</strong></p>",
		"<p><strong>Model/effort usage:</strong></p>",
		"<table><tr><th>Model</th><th>Effort</th><th>input</th><th>Cache hit rate</th><th>output</th><th>total</th></tr>",
		"<tr><td>model&lt;one&gt;</td><td>overall</td><td>130 (cached 90, non-cached 40)</td><td>69.2%</td>",
		"<tr><td>model&lt;one&gt;</td><td>max&amp;safe</td>",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("model/effort HTML missing %q:\n%s", want, rendered)
		}
	}
	for _, forbidden := range []string{"model<one>", "max&safe"} {
		if strings.Contains(rendered, forbidden) {
			t.Fatalf("model/effort HTML did not escape %q:\n%s", forbidden, rendered)
		}
	}
}

func TestRenderCodexTokenStatsHTMLChunksLargeModelEffortTable(t *testing.T) {
	var input strings.Builder
	input.WriteString("STATS: Codex tokens\n")
	input.WriteString("🧠 MAIN AGENT · model/effort detail:\n")
	for index := 0; index < 420; index++ {
		fmt.Fprintf(&input, "Model: model-%03d\nOverall:\ninput: 130 (cached 90, non-cached 40)\nCache hit rate: 69.2%%\noutput: 30 (reasoning 10)\ntotal: 160\nEffort: high\ninput: 130 (cached 90, non-cached 40)\nCache hit rate: 69.2%%\noutput: 30 (reasoning 10)\ntotal: 160\n", index)
	}

	full := renderCodexTokenStatsHTML(input.String())
	if len(full) <= safeTeamsHTMLContentBytes {
		t.Fatalf("large stats fixture rendered to %d bytes, want it to exceed %d", len(full), safeTeamsHTMLContentBytes)
	}
	chunks := renderCodexTokenStatsHTMLChunks(input.String())
	if len(chunks) < 2 {
		t.Fatalf("large stats fixture produced %d chunk(s), want multiple", len(chunks))
	}
	hasModelTable := false
	for index, chunk := range chunks {
		if len(chunk) > teamsChunkHTMLContentBytes {
			t.Fatalf("chunk %d rendered to %d bytes, want <= %d", index+1, len(chunk), teamsChunkHTMLContentBytes)
		}
		if !strings.Contains(chunk, `<p><strong>🔧 Helper:</strong></p>`) {
			preview := chunk
			if len(preview) > 120 {
				preview = preview[:120]
			}
			t.Fatalf("chunk %d lost helper heading: %s", index+1, preview)
		}
		if strings.Contains(chunk, "<table>") {
			hasModelTable = true
			if !strings.Contains(chunk, `<table><tr><th>Model</th><th>Effort</th>`) {
				t.Fatalf("chunk %d lost model/effort table header", index+1)
			}
		}
		if got := renderOutboxHTML(teamstore.OutboxMessage{Kind: fmt.Sprintf("helper-stats-%03d", index+1), Body: chunk}); got != chunk {
			t.Fatalf("chunk %d was not preserved as trusted helper-stats HTML", index+1)
		}
	}
	if !hasModelTable {
		t.Fatal("chunked stats lost the model/effort table")
	}
	for _, want := range []string{"model-000", "model-199", "model-419", "Cache hit rate"} {
		found := false
		for _, chunk := range chunks {
			if strings.Contains(chunk, want) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("chunked stats lost %q", want)
		}
	}
}

func TestBridgeSendStatsToChatQueuesRenderedChunks(t *testing.T) {
	var input strings.Builder
	input.WriteString("STATS: Codex tokens\n🧠 MAIN AGENT · model/effort detail:\n")
	for index := 0; index < 420; index++ {
		fmt.Fprintf(&input, "Model: model-%03d\nOverall:\ninput: 130 (cached 90, non-cached 40)\nCache hit rate: 69.2%%\noutput: 30 (reasoning 10)\ntotal: 160\nEffort: high\ninput: 130 (cached 90, non-cached 40)\nCache hit rate: 69.2%%\noutput: 30 (reasoning 10)\ntotal: 160\n", index)
	}
	want := renderCodexTokenStatsHTMLChunks(input.String())
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, newBridgeTestStore(t), nil)
	if err := bridge.sendStatsToChat(context.Background(), "chat-1", input.String()); err != nil {
		t.Fatalf("sendStatsToChat error: %v", err)
	}
	if len(*sent) != len(want) || len(*sent) < 2 {
		t.Fatalf("sent %d message(s), want %d chunk(s)", len(*sent), len(want))
	}
	for index, message := range *sent {
		content := helperOutboxProvenanceMarkerPattern.ReplaceAllString(message.Content, "")
		if content != want[index] {
			gotPreview, wantPreview := content, want[index]
			if len(gotPreview) > 240 {
				gotPreview = gotPreview[:240]
			}
			if len(wantPreview) > 240 {
				wantPreview = wantPreview[:240]
			}
			t.Fatalf("sent chunk %d differs from queued trusted HTML:\ngot=%s\nwant=%s", index+1, gotPreview, wantPreview)
		}
		if len(message.Content) > safeTeamsHTMLContentBytes {
			t.Fatalf("sent chunk %d rendered to %d bytes, want <= %d", index+1, len(message.Content), safeTeamsHTMLContentBytes)
		}
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
		`{"type":"event_msg","payload":{"type":"thread_settings_applied","thread_settings":{"model":"gpt-5.6-sol","service_tier":"default"}}}` + "\n" +
		`{"type":"turn_context","payload":{"turn_id":"turn-1","model":"gpt-5.6-sol"}}` + "\n" +
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":300,"cached_input_tokens":120,"output_tokens":30,"reasoning_output_tokens":10,"total_tokens":330},"last_token_usage":{"input_tokens":200,"cached_input_tokens":100,"output_tokens":20,"reasoning_output_tokens":5,"total_tokens":220},"model_context_window":1000},"rate_limits":{"limit_id":"codex","plan_type":"business"}}}` + "\n" +
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":100,"cached_input_tokens":50,"output_tokens":10,"reasoning_output_tokens":3,"total_tokens":110},"last_token_usage":{"input_tokens":100,"cached_input_tokens":50,"output_tokens":10,"reasoning_output_tokens":3,"total_tokens":110},"model_context_window":1000}}}` + "\n" +
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":180,"cached_input_tokens":90,"output_tokens":20,"reasoning_output_tokens":5,"total_tokens":200},"last_token_usage":{"input_tokens":80,"cached_input_tokens":40,"output_tokens":10,"reasoning_output_tokens":2,"total_tokens":90},"model_context_window":1000}}}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(transcript), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}

	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{result: ExecutionResult{Text: "should not run"}})
	// Keep this no-subagent regression deterministic: an empty, valid Codex
	// history root means discovery succeeds and the stats view can report
	// NOT USED instead of depending on the host's real Codex home.
	codexHome := t.TempDir()
	if err := os.Mkdir(filepath.Join(codexHome, "sessions"), 0o700); err != nil {
		t.Fatalf("create empty Codex sessions directory: %v", err)
	}
	bridge.scope.CodexHome = codexHome
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
	for _, want := range []string{
		"STATS: Codex tokens",
		"Codex thread: thread-1",
		"🧠 MAIN AGENT · metadata:",
		"🧠 MAIN AGENT · snapshots:",
		"Last recorded model usage",
		"Conversation total",
		"🧠 MAIN AGENT · overall:",
		"🧠 MAIN AGENT · model/effort detail:",
		"gpt-5.6-sol",
		"total: 530",
		"Cache hit rate",
		"🧩 SUBAGENTS · NOT USED:",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("helper stats response missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "Model/tier usage:") {
		t.Fatalf("helper stats response retained the removed model/tier section:\n%s", got)
	}
	html := (*sent)[0].Content
	for _, want := range []string{
		"<p><strong>🔧 Helper:</strong></p>",
		"<p><strong>STATS: Codex tokens</strong></p>",
		"<strong>Session:</strong> s001<br><strong>Codex thread:</strong> thread-1",
		"<p><strong>🧠 MAIN AGENT · metadata:</strong></p>",
		"<p><strong>🧠 MAIN AGENT · snapshots:</strong></p>",
		"<table><tr><th>Metric</th><th>Last recorded model usage</th><th>Conversation total</th></tr>",
		"<tr><td><strong>input</strong></td><td>80 (cached 40, non-cached 40)</td><td>480 (cached 210, non-cached 270)</td></tr>",
		"<tr><td><strong>Cache hit rate</strong></td><td>50.0%</td><td>43.8%</td></tr>",
		"<tr><td><strong>output</strong></td><td>10 (reasoning 2)</td><td>50 (reasoning 15)</td></tr>",
		"<tr><td><strong>total</strong></td><td>90</td><td>530</td></tr>",
		"<p><strong>🧠 MAIN AGENT · overall:</strong></p>",
		"<table><tr><th>Metric</th><th>Overall</th></tr>",
		"<p><strong>🧠 MAIN AGENT · model/effort detail:</strong></p>",
		"<table><tr><th>Model</th><th>Effort</th><th>input</th><th>Cache hit rate</th><th>output</th><th>total</th></tr>",
		"<tr><td>gpt-5.6-sol</td><td>unknown</td>",
		"<p><strong>🧠 MAIN AGENT · analysis:</strong></p>",
		"<ul><li><strong>model context window:</strong> 1,000; current context uses 9.0%; approx remaining: 910</li>",
		"<li><strong>native latest cumulative total:</strong> 200; reconstructed conversation total: 530</li>",
		"<strong>Aggregation:</strong> ignored 0 non-advancing usage snapshot(s); observed 1 native cumulative counter reset(s) and 0 recovery event(s).",
		"<p><strong>🧩 SUBAGENTS · NOT USED:</strong></p>",
		"<table><tr><th>Status</th><th>Value</th></tr>",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("helper stats HTML missing paragraph %q:\n%s", want, html)
		}
	}
	if strings.Contains(html, "Model/tier usage:") {
		t.Fatalf("helper stats HTML retained the removed model/tier section:\n%s", html)
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

func TestBridgeWorkHelperStatsCombinesUserVisibleSubagentsByModelAndEffort(t *testing.T) {
	tempDir := t.TempDir()
	parentPath := filepath.Join(tempDir, "parent.jsonl")
	childMaxPath := filepath.Join(tempDir, "child-max.jsonl")
	childHighPath := filepath.Join(tempDir, "child-high.jsonl")
	writeTranscript := func(path string, model string, effort string, total int) {
		t.Helper()
		transcript := fmt.Sprintf(`{"type":"event_msg","payload":{"type":"thread_settings_applied","thread_settings":{"model":%q,"reasoning_effort":%q}}}`, model, effort) + "\n" +
			fmt.Sprintf(`{"type":"turn_context","payload":{"turn_id":%q,"model":%q,"effort":%q}}`, path, model, effort) + "\n" +
			fmt.Sprintf(`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":%d,"output_tokens":2,"total_tokens":%d},"last_token_usage":{"input_tokens":%d,"output_tokens":2,"total_tokens":%d}}}}`, total-2, total, total-2, total)
		if err := os.WriteFile(path, []byte(transcript), 0o600); err != nil {
			t.Fatalf("write transcript %s: %v", path, err)
		}
	}
	writeTranscript(parentPath, "parent-model", "high", 12)
	writeTranscript(childMaxPath, "model-a", "max", 23)
	writeTranscript(childHighPath, "model-a", "high", 34)

	previousDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(context.Context, string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{Path: tempDir, Sessions: []codexhistory.Session{{
			SessionID: "thread-parent",
			FilePath:  parentPath,
			Subagents: []codexhistory.SubagentSession{
				{SessionID: "child-max", Summary: "child-max", FilePath: childMaxPath},
				{SessionID: "child-high", Summary: "child-high", FilePath: childHighPath},
			},
		}}}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = previousDiscover })

	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{result: ExecutionResult{Text: "should not run"}})
	session := bridge.reg.SessionByChatID("chat-1")
	session.CodexThreadID = "thread-parent"
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		state.ImportCheckpoints[transcriptCheckpointID(session.ID)] = teamstore.ImportCheckpoint{
			ID:         transcriptCheckpointID(session.ID),
			SessionID:  session.ID,
			SourcePath: parentPath,
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
	for _, want := range []string{
		"🧠 MAIN AGENT · overall:",
		"🧩 SUBAGENTS (2) · overall:",
		"🧩 SUBAGENTS (2) · model/effort detail:",
		"model-a",
		"high",
		"max",
		"57",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("combined helper stats missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "child-max") || strings.Contains(got, "child-high") {
		t.Fatalf("helper stats exposed individual subagent details instead of combined usage:\n%s", got)
	}
	if strings.Contains(got, "Subagent token usage (all 2 combined):") {
		t.Fatalf("helper stats retained the old mixed subagent section:\n%s", got)
	}
	html := (*sent)[0].Content
	for _, want := range []string{
		"<p><strong>🧠 MAIN AGENT · overall:</strong></p>",
		"<p><strong>🧩 SUBAGENTS (2) · overall:</strong></p>",
		"<p><strong>🧩 SUBAGENTS (2) · model/effort detail:</strong></p>",
		"<table><tr><th>Model</th><th>Effort</th><th>input</th><th>Cache hit rate</th><th>output</th><th>total</th></tr>",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("combined helper stats HTML missing %q:\n%s", want, html)
		}
	}
}

func TestBridgeWorkHelperStatsAggregatesLinkedParentTranscriptsAfterModelSwitch(t *testing.T) {
	tempDir := t.TempDir()
	oldParentPath := filepath.Join(tempDir, "old-parent.jsonl")
	newParentPath := filepath.Join(tempDir, "new-parent.jsonl")
	childAPath := filepath.Join(tempDir, "child-a.jsonl")
	childBPath := filepath.Join(tempDir, "child-b.jsonl")
	writeTranscript := func(path string, threadID string, model string, effort string, input int, cached int, output int) {
		t.Helper()
		total := input + output
		transcript := fmt.Sprintf(`{"type":"session_meta","payload":{"id":%q}}`, threadID) + "\n" +
			fmt.Sprintf(`{"type":"event_msg","payload":{"type":"thread_settings_applied","thread_settings":{"model":%q,"reasoning_effort":%q}}}`, model, effort) + "\n" +
			fmt.Sprintf(`{"type":"turn_context","payload":{"turn_id":%q,"model":%q,"effort":%q}}`, threadID+"-turn", model, effort) + "\n" +
			fmt.Sprintf(`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":%d,"cached_input_tokens":%d,"output_tokens":%d,"reasoning_output_tokens":%d,"total_tokens":%d},"last_token_usage":{"input_tokens":%d,"cached_input_tokens":%d,"output_tokens":%d,"reasoning_output_tokens":%d,"total_tokens":%d}}}}`, input, cached, output, output/2, total, input, cached, output, output/2, total)
		if err := os.WriteFile(path, []byte(transcript), 0o600); err != nil {
			t.Fatalf("write transcript %s: %v", path, err)
		}
	}
	writeTranscript(oldParentPath, "thread-old", "gpt-5.5", "xhigh", 80, 40, 10)
	writeTranscript(newParentPath, "thread-new", "gpt-5.6-luna", "max", 100, 80, 20)
	writeTranscript(childAPath, "child-a", "gpt-5.6-sol", "high", 10, 5, 3)
	writeTranscript(childBPath, "child-b", "gpt-5.6-sol", "max", 12, 6, 5)

	previousDiscover := discoverCodexProjectsForTeams
	var discoveryCalls int
	discoverCodexProjectsForTeams = func(context.Context, string) ([]codexhistory.Project, error) {
		discoveryCalls++
		now := time.Now()
		sessions := []codexhistory.Session{
			{
				SessionID:  "thread-old",
				FilePath:   oldParentPath,
				ModifiedAt: now.Add(-time.Hour),
				Subagents: []codexhistory.SubagentSession{
					{SessionID: "child-a", FilePath: childAPath, Summary: "child-a"},
				},
			},
			{
				SessionID:  "thread-new",
				FilePath:   newParentPath,
				ModifiedAt: now,
				Subagents: []codexhistory.SubagentSession{
					// The same child is visible from both parent transcripts after
					// discovery; stats must count it only once.
					{SessionID: "child-a", FilePath: childAPath, Summary: "child-a"},
					{SessionID: "child-b", FilePath: childBPath, Summary: "child-b"},
				},
			},
		}
		if discoveryCalls == 1 {
			// Simulate the 30-second catalog cache racing the latest model-switch
			// thread; the stats resolver must perform one targeted refresh.
			sessions = sessions[:1]
		}
		return []codexhistory.Project{{Path: tempDir, Sessions: sessions}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = previousDiscover })

	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{result: ExecutionResult{Text: "should not run"}})
	session := bridge.reg.SessionByChatID("chat-1")
	session.CodexThreadID = "thread-new"
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}
	now := time.Now()
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		state.ImportCheckpoints[transcriptCheckpointID(session.ID)] = teamstore.ImportCheckpoint{
			ID:         transcriptCheckpointID(session.ID),
			SessionID:  session.ID,
			SourcePath: oldParentPath,
			Status:     importCheckpointStatusComplete,
		}
		state.Turns["turn-old"] = teamstore.Turn{
			ID: "turn-old", SessionID: session.ID, CodexThreadID: "thread-old",
			Status: teamstore.TurnStatusCompleted, CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour),
		}
		state.Turns["turn-new"] = teamstore.Turn{
			ID: "turn-new", SessionID: session.ID, CodexThreadID: "thread-new",
			Status: teamstore.TurnStatusCompleted, CreatedAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed checkpoint and prior turns: %v", err)
	}

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessageWithText("helper-stats", "helper stats"), "helper stats"); err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if len(*sent) != 1 {
		t.Fatalf("sent = %d, want 1", len(*sent))
	}
	if discoveryCalls != 2 {
		t.Fatalf("history discovery calls = %d, want cached scan plus one refresh", discoveryCalls)
	}
	got := PlainTextFromTeamsHTML((*sent)[0].Content)
	for _, want := range []string{
		"Source: aggregated across 2 linked parent transcripts",
		"Sources: 2 linked parent transcripts",
		"gpt-5.5",
		"gpt-5.6-luna",
		"xhigh",
		"max",
		"180 (cached 120, non-cached 60)",
		"66.7%",
		"total\t210",
		"🧩 SUBAGENTS (2) · overall:",
		"gpt-5.6-sol",
		"total\t30",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("model-switch helper stats missing %q:\n%s", want, got)
		}
	}
	if strings.Count(got, "child-a.jsonl") != 0 || strings.Count(got, "child-b.jsonl") != 0 {
		t.Fatalf("helper stats exposed individual subagent transcript paths:\n%s", got)
	}
	html := (*sent)[0].Content
	for _, want := range []string{
		"<strong>Source aggregation:</strong> usage and model/effort details are combined across the linked parent transcripts; latest context and rate-limit metadata comes from the newest transcript.",
		"<p><strong>🧩 SUBAGENTS (2) · model/effort detail:</strong></p>",
		"<table><tr><th>Model</th><th>Effort</th><th>input</th><th>Cache hit rate</th><th>output</th><th>total</th></tr>",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("model-switch helper stats HTML missing %q:\n%s", want, html)
		}
	}
}
