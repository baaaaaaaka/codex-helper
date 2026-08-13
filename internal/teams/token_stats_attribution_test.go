package teams

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
)

func writeCumulativeTokenTranscript(t *testing.T, path string, totals ...int64) {
	t.Helper()
	lines := make([]string, 0, len(totals))
	for index, total := range totals {
		lines = append(lines, fmt.Sprintf(
			`{"timestamp":"2026-08-13T00:00:%02dZ","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":%d},"last_token_usage":{"total_tokens":%d}}}}`,
			index, total, total,
		))
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o600); err != nil {
		t.Fatalf("write transcript %s: %v", path, err)
	}
}

type testTokenUsageSnapshot struct {
	Usage  CodexTokenUsage
	Model  string
	Effort string
}

func writeCumulativeUsageTranscript(t *testing.T, path string, snapshots ...testTokenUsageSnapshot) {
	t.Helper()
	lines := make([]string, 0, len(snapshots)*2)
	for index, snapshot := range snapshots {
		timestamp := fmt.Sprintf("2026-08-13T00:01:%02dZ", index)
		if snapshot.Model != "" || snapshot.Effort != "" {
			lines = append(lines, fmt.Sprintf(
				`{"timestamp":%q,"type":"turn_context","payload":{"turn_id":"turn-%d","model":%q,"effort":%q}}`,
				timestamp, index, snapshot.Model, snapshot.Effort,
			))
		}
		last := snapshot.Usage
		if index > 0 {
			last = subtractCodexTokenUsage(snapshot.Usage, snapshots[index-1].Usage)
		}
		lines = append(lines, fmt.Sprintf(
			`{"timestamp":%q,"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":%d,"cached_input_tokens":%d,"output_tokens":%d,"reasoning_output_tokens":%d,"total_tokens":%d},"last_token_usage":{"input_tokens":%d,"cached_input_tokens":%d,"output_tokens":%d,"reasoning_output_tokens":%d,"total_tokens":%d}}}}`,
			timestamp,
			snapshot.Usage.InputTokens,
			snapshot.Usage.CachedInputTokens,
			snapshot.Usage.OutputTokens,
			snapshot.Usage.ReasoningOutputTokens,
			snapshot.Usage.TotalTokens,
			last.InputTokens,
			last.CachedInputTokens,
			last.OutputTokens,
			last.ReasoningOutputTokens,
			last.TotalTokens,
		))
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o600); err != nil {
		t.Fatalf("write transcript %s: %v", path, err)
	}
}

func TestAttributeCodexChildTokenStatsRemovesInheritedPrefix(t *testing.T) {
	parentPath := filepath.Join(t.TempDir(), "parent.jsonl")
	childPath := filepath.Join(filepath.Dir(parentPath), "child.jsonl")
	writeCumulativeTokenTranscript(t, parentPath, 10, 20)
	writeCumulativeTokenTranscript(t, childPath, 10, 20, 35)

	parent, err := ReadCodexTokenStats(parentPath)
	if err != nil {
		t.Fatalf("read parent: %v", err)
	}
	child, err := ReadCodexTokenStats(childPath)
	if err != nil {
		t.Fatalf("read child: %v", err)
	}
	for index := range child.usageTimeline {
		child.usageTimeline[index].Timestamp = child.usageTimeline[index].Timestamp.Add(24 * time.Hour)
	}
	adjusted, err := attributeCodexChildTokenStats(parent, child)
	if err != nil {
		t.Fatalf("attribute child: %v", err)
	}
	if adjusted.Info.Total.TotalTokens != 15 || adjusted.Info.Last.TotalTokens != 15 {
		t.Fatalf("adjusted usage = %#v, want child-only total 15", adjusted.Info)
	}
	if adjusted.NativeLatestTotal.TotalTokens != 15 || adjusted.UsageEventCount != 1 {
		t.Fatalf("adjusted metadata = %#v, want child-only latest and one update", adjusted)
	}
}

func TestAttributeCodexChildTokenStatsReturnsZeroForInheritedOnlyChild(t *testing.T) {
	parentPath := filepath.Join(t.TempDir(), "parent.jsonl")
	childPath := filepath.Join(filepath.Dir(parentPath), "child.jsonl")
	writeCumulativeTokenTranscript(t, parentPath, 10, 20)
	writeCumulativeTokenTranscript(t, childPath, 10, 20)

	parent, err := ReadCodexTokenStats(parentPath)
	if err != nil {
		t.Fatalf("read parent: %v", err)
	}
	child, err := ReadCodexTokenStats(childPath)
	if err != nil {
		t.Fatalf("read child: %v", err)
	}
	adjusted, err := attributeCodexChildTokenStats(parent, child)
	if err != nil {
		t.Fatalf("attribute inherited-only child: %v", err)
	}
	if adjusted.HasUsage() || adjusted.Info.Total != (CodexTokenUsage{}) || adjusted.Info.Last != (CodexTokenUsage{}) {
		t.Fatalf("inherited-only child retained usage: %#v", adjusted)
	}
	if adjusted.UsageEventCount != 0 || adjusted.NativeLatestTotal.hasTokens() {
		t.Fatalf("inherited-only child retained update metadata: %#v", adjusted)
	}
}

func TestAttributeCodexChildTokenStatsRebuildsAllUsageComponentsAndModelEffort(t *testing.T) {
	tempDir := t.TempDir()
	parentPath := filepath.Join(tempDir, "parent.jsonl")
	childPath := filepath.Join(tempDir, "child.jsonl")
	prefix := []testTokenUsageSnapshot{
		{Usage: CodexTokenUsage{InputTokens: 100, CachedInputTokens: 80, OutputTokens: 10, ReasoningOutputTokens: 4, TotalTokens: 110}, Model: "parent-model", Effort: "high"},
		{Usage: CodexTokenUsage{InputTokens: 160, CachedInputTokens: 120, OutputTokens: 18, ReasoningOutputTokens: 7, TotalTokens: 178}, Model: "parent-model", Effort: "high"},
	}
	childSnapshots := append(append([]testTokenUsageSnapshot{}, prefix...), testTokenUsageSnapshot{
		Usage:  CodexTokenUsage{InputTokens: 220, CachedInputTokens: 170, OutputTokens: 25, ReasoningOutputTokens: 10, TotalTokens: 245},
		Model:  "child-model",
		Effort: "max",
	})
	writeCumulativeUsageTranscript(t, parentPath, prefix...)
	writeCumulativeUsageTranscript(t, childPath, childSnapshots...)

	parent, err := ReadCodexTokenStats(parentPath)
	if err != nil {
		t.Fatalf("read parent: %v", err)
	}
	child, err := ReadCodexTokenStats(childPath)
	if err != nil {
		t.Fatalf("read child: %v", err)
	}
	adjusted, err := attributeCodexChildTokenStats(parent, child)
	if err != nil {
		t.Fatalf("attribute child: %v", err)
	}
	want := CodexTokenUsage{InputTokens: 60, CachedInputTokens: 50, OutputTokens: 7, ReasoningOutputTokens: 3, TotalTokens: 67}
	if adjusted.Info.Total != want || adjusted.Info.Last != want || adjusted.NativeLatestTotal != want {
		t.Fatalf("adjusted usage = %#v, want %#v in all cumulative fields", adjusted, want)
	}
	if len(adjusted.ModelUsages) != 1 || adjusted.ModelUsages[0].Model != "child-model" {
		t.Fatalf("model usage = %#v, want only child-model", adjusted.ModelUsages)
	}
	if len(adjusted.ModelUsages[0].EffortUsages) != 1 || adjusted.ModelUsages[0].EffortUsages[0].Effort != "max" || adjusted.ModelUsages[0].EffortUsages[0].Usage != want {
		t.Fatalf("effort usage = %#v, want child max usage %#v", adjusted.ModelUsages[0].EffortUsages, want)
	}
	if len(adjusted.ModelTierUsages) != 1 || adjusted.ModelTierUsages[0].Model != "child-model" || adjusted.ModelTierUsages[0].Usage != want {
		t.Fatalf("model-tier usage = %#v, want child-model usage %#v", adjusted.ModelTierUsages, want)
	}
}

func TestAttributeCodexChildTokenStatsIgnoresRepeatedSnapshotsAndPreservesChildResetEpochs(t *testing.T) {
	parentPath := filepath.Join(t.TempDir(), "parent.jsonl")
	childPath := filepath.Join(filepath.Dir(parentPath), "child.jsonl")
	writeCumulativeTokenTranscript(t, parentPath, 100, 160)
	writeCumulativeTokenTranscript(t, childPath, 100, 160, 200, 30, 30, 50)
	parent, err := ReadCodexTokenStats(parentPath)
	if err != nil {
		t.Fatalf("read parent: %v", err)
	}
	child, err := ReadCodexTokenStats(childPath)
	if err != nil {
		t.Fatalf("read child: %v", err)
	}
	adjusted, err := attributeCodexChildTokenStats(parent, child)
	if err != nil {
		t.Fatalf("attribute child: %v", err)
	}
	if adjusted.Info.Total.TotalTokens != 90 || adjusted.Info.Last.TotalTokens != 20 {
		t.Fatalf("reset-adjusted usage = %#v, want total 90 and final delta 20", adjusted.Info)
	}
	if adjusted.UsageEventCount != 3 || adjusted.NonAdvancingUsageEvents != 1 || adjusted.NativeCounterResets != 1 {
		t.Fatalf("reset-adjusted metadata = %#v, want three child updates, one duplicate, one reset", adjusted)
	}
}

func TestAttributeCodexChildTokenStatsKeepsIndependentCounterWhenPrefixIsEmpty(t *testing.T) {
	parentPath := filepath.Join(t.TempDir(), "parent.jsonl")
	childPath := filepath.Join(filepath.Dir(parentPath), "child.jsonl")
	writeCumulativeTokenTranscript(t, parentPath, 10, 100)
	writeCumulativeTokenTranscript(t, childPath, 30, 45)

	parent, err := ReadCodexTokenStats(parentPath)
	if err != nil {
		t.Fatalf("read parent: %v", err)
	}
	child, err := ReadCodexTokenStats(childPath)
	if err != nil {
		t.Fatalf("read child: %v", err)
	}
	child.usageTimeline[0].Timestamp = parent.usageTimeline[len(parent.usageTimeline)-1].Timestamp.Add(1)
	adjusted, err := attributeCodexChildTokenStats(parent, child)
	if err != nil {
		t.Fatalf("attribute independent child: %v", err)
	}
	if adjusted.Info.Total.TotalTokens != 45 {
		t.Fatalf("independent child usage = %#v, want local total 45", adjusted.Info.Total)
	}
}

func TestAttributeCodexChildTokenStatsRejectsLastUsageWithoutCumulativeTotal(t *testing.T) {
	parentPath := filepath.Join(t.TempDir(), "parent.jsonl")
	childPath := filepath.Join(filepath.Dir(parentPath), "child.jsonl")
	transcript := `{"type":"event_msg","payload":{"type":"token_count","info":{"last_token_usage":{"total_tokens":10}}}}`
	if err := os.WriteFile(parentPath, []byte(transcript), 0o600); err != nil {
		t.Fatalf("write parent transcript: %v", err)
	}
	if err := os.WriteFile(childPath, []byte(transcript), 0o600); err != nil {
		t.Fatalf("write child transcript: %v", err)
	}
	parent, err := ReadCodexTokenStats(parentPath)
	if err != nil {
		t.Fatalf("read parent: %v", err)
	}
	child, err := ReadCodexTokenStats(childPath)
	if err != nil {
		t.Fatalf("read child: %v", err)
	}
	if _, err := attributeCodexChildTokenStats(parent, child); err == nil {
		t.Fatal("last_token_usage-only transcript was accepted for native attribution")
	}
}

func TestAttributeCodexChildTokenStatsRejectsUnprovenZeroLengthPrefix(t *testing.T) {
	parentPath := filepath.Join(t.TempDir(), "parent.jsonl")
	childPath := filepath.Join(filepath.Dir(parentPath), "child.jsonl")
	writeCumulativeTokenTranscript(t, parentPath, 10, 100)
	writeCumulativeTokenTranscript(t, childPath, 120, 150)
	parent, err := ReadCodexTokenStats(parentPath)
	if err != nil {
		t.Fatalf("read parent: %v", err)
	}
	child, err := ReadCodexTokenStats(childPath)
	if err != nil {
		t.Fatalf("read child: %v", err)
	}
	child.usageTimeline[0].Timestamp = parent.usageTimeline[len(parent.usageTimeline)-1].Timestamp.Add(1)
	if _, err := attributeCodexChildTokenStats(parent, child); err == nil {
		t.Fatal("zero-length prefix with child counter above parent was accepted")
	}
}

func TestAttributeCodexChildTokenStatsRejectsMixedCumulativeAndFallbackTimeline(t *testing.T) {
	input := strings.Join([]string{
		`{"timestamp":"2026-08-13T00:00:00Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":10},"last_token_usage":{"total_tokens":10}}}}`,
		`{"timestamp":"2026-08-13T00:00:01Z","type":"turn.completed","usage":{"total_tokens":5}}`,
	}, "\n")
	parent, err := ParseCodexTokenStats(strings.NewReader(input[:strings.Index(input, "\n")]))
	if err != nil {
		t.Fatalf("parse parent: %v", err)
	}
	child, err := ParseCodexTokenStats(strings.NewReader(input))
	if err != nil {
		t.Fatalf("parse child: %v", err)
	}
	if _, err := attributeCodexChildTokenStats(parent, child); err == nil {
		t.Fatal("mixed cumulative/fallback child timeline was accepted")
	}
}

func TestReadSubagentTokenStatsAttributesNestedNativeChildrenToImmediateParents(t *testing.T) {
	tempDir := t.TempDir()
	parentPath := filepath.Join(tempDir, "parent.jsonl")
	childPath := filepath.Join(tempDir, "child.jsonl")
	grandchildPath := filepath.Join(tempDir, "grandchild.jsonl")
	writeCumulativeTokenTranscript(t, parentPath, 10, 20)
	writeCumulativeTokenTranscript(t, childPath, 10, 20, 35)
	writeCumulativeTokenTranscript(t, grandchildPath, 10, 20, 35, 50)

	child := codexhistory.SubagentSession{
		AgentID:         "thread_spawn",
		ParentSessionID: "root",
		SessionID:       "child",
		Summary:         "child",
		FilePath:        childPath,
	}
	projects := []codexhistory.Project{{
		Path: tempDir,
		Sessions: []codexhistory.Session{
			{SessionID: "root", FilePath: parentPath, Subagents: []codexhistory.SubagentSession{child}},
			{SessionID: "grandchild", ParentSessionID: "child", AgentID: "thread_spawn", Summary: "grandchild", FilePath: grandchildPath},
			{SessionID: "unrelated-root", Subagents: []codexhistory.SubagentSession{{SessionID: "unrelated-child", ParentSessionID: "unrelated-root", AgentID: "thread_spawn", Summary: "unrelated", FilePath: filepath.Join(tempDir, "does-not-exist.jsonl")}}},
		},
	}}
	parents := []codexStatsParentSource{{ThreadID: "root", FilePath: parentPath, Subagents: []codexhistory.SubagentSession{child}}}

	summary, count, problems, err := (&Bridge{}).readSubagentTokenStatsForWorkSession(context.Background(), parents, projects, nil)
	if err != nil {
		t.Fatalf("read subagent stats: %v", err)
	}
	if count != 2 || len(problems) != 0 {
		t.Fatalf("subagent attribution metadata = count %d problems %#v", count, problems)
	}
	if summary.Total.TotalTokens != 30 {
		t.Fatalf("nested child summary = %#v, want 15 + 15", summary.Total)
	}
	rendered := strings.Join(formatCodexSubagentStatsLines(summary, count, problems), "\n")
	if !strings.Contains(rendered, "🧩 SUBAGENTS (2) · overall:") || !strings.Contains(rendered, "total: 30") || strings.Contains(rendered, "total: 65") {
		t.Fatalf("nested child report = %s", rendered)
	}
}

func TestReadSubagentTokenStatsFailsClosedWhenNativePrefixCannotBeProven(t *testing.T) {
	tempDir := t.TempDir()
	parentPath := filepath.Join(tempDir, "parent.jsonl")
	childPath := filepath.Join(tempDir, "child.jsonl")
	if err := os.WriteFile(parentPath, []byte(`{"type":"session_meta","payload":{"id":"root"}}`), 0o600); err != nil {
		t.Fatalf("write parent transcript: %v", err)
	}
	writeCumulativeTokenTranscript(t, childPath, 99, 120)
	child := codexhistory.SubagentSession{
		AgentID:         "thread_spawn",
		ParentSessionID: "root",
		SessionID:       "child",
		Summary:         "child",
		FilePath:        childPath,
	}
	parents := []codexStatsParentSource{{ThreadID: "root", FilePath: parentPath, Subagents: []codexhistory.SubagentSession{child}}}

	summary, count, problems, err := (&Bridge{}).readSubagentTokenStatsForWorkSession(context.Background(), parents, nil, nil)
	if err != nil {
		t.Fatalf("read subagent stats: %v", err)
	}
	if count != 1 || len(problems) != 1 {
		t.Fatalf("fail-closed metadata = count %d problems %#v", count, problems)
	}
	if summary.Total.hasTokens() {
		t.Fatalf("unproven native child produced a token number: %#v", summary.Total)
	}
}

func TestReadSubagentTokenStatsFailsClosedInsteadOfReturningPartialSum(t *testing.T) {
	tempDir := t.TempDir()
	parentPath := filepath.Join(tempDir, "parent.jsonl")
	goodPath := filepath.Join(tempDir, "good-child.jsonl")
	badPath := filepath.Join(tempDir, "bad-child.jsonl")
	writeCumulativeTokenTranscript(t, parentPath, 10, 20)
	writeCumulativeTokenTranscript(t, goodPath, 10, 20, 35)
	writeCumulativeTokenTranscript(t, badPath, 99, 120)
	children := []codexhistory.SubagentSession{
		{AgentID: "thread_spawn", ParentSessionID: "root", SessionID: "good", Summary: "good", FilePath: goodPath},
		{AgentID: "thread_spawn", ParentSessionID: "root", SessionID: "bad", Summary: "bad", FilePath: badPath},
	}
	parents := []codexStatsParentSource{{ThreadID: "root", FilePath: parentPath, Subagents: children}}
	projects := []codexhistory.Project{{Path: tempDir, Sessions: []codexhistory.Session{{SessionID: "root", FilePath: parentPath, Subagents: children}}}}

	summary, count, problems, err := (&Bridge{}).readSubagentTokenStatsForWorkSession(context.Background(), parents, projects, nil)
	if err != nil {
		t.Fatalf("read subagent stats: %v", err)
	}
	if count != 2 || len(problems) != 1 {
		t.Fatalf("partial attribution metadata = count %d problems %#v", count, problems)
	}
	if summary.Total.hasTokens() {
		t.Fatalf("partial child sum was returned: %#v", summary.Total)
	}
	rendered := strings.Join(formatCodexSubagentStatsLines(summary, count, problems), "\n")
	if !strings.Contains(rendered, "Token usage unavailable") || strings.Contains(rendered, "total\t15") {
		t.Fatalf("fail-closed report = %s", rendered)
	}
}

func TestCombineCodexTokenUsageSummariesIncludesMainAndSubagentModelEffortUsage(t *testing.T) {
	mainUsage := CodexTokenUsage{InputTokens: 100, CachedInputTokens: 80, OutputTokens: 20, ReasoningOutputTokens: 5, TotalTokens: 120}
	subagentUsage := CodexTokenUsage{InputTokens: 60, CachedInputTokens: 30, OutputTokens: 10, ReasoningOutputTokens: 3, TotalTokens: 70}
	combined := combineCodexTokenUsageSummaries(
		codexTokenUsageSummary{
			Total: mainUsage,
			ModelUsages: []CodexModelUsage{{
				Model:   "main-model",
				Overall: mainUsage,
				EffortUsages: []CodexEffortUsage{{
					Effort: "high",
					Usage:  mainUsage,
				}},
			}},
		},
		codexTokenUsageSummary{
			Total: subagentUsage,
			ModelUsages: []CodexModelUsage{{
				Model:   "subagent-model",
				Overall: subagentUsage,
				EffortUsages: []CodexEffortUsage{{
					Effort: "max",
					Usage:  subagentUsage,
				}},
			}},
		},
	)
	want := CodexTokenUsage{InputTokens: 160, CachedInputTokens: 110, OutputTokens: 30, ReasoningOutputTokens: 8, TotalTokens: 190}
	if combined.Total != want {
		t.Fatalf("combined total = %#v, want %#v", combined.Total, want)
	}
	if len(combined.ModelUsages) != 2 {
		t.Fatalf("combined model usage = %#v, want main and subagent models", combined.ModelUsages)
	}
	rendered := strings.Join(formatCodexTotalStatsLines(combined, true), "\n")
	for _, wantText := range []string{
		"🧮 TOTAL (MAIN + SUBAGENTS) · overall:",
		"input: 160 (cached 110, non-cached 50)",
		"output: 30 (reasoning 8)",
		"total: 190",
		"🧮 TOTAL (MAIN + SUBAGENTS) · model/effort detail:",
		"main-model",
		"subagent-model",
		"high",
		"max",
	} {
		if !strings.Contains(rendered, wantText) {
			t.Fatalf("combined total report missing %q:\n%s", wantText, rendered)
		}
	}
}

func TestFormatCodexTotalStatsLinesFailsClosedWhenUsageIsIncomplete(t *testing.T) {
	summary := codexTokenUsageSummary{Total: CodexTokenUsage{TotalTokens: 190}}
	rendered := strings.Join(formatCodexTotalStatsLines(summary, false), "\n")
	if !strings.Contains(rendered, "Token usage unavailable") || strings.Contains(rendered, "190") {
		t.Fatalf("incomplete total report exposed a number: %s", rendered)
	}
}

func BenchmarkParseCodexTokenStatsCumulativeLedger(b *testing.B) {
	const snapshotCount = 2000
	var builder strings.Builder
	for index := 0; index < snapshotCount; index++ {
		timestamp := time.Unix(int64(index), 0).UTC().Format(time.RFC3339)
		total := int64(index+1) * 100
		fmt.Fprintf(&builder,
			`{"timestamp":%q,"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":%d,"output_tokens":%d,"total_tokens":%d},"last_token_usage":{"input_tokens":100,"output_tokens":0,"total_tokens":100}}}}`+"\n",
			timestamp, total, 0, total,
		)
	}
	input := builder.String()
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		stats, err := ParseCodexTokenStats(strings.NewReader(input))
		if err != nil {
			b.Fatal(err)
		}
		if stats.UsageEventCount != snapshotCount {
			b.Fatalf("usage event count = %d, want %d", stats.UsageEventCount, snapshotCount)
		}
	}
}
