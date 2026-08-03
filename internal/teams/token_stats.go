package teams

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const codexContextBaselineTokens int64 = 12000

type CodexTokenStats struct {
	SourcePath               string
	SourceLine               int
	Source                   string
	Info                     CodexTokenUsageInfo
	ModelTierUsages          []CodexModelTierUsage
	ModelUsages              []CodexModelUsage
	NativeLatestTotal        CodexTokenUsage
	UsageEventCount          int
	NonAdvancingUsageEvents  int
	NativeCounterResets      int
	NativeCounterRecoveries  int
	MissingLastUsageEvents   int
	UsageAggregationOverflow bool
	RateLimits               CodexRateLimits
	Diagnostics              []TokenStatsDiagnostic
	UsedFallbackOnly         bool
}

// CodexModelTierUsage is the token usage attributed to one model/service-tier
// combination observed in a Codex transcript.
type CodexModelTierUsage struct {
	Model string
	Tier  string
	Usage CodexTokenUsage
}

// CodexModelUsage is the usage attributed to one model. Overall is the sum of
// all observed service tiers and effort levels for the model, while
// EffortUsages preserves the per-effort breakdown. The legacy
// ModelTierUsages view keeps service-tier details separately.
type CodexModelUsage struct {
	Model        string
	Overall      CodexTokenUsage
	EffortUsages []CodexEffortUsage
}

// CodexEffortUsage is the usage attributed to one reasoning-effort setting.
type CodexEffortUsage struct {
	Effort string
	Usage  CodexTokenUsage
}

type CodexTokenUsageInfo struct {
	Total              CodexTokenUsage
	Last               CodexTokenUsage
	ModelContextWindow int64
}

type CodexTokenUsage struct {
	InputTokens           int64
	CachedInputTokens     int64
	OutputTokens          int64
	ReasoningOutputTokens int64
	TotalTokens           int64
}

type codexTokenUsageSummary struct {
	Total       CodexTokenUsage
	ModelUsages []CodexModelUsage
}

func summarizeCodexTokenStats(stats []CodexTokenStats) codexTokenUsageSummary {
	var summary codexTokenUsageSummary
	modelGroups := make(map[string]*CodexModelUsage)
	for _, stat := range stats {
		total := stat.Info.Total
		if !total.hasTokens() {
			total = stat.Info.Last
		}
		summary.Total, _ = addCodexTokenUsage(summary.Total, total)
		for _, modelUsage := range stat.ModelUsages {
			model := normalizedCodexModelName(modelUsage.Model)
			group := modelGroups[model]
			if group == nil {
				group = &CodexModelUsage{Model: model}
				modelGroups[model] = group
			}
			group.Overall, _ = addCodexTokenUsage(group.Overall, modelUsage.Overall)
			for _, effortUsage := range modelUsage.EffortUsages {
				effort := normalizedCodexEffortName(effortUsage.Effort)
				found := false
				for index := range group.EffortUsages {
					if group.EffortUsages[index].Effort != effort {
						continue
					}
					group.EffortUsages[index].Usage, _ = addCodexTokenUsage(group.EffortUsages[index].Usage, effortUsage.Usage)
					found = true
					break
				}
				if !found {
					group.EffortUsages = append(group.EffortUsages, CodexEffortUsage{Effort: effort, Usage: effortUsage.Usage})
				}
			}
		}
	}
	models := make([]string, 0, len(modelGroups))
	for model, group := range modelGroups {
		if group.Overall.hasTokens() {
			models = append(models, model)
		}
	}
	sort.Strings(models)
	for _, model := range models {
		group := *modelGroups[model]
		sort.Slice(group.EffortUsages, func(i, j int) bool {
			return lessCodexEffortName(group.EffortUsages[i].Effort, group.EffortUsages[j].Effort)
		})
		summary.ModelUsages = append(summary.ModelUsages, group)
	}
	return summary
}

func codexModelUsagesTotal(usages []CodexModelUsage) CodexTokenUsage {
	var total CodexTokenUsage
	for _, usage := range usages {
		total, _ = addCodexTokenUsage(total, usage.Overall)
	}
	return total
}

type codexTokenUsageAccumulator struct {
	seen                    bool
	total                   CodexTokenUsage
	completedEpochTotal     CodexTokenUsage
	last                    CodexTokenUsage
	nativeLatestTotal       CodexTokenUsage
	previousTotal           CodexTokenUsage
	modelContextWindow      int64
	usageEventCount         int
	nonAdvancingUsageEvents int
	nativeCounterResets     int
	nativeCounterRecoveries int
	nativeCounterGlobal     bool
	missingLastUsageEvents  int
	aggregationOverflow     bool
}

// observe returns the newly observed usage delta and whether the snapshot
// advanced usage. Non-advancing snapshots can be metadata replays before the
// first real usage of a turn.
func (a *codexTokenUsageAccumulator) observe(info CodexTokenUsageInfo, atTurnStart bool) (CodexTokenUsage, bool) {
	nativeTotal := info.Total
	if !nativeTotal.hasTokens() {
		nativeTotal = info.Last
	}
	if !a.seen {
		// Seed from the first cumulative snapshot so parsing also works when the
		// reader starts after earlier model calls. Later cumulative snapshots are
		// validation signals, never values to add together.
		a.seen = true
		a.total = nativeTotal
		if !a.total.hasTokens() {
			a.total = info.Last
		}
		a.last = info.Last
		a.nativeLatestTotal = nativeTotal
		a.previousTotal = nativeTotal
		a.modelContextWindow = info.ModelContextWindow
		a.usageEventCount = 1
		a.nativeCounterGlobal = true
		return nativeTotal, true
	}

	progress := compareCodexTokenUsageTotals(nativeTotal, a.previousTotal)
	componentResetAtTurnStart := atTurnStart && codexTokenUsageComponentDecreased(nativeTotal, a.previousTotal)
	if progress == 0 && !componentResetAtTurnStart {
		// Codex can repeat a cumulative snapshot while refreshing last usage or
		// rate metadata. No cumulative progress means there is no new usage to add.
		a.nonAdvancingUsageEvents++
		if a.nativeCounterGlobal {
			a.total = nativeTotal
		}
		if info.Last.hasTokens() {
			a.last = info.Last
		}
		if info.ModelContextWindow > 0 {
			a.modelContextWindow = info.ModelContextWindow
		}
		a.nativeLatestTotal = nativeTotal
		a.previousTotal = nativeTotal
		return CodexTokenUsage{}, false
	}

	previousReconstructedTotal := a.total
	a.usageEventCount++
	resetAtTurnStart := componentResetAtTurnStart ||
		(atTurnStart && progress > 0 && info.Last.hasTokens() && nativeTotal == info.Last)
	if progress < 0 || resetAtTurnStart {
		a.nativeCounterResets++
		a.completedEpochTotal = a.total
		// The first observed snapshot in a reset epoch may already include several
		// model calls, so add the complete new epoch instead of only its latest call.
		var overflow bool
		a.total, overflow = addCodexTokenUsage(a.completedEpochTotal, nativeTotal)
		a.aggregationOverflow = a.aggregationOverflow || overflow
		a.nativeCounterGlobal = false
	} else if a.nativeCounterGlobal {
		// A monotonic cumulative counter is the most complete source and can bridge
		// over a missing intermediate transcript event.
		a.total = nativeTotal
	} else {
		// Within a reset epoch, the native counter is still the most complete view
		// of that epoch. Prefix it with the completed epochs so missing intermediate
		// transcript updates cannot lose usage. A future Codex fix may restore the
		// conversation-global counter in place. Detect that transition only on the
		// first usage snapshot of a new turn and only when the native snapshot
		// exactly equals the previous reconstructed total plus last_token_usage; a
		// >= check is unsafe because a local epoch can also jump after omitted
		// intermediate updates.
		if info.Last.hasTokens() {
			recoveryCandidate, recoveryOverflow := addCodexTokenUsage(a.total, info.Last)
			if atTurnStart && !recoveryOverflow && nativeTotal == recoveryCandidate {
				a.total = nativeTotal
				a.completedEpochTotal = CodexTokenUsage{}
				a.nativeCounterGlobal = true
				a.nativeCounterRecoveries++
			} else {
				var overflow bool
				a.total, overflow = addCodexTokenUsage(a.completedEpochTotal, nativeTotal)
				a.aggregationOverflow = a.aggregationOverflow || overflow
			}
		} else {
			var overflow bool
			a.total, overflow = addCodexTokenUsage(a.completedEpochTotal, nativeTotal)
			a.aggregationOverflow = a.aggregationOverflow || overflow
			a.missingLastUsageEvents++
		}
	}
	if info.Last.hasTokens() {
		a.last = info.Last
	}
	if info.ModelContextWindow > 0 {
		a.modelContextWindow = info.ModelContextWindow
	}
	a.nativeLatestTotal = nativeTotal
	a.previousTotal = nativeTotal
	return subtractCodexTokenUsage(a.total, previousReconstructedTotal), true
}

func subtractCodexTokenUsage(current CodexTokenUsage, previous CodexTokenUsage) CodexTokenUsage {
	return CodexTokenUsage{
		InputTokens:           subtractTokenCount(current.InputTokens, previous.InputTokens),
		CachedInputTokens:     subtractTokenCount(current.CachedInputTokens, previous.CachedInputTokens),
		OutputTokens:          subtractTokenCount(current.OutputTokens, previous.OutputTokens),
		ReasoningOutputTokens: subtractTokenCount(current.ReasoningOutputTokens, previous.ReasoningOutputTokens),
		TotalTokens:           subtractTokenCount(current.TotalTokens, previous.TotalTokens),
	}
}

func subtractTokenCount(current int64, previous int64) int64 {
	if current < 0 || previous < 0 || current < previous {
		return maxInt64(0, current)
	}
	return current - previous
}

func codexTokenUsageComponentDecreased(current CodexTokenUsage, previous CodexTokenUsage) bool {
	currentValues := [...]int64{
		current.InputTokens,
		current.CachedInputTokens,
		current.OutputTokens,
		current.ReasoningOutputTokens,
	}
	previousValues := [...]int64{
		previous.InputTokens,
		previous.CachedInputTokens,
		previous.OutputTokens,
		previous.ReasoningOutputTokens,
	}
	for i, currentValue := range currentValues {
		// A zero can mean that an older event schema omitted the component, so it
		// is not sufficient evidence of a reset on its own.
		if currentValue > 0 && previousValues[i] > 0 && currentValue < previousValues[i] {
			return true
		}
	}
	return false
}

func (a codexTokenUsageAccumulator) info() CodexTokenUsageInfo {
	return CodexTokenUsageInfo{
		Total:              a.total,
		Last:               a.last,
		ModelContextWindow: a.modelContextWindow,
	}
}

func compareCodexTokenUsageTotals(left CodexTokenUsage, right CodexTokenUsage) int {
	leftTotal := effectiveCodexTokenUsageTotal(left)
	rightTotal := effectiveCodexTokenUsageTotal(right)
	switch {
	case leftTotal < rightTotal:
		return -1
	case leftTotal > rightTotal:
		return 1
	default:
		return 0
	}
}

func effectiveCodexTokenUsageTotal(usage CodexTokenUsage) int64 {
	if usage.TotalTokens != 0 {
		return usage.TotalTokens
	}
	total, _ := saturatingAddNonNegativeInt64(usage.InputTokens, usage.OutputTokens)
	return total
}

func addCodexTokenUsage(left CodexTokenUsage, right CodexTokenUsage) (CodexTokenUsage, bool) {
	input, inputOverflow := saturatingAddNonNegativeInt64(left.InputTokens, right.InputTokens)
	cached, cachedOverflow := saturatingAddNonNegativeInt64(left.CachedInputTokens, right.CachedInputTokens)
	output, outputOverflow := saturatingAddNonNegativeInt64(left.OutputTokens, right.OutputTokens)
	reasoning, reasoningOverflow := saturatingAddNonNegativeInt64(left.ReasoningOutputTokens, right.ReasoningOutputTokens)
	total, totalOverflow := saturatingAddNonNegativeInt64(left.TotalTokens, right.TotalTokens)
	return CodexTokenUsage{
		InputTokens:           input,
		CachedInputTokens:     cached,
		OutputTokens:          output,
		ReasoningOutputTokens: reasoning,
		TotalTokens:           total,
	}, inputOverflow || cachedOverflow || outputOverflow || reasoningOverflow || totalOverflow
}

func saturatingAddNonNegativeInt64(left int64, right int64) (int64, bool) {
	if left < 0 || right < 0 {
		return left, true
	}
	if left > math.MaxInt64-right {
		return math.MaxInt64, true
	}
	return left + right, false
}

type CodexRateLimits struct {
	Present     bool
	LimitID     string
	LimitName   string
	PlanType    string
	ReachedType string
	Credits     CodexCreditsSnapshot
	Windows     []CodexRateLimitWindow
}

type CodexCreditsSnapshot struct {
	Present    bool
	HasCredits bool
	Unlimited  bool
	Balance    string
}

type CodexRateLimitWindow struct {
	Name             string
	UsedPercent      float64
	HasUsedPercent   bool
	ResetAt          string
	Remaining        int64
	HasRemaining     bool
	WindowMinutes    int64
	HasWindowMinutes bool
	WindowSeconds    int64
	HasWindowSeconds bool
}

type TokenStatsDiagnostic struct {
	SourceLine int
	Kind       string
	Message    string
}

func (s CodexTokenStats) HasUsage() bool {
	return s.Info.Total.hasTokens() || s.Info.Last.hasTokens()
}

func (u CodexTokenUsage) hasTokens() bool {
	return u.InputTokens != 0 ||
		u.CachedInputTokens != 0 ||
		u.OutputTokens != 0 ||
		u.ReasoningOutputTokens != 0 ||
		u.TotalTokens != 0
}

func (u CodexTokenUsage) nonCachedInputTokens() int64 {
	if u.InputTokens <= 0 || u.CachedInputTokens <= 0 {
		if u.InputTokens > 0 {
			return u.InputTokens
		}
		return 0
	}
	return maxInt64(0, u.InputTokens-u.CachedInputTokens)
}

func (u CodexTokenUsage) cachePercent() (float64, bool) {
	if u.InputTokens <= 0 || u.CachedInputTokens <= 0 {
		return 0, false
	}
	return 100 * float64(u.CachedInputTokens) / float64(u.InputTokens), true
}

func ReadCodexTokenStats(filePath string) (CodexTokenStats, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return CodexTokenStats{}, err
	}
	defer f.Close()

	sourceName := filePath
	if abs, err := filepath.Abs(filePath); err == nil {
		sourceName = abs
	}
	stats, err := ParseCodexTokenStats(f)
	stats.SourcePath = sourceName
	return stats, err
}

func ParseCodexTokenStats(r io.Reader) (CodexTokenStats, error) {
	var fallback CodexTokenStats
	var usage codexTokenUsageAccumulator
	modelTierUsage := newCodexModelTierUsageAccumulator()
	var rateLimits CodexRateLimits
	var diagnostics []TokenStatsDiagnostic
	var tokenSourceLine int
	var lastTokenCountLine int
	atTurnStart := false
	reader := bufio.NewReader(r)
	lineNo := 0
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) == 0 && readErr == io.EOF {
			break
		}
		if readErr != nil && readErr != io.EOF {
			return finishCodexTokenStats(usage, fallback, rateLimits, diagnostics, tokenSourceLine, lastTokenCountLine, modelTierUsage.snapshot(), modelTierUsage.modelSnapshot()), fmt.Errorf("read Codex token stats: %w", readErr)
		}
		lineNo++
		line = bytes.TrimSpace(line)
		if len(line) == 0 || line[0] != '{' {
			if readErr == io.EOF {
				break
			}
			continue
		}
		var event codexTokenStatsEvent
		if err := json.Unmarshal(line, &event); err != nil {
			diagnostics = append(diagnostics, TokenStatsDiagnostic{
				SourceLine: lineNo,
				Kind:       "invalid_json",
				Message:    err.Error(),
			})
			if readErr == io.EOF {
				break
			}
			continue
		}
		if context, ok := parseCodexModelTierContext(event); ok {
			if event.Type == "event_msg" || event.Type == "thread_settings_applied" {
				modelTierUsage.applySettings(context)
			} else {
				modelTierUsage.applyTurnContext(context)
			}
		}
		if event.Type == "turn_context" {
			// A fixed Codex can restore its conversation-global token counter only
			// when a new turn starts. Retain this boundary until the first usage
			// snapshot so an in-turn local-epoch jump cannot masquerade as recovery.
			atTurnStart = true
		}
		if tokenCount, ok := parseCodexTokenCountEvent(event, line); ok {
			lastTokenCountLine = lineNo
			if tokenCount.RateLimits.Present {
				rateLimits = tokenCount.RateLimits
			}
			tokenCountHasUsage := tokenCount.Info.Total.hasTokens() || tokenCount.Info.Last.hasTokens()
			if tokenCountHasUsage {
				usageDelta, advanced := usage.observe(tokenCount.Info, atTurnStart)
				modelTierUsage.observeTokenUsage(usageDelta, advanced)
				if advanced {
					atTurnStart = false
				}
				tokenSourceLine = lineNo
			}
			if readErr == io.EOF {
				break
			}
			continue
		}
		if usage := normalizeCodexUsage(event.Usage); usage.hasTokens() {
			atTurnStart = false
			modelTierUsage.observeFallbackUsage(usage)
			fallback = CodexTokenStats{
				SourceLine:       lineNo,
				Source:           "event usage",
				Info:             CodexTokenUsageInfo{Total: usage, Last: usage},
				UsedFallbackOnly: true,
			}
		}
		if readErr == io.EOF {
			break
		}
	}
	return finishCodexTokenStats(usage, fallback, rateLimits, diagnostics, tokenSourceLine, lastTokenCountLine, modelTierUsage.snapshot(), modelTierUsage.modelSnapshot()), nil
}

func finishCodexTokenStats(
	usage codexTokenUsageAccumulator,
	fallback CodexTokenStats,
	rateLimits CodexRateLimits,
	diagnostics []TokenStatsDiagnostic,
	tokenSourceLine int,
	lastTokenCountLine int,
	modelTierUsages []CodexModelTierUsage,
	modelUsages []CodexModelUsage,
) CodexTokenStats {
	if usage.seen {
		info := usage.info()
		// A transcript can contain a native token_count stream plus a later
		// turn.completed usage fallback for a turn that never emitted token_count.
		// The model/effort accumulator retains that fallback-only turn, so use its
		// sum as the conversation total to keep overall and detailed tables
		// internally consistent. NativeLatestTotal remains available to explain
		// the reconciliation in the analysis section.
		if modelTotal := codexModelUsagesTotal(modelUsages); modelTotal.hasTokens() && modelTotal != info.Total {
			info.Total = modelTotal
		}
		return CodexTokenStats{
			SourceLine:               tokenSourceLine,
			Source:                   "token_count",
			Info:                     info,
			ModelTierUsages:          modelTierUsages,
			ModelUsages:              modelUsages,
			NativeLatestTotal:        usage.nativeLatestTotal,
			UsageEventCount:          usage.usageEventCount,
			NonAdvancingUsageEvents:  usage.nonAdvancingUsageEvents,
			NativeCounterResets:      usage.nativeCounterResets,
			NativeCounterRecoveries:  usage.nativeCounterRecoveries,
			MissingLastUsageEvents:   usage.missingLastUsageEvents,
			UsageAggregationOverflow: usage.aggregationOverflow,
			RateLimits:               rateLimits,
			Diagnostics:              diagnostics,
		}
	}
	if fallback.HasUsage() {
		if total := codexModelUsagesTotal(modelUsages); total.hasTokens() {
			fallback.Info.Total = total
		}
		fallback.RateLimits = rateLimits
		fallback.Diagnostics = diagnostics
		fallback.ModelTierUsages = modelTierUsages
		fallback.ModelUsages = modelUsages
		return fallback
	}
	if rateLimits.Present {
		return CodexTokenStats{
			SourceLine:      lastTokenCountLine,
			Source:          "token_count",
			ModelTierUsages: modelTierUsages,
			ModelUsages:     modelUsages,
			RateLimits:      rateLimits,
			Diagnostics:     diagnostics,
		}
	}
	fallback.Diagnostics = append(fallback.Diagnostics, diagnostics...)
	fallback.ModelTierUsages = modelTierUsages
	fallback.ModelUsages = modelUsages
	return fallback
}

type codexTokenStatsEvent struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
	Usage   codexTokenUsage `json:"usage"`
}

type codexTokenCountPayload struct {
	Type        string              `json:"type"`
	Info        codexTokenUsageInfo `json:"info"`
	RateLimits  json.RawMessage     `json:"rate_limits"`
	RateLimits2 json.RawMessage     `json:"rateLimits"`
	Usage       codexTokenUsage     `json:"usage"`
}

type codexTokenUsageInfo struct {
	Total               codexTokenUsage `json:"total_token_usage"`
	Total2              codexTokenUsage `json:"totalTokenUsage"`
	Last                codexTokenUsage `json:"last_token_usage"`
	Last2               codexTokenUsage `json:"lastTokenUsage"`
	ModelContextWindow  int64           `json:"model_context_window"`
	ModelContextWindow2 int64           `json:"modelContextWindow"`
}

type codexTokenUsage struct {
	InputTokens            int64 `json:"input_tokens"`
	InputTokens2           int64 `json:"inputTokens"`
	CachedInputTokens      int64 `json:"cached_input_tokens"`
	CachedInputTokens2     int64 `json:"cachedInputTokens"`
	OutputTokens           int64 `json:"output_tokens"`
	OutputTokens2          int64 `json:"outputTokens"`
	ReasoningOutputTokens  int64 `json:"reasoning_output_tokens"`
	ReasoningOutputTokens2 int64 `json:"reasoningOutputTokens"`
	TotalTokens            int64 `json:"total_tokens"`
	TotalTokens2           int64 `json:"totalTokens"`
	InputTokensDetails     struct {
		CachedInputTokens int64 `json:"cached_input_tokens"`
		CachedTokens      int64 `json:"cached_tokens"`
	} `json:"input_tokens_details"`
	PromptTokensDetails struct {
		CachedTokens int64 `json:"cached_tokens"`
	} `json:"prompt_tokens_details"`
}

type codexModelTierContext struct {
	Model  string
	Tier   string
	Effort string
	TurnID string
}

type codexModelTierUsageKey struct {
	Model string
	Tier  string
}

type codexModelTierEffortUsageKey struct {
	Model  string
	Tier   string
	Effort string
}

type codexModelTierUsageAccumulator struct {
	current             codexModelTierContext
	currentTurnID       string
	hasContext          bool
	awaitingTurnContext bool
	turnHasTokenUsage   bool
	fallbackUsage       CodexTokenUsage
	groups              map[codexModelTierEffortUsageKey]CodexTokenUsage
}

func newCodexModelTierUsageAccumulator() codexModelTierUsageAccumulator {
	return codexModelTierUsageAccumulator{
		groups: make(map[codexModelTierEffortUsageKey]CodexTokenUsage),
	}
}

func (a *codexModelTierUsageAccumulator) applySettings(context codexModelTierContext) {
	a.finishTurn()
	a.current = normalizedCodexModelTierContext(context)
	a.currentTurnID = ""
	a.hasContext = true
	a.awaitingTurnContext = true
	a.resetTurn()
}

func (a *codexModelTierUsageAccumulator) applyTurnContext(context codexModelTierContext) {
	context = normalizedCodexModelTierContext(context)
	if !a.hasContext {
		a.current = context
		a.currentTurnID = context.TurnID
		a.hasContext = true
		a.awaitingTurnContext = false
		return
	}
	if context.TurnID != "" && a.currentTurnID != "" && context.TurnID != a.currentTurnID {
		a.finishTurn()
		a.current = context
		a.currentTurnID = context.TurnID
		a.awaitingTurnContext = false
		a.resetTurn()
		return
	}
	if context.TurnID != "" && a.currentTurnID == "" && !a.awaitingTurnContext && (a.turnHasTokenUsage || a.fallbackUsage.hasTokens()) {
		a.finishTurn()
		a.current = context
		a.currentTurnID = context.TurnID
		a.resetTurn()
	} else {
		if context.Model != "" {
			a.current.Model = context.Model
		}
		if context.Tier != "" {
			a.current.Tier = context.Tier
		}
		if context.Effort != "" {
			a.current.Effort = context.Effort
		}
		if context.TurnID != "" {
			a.currentTurnID = context.TurnID
		}
	}
	a.awaitingTurnContext = false
}

func (a *codexModelTierUsageAccumulator) observeTokenUsage(delta CodexTokenUsage, advanced bool) {
	if !advanced || !delta.hasTokens() {
		return
	}
	a.turnHasTokenUsage = true
	a.fallbackUsage = CodexTokenUsage{}
	key := codexModelTierEffortUsageKey{
		Model:  normalizedCodexModelName(a.current.Model),
		Tier:   normalizedCodexTierName(a.current.Tier),
		Effort: normalizedCodexEffortName(a.current.Effort),
	}
	previous := a.groups[key]
	a.groups[key], _ = addCodexTokenUsage(previous, delta)
}

func (a *codexModelTierUsageAccumulator) observeFallbackUsage(usage CodexTokenUsage) {
	if a.turnHasTokenUsage || !usage.hasTokens() {
		return
	}
	a.fallbackUsage = usage
}

func (a *codexModelTierUsageAccumulator) finishTurn() {
	if a.turnHasTokenUsage || !a.fallbackUsage.hasTokens() {
		a.resetTurn()
		return
	}
	key := codexModelTierEffortUsageKey{
		Model:  normalizedCodexModelName(a.current.Model),
		Tier:   normalizedCodexTierName(a.current.Tier),
		Effort: normalizedCodexEffortName(a.current.Effort),
	}
	previous := a.groups[key]
	a.groups[key], _ = addCodexTokenUsage(previous, a.fallbackUsage)
	a.resetTurn()
}

func (a *codexModelTierUsageAccumulator) resetTurn() {
	a.turnHasTokenUsage = false
	a.fallbackUsage = CodexTokenUsage{}
}

func (a *codexModelTierUsageAccumulator) snapshot() []CodexModelTierUsage {
	a.finishTurn()
	grouped := make(map[codexModelTierUsageKey]CodexTokenUsage)
	for key, usage := range a.groups {
		if usage.hasTokens() {
			legacyKey := codexModelTierUsageKey{Model: key.Model, Tier: key.Tier}
			previous := grouped[legacyKey]
			grouped[legacyKey], _ = addCodexTokenUsage(previous, usage)
		}
	}
	keys := make([]codexModelTierUsageKey, 0, len(grouped))
	for key, usage := range grouped {
		if usage.hasTokens() {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Model != keys[j].Model {
			return keys[i].Model < keys[j].Model
		}
		return keys[i].Tier < keys[j].Tier
	})
	result := make([]CodexModelTierUsage, 0, len(keys))
	for _, key := range keys {
		result = append(result, CodexModelTierUsage{
			Model: key.Model,
			Tier:  key.Tier,
			Usage: grouped[key],
		})
	}
	return result
}

func (a *codexModelTierUsageAccumulator) modelSnapshot() []CodexModelUsage {
	a.finishTurn()
	type effortGroup struct {
		usage CodexTokenUsage
	}
	type modelGroup struct {
		overall CodexTokenUsage
		efforts map[string]effortGroup
	}
	grouped := make(map[string]*modelGroup)
	for key, usage := range a.groups {
		if !usage.hasTokens() {
			continue
		}
		model := normalizedCodexModelName(key.Model)
		group := grouped[model]
		if group == nil {
			group = &modelGroup{efforts: make(map[string]effortGroup)}
			grouped[model] = group
		}
		group.overall, _ = addCodexTokenUsage(group.overall, usage)
		effort := normalizedCodexEffortName(key.Effort)
		perEffort := group.efforts[effort]
		perEffort.usage, _ = addCodexTokenUsage(perEffort.usage, usage)
		group.efforts[effort] = perEffort
	}
	models := make([]string, 0, len(grouped))
	for model, group := range grouped {
		if group.overall.hasTokens() {
			models = append(models, model)
		}
	}
	sort.Strings(models)
	result := make([]CodexModelUsage, 0, len(models))
	for _, model := range models {
		group := grouped[model]
		efforts := make([]string, 0, len(group.efforts))
		for effort, perEffort := range group.efforts {
			if perEffort.usage.hasTokens() {
				efforts = append(efforts, effort)
			}
		}
		sort.Slice(efforts, func(i, j int) bool {
			return lessCodexEffortName(efforts[i], efforts[j])
		})
		usage := CodexModelUsage{Model: model, Overall: group.overall, EffortUsages: make([]CodexEffortUsage, 0, len(efforts))}
		for _, effort := range efforts {
			usage.EffortUsages = append(usage.EffortUsages, CodexEffortUsage{Effort: effort, Usage: group.efforts[effort].usage})
		}
		result = append(result, usage)
	}
	return result
}

func normalizedCodexModelTierContext(context codexModelTierContext) codexModelTierContext {
	context.Model = strings.TrimSpace(context.Model)
	context.Tier = strings.TrimSpace(context.Tier)
	context.Effort = strings.TrimSpace(context.Effort)
	context.TurnID = strings.TrimSpace(context.TurnID)
	return context
}

func normalizedCodexModelName(model string) string {
	model = strings.TrimSpace(model)
	if model == "" {
		return "unknown"
	}
	return model
}

func normalizedCodexTierName(tier string) string {
	tier = strings.TrimSpace(tier)
	if tier == "" {
		return "unknown"
	}
	return tier
}

func normalizedCodexEffortName(effort string) string {
	effort = strings.TrimSpace(effort)
	if effort == "" {
		return "unknown"
	}
	return effort
}

func lessCodexEffortName(left string, right string) bool {
	left = normalizedCodexEffortName(left)
	right = normalizedCodexEffortName(right)
	leftOrder := codexEffortOrder(left)
	rightOrder := codexEffortOrder(right)
	if leftOrder != rightOrder {
		return leftOrder < rightOrder
	}
	return left < right
}

func codexEffortOrder(effort string) int {
	switch strings.ToLower(normalizedCodexEffortName(effort)) {
	case "low":
		return 0
	case "medium":
		return 1
	case "high":
		return 2
	case "xhigh":
		return 3
	case "max":
		return 4
	case "unknown":
		return 100
	default:
		return 50
	}
}

type codexContextSettings struct {
	Model            string `json:"model"`
	ModelSlug        string `json:"model_slug"`
	ModelSlug2       string `json:"modelSlug"`
	ServiceTier      string `json:"service_tier"`
	ServiceTier2     string `json:"serviceTier"`
	Effort           string `json:"effort"`
	ReasoningEffort  string `json:"reasoning_effort"`
	ReasoningEffort2 string `json:"reasoningEffort"`
}

type codexContextPayload struct {
	Type              string               `json:"type"`
	TurnID            string               `json:"turn_id"`
	TurnID2           string               `json:"turnId"`
	Model             string               `json:"model"`
	ModelSlug         string               `json:"model_slug"`
	ModelSlug2        string               `json:"modelSlug"`
	ServiceTier       string               `json:"service_tier"`
	ServiceTier2      string               `json:"serviceTier"`
	Effort            string               `json:"effort"`
	ReasoningEffort   string               `json:"reasoning_effort"`
	ReasoningEffort2  string               `json:"reasoningEffort"`
	ThreadSettings    codexContextSettings `json:"thread_settings"`
	ThreadSettings2   codexContextSettings `json:"threadSettings"`
	CollaborationMode struct {
		Settings codexContextSettings `json:"settings"`
	} `json:"collaboration_mode"`
}

func parseCodexModelTierContext(event codexTokenStatsEvent) (codexModelTierContext, bool) {
	if event.Type != "turn_context" && event.Type != "thread_settings_applied" && event.Type != "event_msg" {
		return codexModelTierContext{}, false
	}
	if len(bytes.TrimSpace(event.Payload)) == 0 {
		return codexModelTierContext{}, false
	}
	var payload codexContextPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return codexModelTierContext{}, false
	}
	if event.Type == "event_msg" && payload.Type != "thread_settings_applied" {
		return codexModelTierContext{}, false
	}
	if event.Type == "thread_settings_applied" && payload.Type != "" && payload.Type != "thread_settings_applied" {
		return codexModelTierContext{}, false
	}
	model := firstNonEmptyCodexString(
		payload.Model,
		payload.ModelSlug,
		payload.ModelSlug2,
		payload.ThreadSettings.Model,
		payload.ThreadSettings.ModelSlug,
		payload.ThreadSettings.ModelSlug2,
		payload.ThreadSettings2.Model,
		payload.ThreadSettings2.ModelSlug,
		payload.ThreadSettings2.ModelSlug2,
		payload.CollaborationMode.Settings.Model,
		payload.CollaborationMode.Settings.ModelSlug,
		payload.CollaborationMode.Settings.ModelSlug2,
	)
	tier := firstNonEmptyCodexString(
		payload.ServiceTier,
		payload.ServiceTier2,
		payload.ThreadSettings.ServiceTier,
		payload.ThreadSettings.ServiceTier2,
		payload.ThreadSettings2.ServiceTier,
		payload.ThreadSettings2.ServiceTier2,
		payload.CollaborationMode.Settings.ServiceTier,
		payload.CollaborationMode.Settings.ServiceTier2,
	)
	effort := firstNonEmptyCodexString(
		payload.Effort,
		payload.ReasoningEffort,
		payload.ReasoningEffort2,
		payload.ThreadSettings.Effort,
		payload.ThreadSettings.ReasoningEffort,
		payload.ThreadSettings.ReasoningEffort2,
		payload.ThreadSettings2.Effort,
		payload.ThreadSettings2.ReasoningEffort,
		payload.ThreadSettings2.ReasoningEffort2,
		payload.CollaborationMode.Settings.Effort,
		payload.CollaborationMode.Settings.ReasoningEffort,
		payload.CollaborationMode.Settings.ReasoningEffort2,
	)
	turnID := firstNonEmptyCodexString(payload.TurnID, payload.TurnID2)
	if model == "" && tier == "" && effort == "" && turnID == "" {
		return codexModelTierContext{}, false
	}
	return codexModelTierContext{Model: model, Tier: tier, Effort: effort, TurnID: turnID}, true
}

func firstNonEmptyCodexString(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func parseCodexTokenCountEvent(event codexTokenStatsEvent, raw []byte) (CodexTokenStats, bool) {
	payloadRaw := event.Payload
	if event.Type == "token_count" {
		payloadRaw = raw
	} else if event.Type != "event_msg" && event.Type != "response_item" {
		return CodexTokenStats{}, false
	}
	if len(bytes.TrimSpace(payloadRaw)) == 0 {
		return CodexTokenStats{}, false
	}
	var payload codexTokenCountPayload
	if err := json.Unmarshal(payloadRaw, &payload); err != nil {
		return CodexTokenStats{}, false
	}
	if payload.Type != "token_count" {
		return CodexTokenStats{}, false
	}
	info := CodexTokenUsageInfo{
		Total:              normalizeCodexUsage(firstNonZeroCodexTokenUsage(payload.Info.Total, payload.Info.Total2)),
		Last:               normalizeCodexUsage(firstNonZeroCodexTokenUsage(payload.Info.Last, payload.Info.Last2)),
		ModelContextWindow: firstNonZeroInt64Teams(payload.Info.ModelContextWindow, payload.Info.ModelContextWindow2),
	}
	if !info.Total.hasTokens() && !info.Last.hasTokens() {
		usage := normalizeCodexUsage(payload.Usage)
		info.Total = usage
		info.Last = usage
	}
	rawRateLimits := payload.RateLimits
	if len(bytes.TrimSpace(rawRateLimits)) == 0 {
		rawRateLimits = payload.RateLimits2
	}
	return CodexTokenStats{
		Source:     "token_count",
		Info:       info,
		RateLimits: parseCodexRateLimits(rawRateLimits),
	}, true
}

func normalizeCodexUsage(raw codexTokenUsage) CodexTokenUsage {
	cached := firstNonZeroInt64Teams(
		raw.CachedInputTokens,
		raw.CachedInputTokens2,
		raw.InputTokensDetails.CachedInputTokens,
		raw.InputTokensDetails.CachedTokens,
		raw.PromptTokensDetails.CachedTokens,
	)
	return CodexTokenUsage{
		InputTokens:           firstNonZeroInt64Teams(raw.InputTokens, raw.InputTokens2),
		CachedInputTokens:     cached,
		OutputTokens:          firstNonZeroInt64Teams(raw.OutputTokens, raw.OutputTokens2),
		ReasoningOutputTokens: firstNonZeroInt64Teams(raw.ReasoningOutputTokens, raw.ReasoningOutputTokens2),
		TotalTokens:           firstNonZeroInt64Teams(raw.TotalTokens, raw.TotalTokens2),
	}
}

func firstNonZeroCodexTokenUsage(values ...codexTokenUsage) codexTokenUsage {
	for _, value := range values {
		if normalizeCodexUsage(value).hasTokens() {
			return value
		}
	}
	return codexTokenUsage{}
}

func parseCodexRateLimits(raw json.RawMessage) CodexRateLimits {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return CodexRateLimits{}
	}
	out := CodexRateLimits{Present: true}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return out
	}
	out.LimitID = firstJSONMapString(fields, "limit_id", "limitId")
	out.LimitName = firstJSONMapString(fields, "limit_name", "limitName")
	out.PlanType = firstJSONMapString(fields, "plan_type", "planType")
	out.ReachedType = firstJSONMapString(fields, "rate_limit_reached_type", "rateLimitReachedType")
	out.Credits = parseCodexCreditsSnapshot(fields["credits"])
	windowNames := make([]string, 0, len(fields))
	for name, value := range fields {
		if isScalarRateLimitField(name) {
			continue
		}
		if len(bytes.TrimSpace(value)) == 0 || bytes.TrimSpace(value)[0] != '{' {
			continue
		}
		windowNames = append(windowNames, name)
	}
	sort.Strings(windowNames)
	for _, name := range windowNames {
		if window, ok := parseCodexRateLimitWindow(name, fields[name]); ok {
			out.Windows = append(out.Windows, window)
		}
	}
	return out
}

func parseCodexRateLimitWindow(name string, raw json.RawMessage) (CodexRateLimitWindow, bool) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return CodexRateLimitWindow{}, false
	}
	out := CodexRateLimitWindow{Name: name}
	if value, ok := firstJSONMapFloat(fields, "used_percent", "usedPercent", "usage_percent", "usagePercent", "percent_used", "percentUsed"); ok {
		out.UsedPercent = value
		out.HasUsedPercent = true
	}
	out.ResetAt = firstJSONMapStringLike(fields, "reset_at", "resetAt", "resets_at", "resetsAt")
	if value, ok := firstJSONMapInt(fields, "remaining", "remaining_tokens", "remainingTokens"); ok {
		out.Remaining = value
		out.HasRemaining = true
	}
	if value, ok := firstJSONMapInt(fields, "window_minutes", "windowMinutes", "window_duration_mins", "windowDurationMins"); ok {
		out.WindowMinutes = value
		out.HasWindowMinutes = true
	}
	if value, ok := firstJSONMapInt(fields, "window_seconds", "windowSeconds", "seconds"); ok {
		out.WindowSeconds = value
		out.HasWindowSeconds = true
	}
	return out, out.HasUsedPercent || out.ResetAt != "" || out.HasRemaining || out.HasWindowMinutes || out.HasWindowSeconds
}

func parseCodexCreditsSnapshot(raw json.RawMessage) CodexCreditsSnapshot {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) || raw[0] != '{' {
		return CodexCreditsSnapshot{}
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return CodexCreditsSnapshot{Present: true}
	}
	out := CodexCreditsSnapshot{Present: true}
	if value, ok := firstJSONMapBool(fields, "has_credits", "hasCredits"); ok {
		out.HasCredits = value
	}
	if value, ok := firstJSONMapBool(fields, "unlimited"); ok {
		out.Unlimited = value
	}
	out.Balance = firstJSONMapStringLike(fields, "balance")
	return out
}

func isScalarRateLimitField(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "limit_id", "limitid", "limit_name", "limitname", "plan_type", "plantype", "rate_limit_reached_type", "ratelimitreachedtype", "credits":
		return true
	default:
		return false
	}
}

func firstJSONMapString(fields map[string]json.RawMessage, keys ...string) string {
	for _, key := range keys {
		raw, ok := fields[key]
		if !ok {
			continue
		}
		var value string
		if err := json.Unmarshal(raw, &value); err == nil && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstJSONMapStringLike(fields map[string]json.RawMessage, keys ...string) string {
	for _, key := range keys {
		raw, ok := fields[key]
		if !ok {
			continue
		}
		var value string
		if err := json.Unmarshal(raw, &value); err == nil && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
		var number json.Number
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&number); err == nil {
			return number.String()
		}
		var boolValue bool
		if err := json.Unmarshal(raw, &boolValue); err == nil {
			return strconv.FormatBool(boolValue)
		}
	}
	return ""
}

func firstJSONMapBool(fields map[string]json.RawMessage, keys ...string) (bool, bool) {
	for _, key := range keys {
		raw, ok := fields[key]
		if !ok {
			continue
		}
		var value bool
		if err := json.Unmarshal(raw, &value); err == nil {
			return value, true
		}
	}
	return false, false
}

func firstJSONMapInt(fields map[string]json.RawMessage, keys ...string) (int64, bool) {
	for _, key := range keys {
		raw, ok := fields[key]
		if !ok {
			continue
		}
		var value int64
		if err := json.Unmarshal(raw, &value); err == nil {
			return value, true
		}
		var floatValue float64
		if err := json.Unmarshal(raw, &floatValue); err == nil {
			return int64(floatValue), true
		}
	}
	return 0, false
}

func firstJSONMapFloat(fields map[string]json.RawMessage, keys ...string) (float64, bool) {
	for _, key := range keys {
		raw, ok := fields[key]
		if !ok {
			continue
		}
		var value float64
		if err := json.Unmarshal(raw, &value); err == nil {
			return value, true
		}
	}
	return 0, false
}

func (b *Bridge) formatWorkSessionStats(ctx context.Context, session *Session) string {
	if session == nil {
		return "STATS: Codex tokens\nSession: not found"
	}
	lines := []string{
		"STATS: Codex tokens",
		"",
		"Session: " + session.ID,
	}
	if strings.TrimSpace(session.CodexThreadID) != "" {
		lines = append(lines, "", "Codex thread: "+strings.TrimSpace(session.CodexThreadID))
	} else {
		lines = append(lines, "", "Codex thread: not linked yet")
	}
	if latest, ok := b.latestTurnForStats(ctx, session.ID); ok {
		lines = append(lines, "", "Latest request: "+userFacingTurnStatus(latest.Status))
		if strings.TrimSpace(latest.CodexTurnID) != "" {
			lines = append(lines, "", "Latest Codex turn: "+latest.CodexTurnID)
		}
	}
	if strings.TrimSpace(session.CodexThreadID) == "" {
		lines = append(lines, "", "Token stats unavailable: this Work chat does not have a linked Codex thread yet.")
		return strings.Join(lines, "\n")
	}
	local, ok, err := b.localCodexSessionForTeamsSession(ctx, *session)
	if err != nil {
		lines = append(lines, "", "Token stats unavailable: "+err.Error())
		return strings.Join(lines, "\n")
	}
	if !ok || strings.TrimSpace(local.FilePath) == "" {
		lines = append(lines, "", "Token stats unavailable: no local Codex transcript is linked to this Work chat.")
		return strings.Join(lines, "\n")
	}
	stats, err := ReadCodexTokenStats(local.FilePath)
	if err != nil {
		lines = append(lines, "", "Token stats unavailable: read local Codex transcript failed: "+err.Error())
		return strings.Join(lines, "\n")
	}
	lines = append(lines, "")
	lines = append(lines, formatCodexMainAgentStatsLines(stats)...)
	subagentSummary, subagentCount, subagentProblems, discoveryErr := b.readSubagentTokenStatsForWorkSession(ctx, session.CodexThreadID)
	lines = append(lines, "")
	if discoveryErr != nil {
		lines = append(lines, formatCodexSubagentUnavailableLines(discoveryErr)...)
		return strings.Join(lines, "\n")
	}
	if subagentCount == 0 {
		lines = append(lines, formatCodexNoSubagentLines(subagentProblems)...)
	} else {
		lines = append(lines, formatCodexSubagentStatsLines(subagentSummary, subagentCount, subagentProblems)...)
	}
	return strings.Join(lines, "\n")
}

func formatCodexTokenStatsMetadataLines(stats CodexTokenStats) []string {
	source := strings.TrimSpace(stats.Source)
	if source == "" {
		source = "unknown"
	}
	sourceLine := "Source: " + source
	if stats.SourceLine > 0 {
		sourceLine += " at transcript line " + strconv.Itoa(stats.SourceLine)
	}
	if strings.TrimSpace(stats.SourcePath) != "" {
		sourceLine += " (" + stats.SourcePath + ")"
	}
	lines := []string{sourceLine}
	if stats.UsedFallbackOnly {
		lines = append(lines, "", "Reliability: using runner usage fallback because no `token_count` event was found; conversation totals and context-window analysis may be incomplete.")
	} else if stats.HasUsage() {
		lines = append(lines, "", fmt.Sprintf(
			"Reliability: reconstructed conversation usage from %d unique Codex `token_count` update(s) in local history; malformed trailing JSONL lines are ignored and reported below.",
			stats.UsageEventCount,
		))
		if stats.NonAdvancingUsageEvents > 0 || stats.NativeCounterResets > 0 || stats.NativeCounterRecoveries > 0 {
			lines = append(lines, "", fmt.Sprintf(
				"Aggregation: ignored %d non-advancing usage snapshot(s); observed %d native cumulative counter reset(s) and %d recovery event(s).",
				stats.NonAdvancingUsageEvents,
				stats.NativeCounterResets,
				stats.NativeCounterRecoveries,
			))
		}
	} else {
		lines = append(lines, "", "Reliability: Codex `token_count` metadata was found, but it did not contain a usage snapshot.")
		lines = append(lines, "", "Token usage unavailable: no Codex usage event was found in the linked transcript.")
	}
	return lines
}

func formatCodexMainAgentStatsLines(stats CodexTokenStats) []string {
	lines := []string{"🧠 MAIN AGENT · metadata:"}
	lines = append(lines, formatCodexTokenStatsMetadataLines(stats)...)
	if !stats.HasUsage() {
		lines = append(lines, "", "🧠 MAIN AGENT · UNAVAILABLE:")
		lines = append(lines,
			"Status: no Codex usage event was found",
			"Token usage: not reported",
			"Cache hit rate: N/A",
			"Model/effort breakdown: not reported",
		)
	} else {
		overall := stats.Info.Total
		if !overall.hasTokens() {
			overall = stats.Info.Last
		}
		lines = append(lines, "", "🧠 MAIN AGENT · snapshots:")
		if stats.Info.Last.hasTokens() {
			lines = append(lines, "Last recorded model usage:")
			lines = append(lines, formatTokenUsageLines(stats.Info.Last)...)
		}
		if stats.Info.Total.hasTokens() {
			lines = append(lines, "Conversation total:")
			lines = append(lines, formatTokenUsageLines(stats.Info.Total)...)
		}
		lines = append(lines, "", "🧠 MAIN AGENT · overall:")
		lines = append(lines, formatTokenUsageLines(overall)...)
		lines = append(lines, "", "🧠 MAIN AGENT · model/effort detail:")
		if modelLines := formatCodexModelUsageLines(stats.ModelUsages); len(modelLines) > 0 {
			lines = append(lines, modelLines...)
		} else {
			lines = append(lines, "Model/effort breakdown: unavailable")
		}
		analysisInfo := stats.Info
		analysisInfo.Total = overall
		analysis := formatTokenUsageAnalysis(analysisInfo)
		analysis = append(analysis, formatCodexModelUsageAnalysis(stats.ModelUsages)...)
		analysis = append(analysis, formatTokenAggregationAnalysis(stats)...)
		if len(analysis) > 0 {
			lines = append(lines, "", "🧠 MAIN AGENT · analysis:")
			lines = append(lines, analysis...)
		}
	}
	if rateLines := formatCodexRateLimitLines(stats.RateLimits); len(rateLines) > 0 {
		lines = append(lines, "", "🧠 MAIN AGENT · rate limits:")
		lines = append(lines, rateLines...)
	}
	if diagnostics := formatTokenStatsDiagnostics(stats.Diagnostics); len(diagnostics) > 0 {
		lines = append(lines, "", "🧠 MAIN AGENT · diagnostics:")
		lines = append(lines, diagnostics...)
	}
	return lines
}

func formatCodexSubagentStatsLines(summary codexTokenUsageSummary, count int, problems []string) []string {
	title := fmt.Sprintf("🧩 SUBAGENTS (%d) ·", count)
	lines := []string{title + " overall:"}
	if summary.Total.hasTokens() {
		lines = append(lines, formatTokenUsageLines(summary.Total)...)
	} else {
		lines = append(lines, "Token usage unavailable: no readable subagent usage event was found.")
	}
	lines = append(lines, "", title+" model/effort detail:")
	if modelLines := formatCodexModelUsageLines(summary.ModelUsages); len(modelLines) > 0 {
		lines = append(lines, modelLines...)
	} else {
		lines = append(lines, "Model/effort breakdown: unavailable")
	}
	if len(problems) > 0 {
		lines = append(lines, "", title+" diagnostics:")
		for _, problem := range problems {
			lines = append(lines, "- "+problem)
		}
	}
	return lines
}

func formatCodexNoSubagentLines(problems []string) []string {
	lines := []string{
		"🧩 SUBAGENTS · NOT USED:",
		"Status: no user-visible subagent transcript was found",
		"Transcript count: 0",
		"Token usage: 0",
		"Cache hit rate: N/A",
		"Model/effort breakdown: not applicable",
	}
	if len(problems) > 0 {
		lines = append(lines, "", "🧩 SUBAGENTS · diagnostics:")
		for _, problem := range problems {
			lines = append(lines, "- "+problem)
		}
	}
	return lines
}

func formatCodexSubagentUnavailableLines(err error) []string {
	return []string{
		"🧩 SUBAGENTS · UNAVAILABLE:",
		"Status: history discovery failed",
		"Reason: " + err.Error(),
		"Token usage: not reported",
		"Cache hit rate: N/A",
		"Model/effort breakdown: not reported",
	}
}

// discoverSubagentProjects bounds the expensive history catalog scan that a
// stats request needs in order to connect a Work-chat thread to its child
// transcripts. The short cache keeps repeated helper-stats commands from
// rescanning every session file while still refreshing promptly as history
// changes. Partial projects and the discovery error are cached together so
// callers keep the existing fail-closed diagnostics behavior.
func (b *Bridge) discoverSubagentProjects(ctx context.Context) ([]codexhistory.Project, error) {
	if b == nil {
		return discoverCodexProjectsForTeams(ctx, "")
	}
	root := strings.TrimSpace(b.scope.CodexHome)
	now := time.Now()
	b.subagentProjectsMu.Lock()
	if !b.subagentProjectsCachedAt.IsZero() && b.subagentProjectsCacheRoot == root && now.Sub(b.subagentProjectsCachedAt) < subagentProjectsCacheTTL {
		projects := cloneCodexProjects(b.subagentProjectsCache)
		err := b.subagentProjectsCacheErr
		b.subagentProjectsMu.Unlock()
		return projects, err
	}
	b.subagentProjectsMu.Unlock()

	projects, err := discoverCodexProjectsForTeams(ctx, root)
	cached := cloneCodexProjects(projects)
	b.subagentProjectsMu.Lock()
	b.subagentProjectsCache = cached
	b.subagentProjectsCacheRoot = root
	b.subagentProjectsCacheErr = err
	b.subagentProjectsCachedAt = now
	b.subagentProjectsMu.Unlock()
	return cloneCodexProjects(cached), err
}

func (b *Bridge) readSubagentTokenStatsForWorkSession(ctx context.Context, threadID string) (codexTokenUsageSummary, int, []string, error) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return codexTokenUsageSummary{}, 0, nil, nil
	}
	projects, discoveryErr := b.discoverSubagentProjects(ctx)
	if discoveryErr != nil && len(projects) == 0 {
		return codexTokenUsageSummary{}, 0, nil, discoveryErr
	}
	parent, _, ok := findCodexSession(projects, threadID)
	if !ok {
		if discoveryErr != nil {
			return codexTokenUsageSummary{}, 0, nil, discoveryErr
		}
		return codexTokenUsageSummary{}, 0, nil, nil
	}
	subagents := codexhistory.FilterUserVisibleSubagentSessions(parent.Subagents)
	if len(subagents) == 0 {
		if discoveryErr != nil {
			return codexTokenUsageSummary{}, 0, nil, discoveryErr
		}
		return codexTokenUsageSummary{}, 0, nil, nil
	}
	stats := make([]CodexTokenStats, 0, len(subagents))
	problems := make([]string, 0)
	if discoveryErr != nil {
		problems = append(problems, "history discovery: "+discoveryErr.Error())
	}
	for _, subagent := range subagents {
		path := strings.TrimSpace(subagent.FilePath)
		if path == "" {
			problems = append(problems, subagent.DisplayTitle()+" has no linked transcript")
			continue
		}
		stat, readErr := ReadCodexTokenStats(path)
		if readErr != nil {
			problems = append(problems, subagent.DisplayTitle()+": "+readErr.Error())
			continue
		}
		stats = append(stats, stat)
	}
	return summarizeCodexTokenStats(stats), len(subagents), problems, nil
}

func (b *Bridge) latestTurnForStats(ctx context.Context, sessionID string) (teamstore.Turn, bool) {
	if b == nil || b.store == nil || strings.TrimSpace(sessionID) == "" {
		return teamstore.Turn{}, false
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return teamstore.Turn{}, false
	}
	if session := state.Sessions[sessionID]; strings.TrimSpace(session.LatestTurnID) != "" {
		if turn, ok := state.Turns[session.LatestTurnID]; ok {
			return turn, true
		}
	}
	var latest teamstore.Turn
	var ok bool
	for _, turn := range state.Turns {
		if turn.SessionID != sessionID {
			continue
		}
		if !ok || turn.CreatedAt.After(latest.CreatedAt) || turn.UpdatedAt.After(latest.UpdatedAt) {
			latest = turn
			ok = true
		}
	}
	return latest, ok
}

func formatCodexTokenStatsLines(stats CodexTokenStats) []string {
	lines := []string{}
	source := strings.TrimSpace(stats.Source)
	if source == "" {
		source = "unknown"
	}
	sourceLine := "Source: " + source
	if stats.SourceLine > 0 {
		sourceLine += " at transcript line " + strconv.Itoa(stats.SourceLine)
	}
	if strings.TrimSpace(stats.SourcePath) != "" {
		sourceLine += " (" + stats.SourcePath + ")"
	}
	lines = append(lines, sourceLine)
	if stats.UsedFallbackOnly {
		lines = append(lines, "", "Reliability: using runner usage fallback because no `token_count` event was found; conversation totals and context-window analysis may be incomplete.")
	} else if stats.HasUsage() {
		lines = append(lines, "", fmt.Sprintf(
			"Reliability: reconstructed conversation usage from %d unique Codex `token_count` update(s) in local history; malformed trailing JSONL lines are ignored and reported below.",
			stats.UsageEventCount,
		))
		if stats.NonAdvancingUsageEvents > 0 || stats.NativeCounterResets > 0 || stats.NativeCounterRecoveries > 0 {
			lines = append(lines, "", fmt.Sprintf(
				"Aggregation: ignored %d non-advancing usage snapshot(s); observed %d native cumulative counter reset(s) and %d recovery event(s).",
				stats.NonAdvancingUsageEvents,
				stats.NativeCounterResets,
				stats.NativeCounterRecoveries,
			))
		}
	} else {
		lines = append(lines, "", "Reliability: Codex `token_count` metadata was found, but it did not contain a usage snapshot.")
	}
	if !stats.HasUsage() {
		lines = append(lines, "", "Token usage unavailable: no Codex usage event was found in the linked transcript.")
		if rateLines := formatCodexRateLimitLines(stats.RateLimits); len(rateLines) > 0 {
			lines = append(lines, "")
			lines = append(lines, "Rate limits:")
			lines = append(lines, rateLines...)
		}
		if diagnostics := formatTokenStatsDiagnostics(stats.Diagnostics); len(diagnostics) > 0 {
			lines = append(lines, "")
			lines = append(lines, diagnostics...)
		}
		return lines
	}
	if stats.Info.Last.hasTokens() {
		lines = append(lines, "")
		lines = append(lines, "Last recorded model usage:")
		lines = append(lines, "")
		lines = append(lines, formatTokenUsageLines(stats.Info.Last)...)
	}
	if stats.Info.Total.hasTokens() && stats.Info.Total != stats.Info.Last {
		lines = append(lines, "")
		lines = append(lines, "Conversation total:")
		lines = append(lines, "")
		lines = append(lines, formatTokenUsageLines(stats.Info.Total)...)
	}
	if modelTierLines := formatCodexModelTierUsageLines(stats.ModelTierUsages); len(modelTierLines) > 0 {
		lines = append(lines, "")
		lines = append(lines, "Model/tier usage:")
		lines = append(lines, "")
		lines = append(lines, modelTierLines...)
	}
	if modelLines := formatCodexModelUsageLines(stats.ModelUsages); len(modelLines) > 0 {
		lines = append(lines, "")
		lines = append(lines, "Model/effort usage:")
		lines = append(lines, "")
		lines = append(lines, modelLines...)
	}
	analysis := formatTokenUsageAnalysis(stats.Info)
	analysis = append(analysis, formatCodexModelTierUsageAnalysis(stats.ModelTierUsages)...)
	analysis = append(analysis, formatCodexModelUsageAnalysis(stats.ModelUsages)...)
	analysis = append(analysis, formatTokenAggregationAnalysis(stats)...)
	if len(analysis) > 0 {
		lines = append(lines, "")
		lines = append(lines, "Analysis:")
		lines = append(lines, "")
		lines = append(lines, analysis...)
	}
	if rateLines := formatCodexRateLimitLines(stats.RateLimits); len(rateLines) > 0 {
		lines = append(lines, "")
		lines = append(lines, "Rate limits:")
		lines = append(lines, rateLines...)
	}
	if diagnostics := formatTokenStatsDiagnostics(stats.Diagnostics); len(diagnostics) > 0 {
		lines = append(lines, "")
		lines = append(lines, diagnostics...)
	}
	return lines
}

func formatCodexModelTierUsageLines(usages []CodexModelTierUsage) []string {
	var lines []string
	for index, usage := range usages {
		if index > 0 {
			lines = append(lines, "")
		}
		lines = append(lines, "Model: "+normalizedCodexModelName(usage.Model))
		lines = append(lines, "Tier: "+normalizedCodexTierName(usage.Tier))
		lines = append(lines, formatTokenUsageLines(usage.Usage)...)
	}
	return lines
}

func formatCodexModelTierUsageAnalysis(usages []CodexModelTierUsage) []string {
	if len(usages) == 0 {
		return nil
	}
	unknownGroups := 0
	unknownUsage := CodexTokenUsage{}
	for _, usage := range usages {
		if normalizedCodexModelName(usage.Model) != "unknown" && normalizedCodexTierName(usage.Tier) != "unknown" {
			continue
		}
		unknownGroups++
		unknownUsage, _ = addCodexTokenUsage(unknownUsage, usage.Usage)
	}
	lines := []string{fmt.Sprintf("model/tier attribution: %d observed combination(s)", len(usages))}
	if unknownGroups > 0 {
		lines = append(lines, fmt.Sprintf(
			"%d combination(s), %s total, have missing model or service-tier metadata; they remain `unknown` instead of being guessed or merged",
			unknownGroups,
			formatTokenCount(effectiveCodexTokenUsageTotal(unknownUsage)),
		))
	}
	return lines
}

func formatCodexModelUsageLines(usages []CodexModelUsage) []string {
	var lines []string
	for index, usage := range usages {
		if index > 0 {
			lines = append(lines, "")
		}
		lines = append(lines, "Model: "+normalizedCodexModelName(usage.Model))
		lines = append(lines, "Overall:")
		lines = append(lines, formatTokenUsageLines(usage.Overall)...)
		for _, effort := range usage.EffortUsages {
			lines = append(lines, "")
			lines = append(lines, "Effort: "+normalizedCodexEffortName(effort.Effort))
			lines = append(lines, formatTokenUsageLines(effort.Usage)...)
		}
	}
	return lines
}

func formatCodexModelUsageAnalysis(usages []CodexModelUsage) []string {
	if len(usages) == 0 {
		return nil
	}
	effortGroups := 0
	unknownGroups := 0
	unknownUsage := CodexTokenUsage{}
	for _, usage := range usages {
		modelUnknown := normalizedCodexModelName(usage.Model) == "unknown"
		if modelUnknown {
			unknownGroups++
			unknownUsage, _ = addCodexTokenUsage(unknownUsage, usage.Overall)
		}
		for _, effort := range usage.EffortUsages {
			effortGroups++
			if !modelUnknown && normalizedCodexEffortName(effort.Effort) == "unknown" {
				unknownGroups++
				unknownUsage, _ = addCodexTokenUsage(unknownUsage, effort.Usage)
			}
		}
	}
	lines := []string{fmt.Sprintf("model/effort attribution: %d model(s), %d per-effort group(s)", len(usages), effortGroups)}
	if unknownGroups > 0 {
		lines = append(lines, fmt.Sprintf(
			"%d model or effort group(s), %s total, have missing metadata; they remain `unknown` instead of being guessed or merged",
			unknownGroups,
			formatTokenCount(effectiveCodexTokenUsageTotal(unknownUsage)),
		))
	}
	return lines
}

func formatTokenAggregationAnalysis(stats CodexTokenStats) []string {
	var lines []string
	if stats.NativeLatestTotal.hasTokens() && stats.NativeLatestTotal != stats.Info.Total {
		lines = append(lines, fmt.Sprintf(
			"native latest cumulative total: %s; reconstructed conversation total: %s",
			formatTokenCount(stats.NativeLatestTotal.TotalTokens),
			formatTokenCount(stats.Info.Total.TotalTokens),
		))
	}
	if stats.MissingLastUsageEvents > 0 {
		lines = append(lines, fmt.Sprintf(
			"could not verify native-counter recovery for %d advancing update(s) without `last_token_usage`; totals remain reconstructed as reset epochs",
			stats.MissingLastUsageEvents,
		))
	}
	if stats.UsageAggregationOverflow {
		lines = append(lines, "usage reconstruction overflowed int64 and was saturated; reported totals are incomplete")
	}
	return lines
}

func renderCodexTokenStatsHTML(text string) string {
	text = strings.TrimSpace(normalizeTeamsRenderText(text))
	label := teamsRenderLabel(TeamsRenderHelper, 1, 1)
	var out strings.Builder
	out.WriteString("<p><strong>")
	out.WriteString(html.EscapeString(label))
	out.WriteString(":</strong></p>")
	if text == "" {
		return out.String()
	}

	lines := compactStatsRenderLines(strings.Split(text, "\n"))
	if len(lines) == 0 {
		return out.String()
	}
	idx := 0
	if strings.EqualFold(lines[0], "STATS: Codex tokens") {
		out.WriteString("<p><strong>")
		out.WriteString(html.EscapeString(lines[0]))
		out.WriteString("</strong></p>")
		idx = 1
	}
	var meta []string
	for idx < len(lines) && !isStatsSectionHeader(lines[idx]) {
		meta = append(meta, lines[idx])
		idx++
	}
	if len(meta) > 0 {
		out.WriteString(renderStatsParagraphLinesHTML(meta))
	}
	var sections []statsRenderSection
	for idx < len(lines) {
		header := lines[idx]
		idx++
		var block []string
		scopedKind, scoped := parseStatsScopedSectionHeader(header)
		for idx < len(lines) && !isStatsSectionHeader(lines[idx]) {
			block = append(block, lines[idx])
			idx++
		}
		// Snapshot labels are nested inside the scoped snapshots section. They
		// must stay in one block so the renderer can build the comparison table;
		// they are top-level sections only for the legacy, unscoped format.
		if scoped && scopedKind == "snapshots" {
			for idx < len(lines) {
				line := lines[idx]
				if _, ok := parseStatsScopedSectionHeader(line); ok {
					break
				}
				block = append(block, line)
				idx++
			}
		}
		sections = append(sections, statsRenderSection{Header: header, Lines: block})
	}
	for idx := 0; idx < len(sections); idx++ {
		section := sections[idx]
		if scopedKind, ok := parseStatsScopedSectionHeader(section.Header); ok {
			out.WriteString("<p>&nbsp;</p>")
			out.WriteString("<p><strong>")
			out.WriteString(html.EscapeString(section.Header))
			out.WriteString("</strong></p>")
			switch scopedKind {
			case "overall":
				out.WriteString(renderStatsSingleUsageTableHTML(section.Lines))
			case "snapshots":
				out.WriteString(renderStatsUsageSnapshotsHTML(section.Lines))
			case "model/effort detail":
				out.WriteString(renderStatsModelEffortUsageHTML(section.Lines))
			case "analysis", "rate limits", "diagnostics":
				out.WriteString(renderStatsListHTML(section.Lines))
			case "not used", "unavailable":
				out.WriteString(renderStatsStatusTableHTML(section.Lines))
			default:
				out.WriteString(renderStatsParagraphLinesHTML(section.Lines))
			}
			continue
		}
		if section.Header == "Last recorded model usage:" || section.Header == "Conversation total:" {
			last := statsRenderSection{}
			conversation := statsRenderSection{}
			if section.Header == "Last recorded model usage:" {
				last = section
			} else {
				conversation = section
			}
			if idx+1 < len(sections) {
				next := sections[idx+1]
				if next.Header == "Last recorded model usage:" || next.Header == "Conversation total:" {
					if next.Header == "Last recorded model usage:" {
						last = next
					} else {
						conversation = next
					}
					idx++
				}
			}
			out.WriteString("<p>&nbsp;</p>")
			out.WriteString("<p><strong>Model usage:</strong></p>")
			out.WriteString(renderStatsUsageComparisonTableHTML(last.Lines, conversation.Lines))
			continue
		}
		out.WriteString("<p>&nbsp;</p>")
		out.WriteString("<p><strong>")
		out.WriteString(html.EscapeString(section.Header))
		out.WriteString("</strong></p>")
		switch section.Header {
		case "Analysis:", "Rate limits:":
			out.WriteString(renderStatsListHTML(section.Lines))
		case "Model/tier usage:":
			out.WriteString(renderStatsModelTierUsageHTML(section.Lines))
		case "Model/effort usage:":
			out.WriteString(renderStatsModelEffortUsageHTML(section.Lines))
		default:
			out.WriteString(renderStatsParagraphLinesHTML(section.Lines))
		}
	}
	return out.String()
}

// renderCodexTokenStatsHTMLChunks keeps helper stats as trusted HTML while
// respecting the same Graph payload budget used by the normal outbox renderer.
// Stats are rendered as tables, so chunking the source markdown would either
// lose the table structure or turn the HTML into escaped text. We therefore
// split the rendered document at section boundaries and, when necessary, at
// table rows (repeating the table header in each continuation part).
func renderCodexTokenStatsHTMLChunks(text string) []string {
	body := renderCodexTokenStatsHTML(text)
	if len(body) <= safeTeamsHTMLContentBytes {
		return []string{body}
	}
	return splitCodexTokenStatsHTML(body, teamsChunkHTMLContentBytes)
}

func splitCodexTokenStatsHTML(body string, targetBytes int) []string {
	if strings.TrimSpace(body) == "" {
		return []string{""}
	}
	if targetBytes <= 0 {
		targetBytes = teamsChunkHTMLContentBytes
	}
	prefix := renderCodexTokenStatsHTMLPrefix()
	if !strings.HasPrefix(body, prefix) {
		return []string{body}
	}
	remainder := strings.TrimPrefix(body, prefix)
	sections := strings.Split(remainder, "<p>&nbsp;</p>")
	chunks := make([]string, 0, len(sections))
	current := prefix
	flushCurrent := func() {
		if current != prefix {
			chunks = append(chunks, current)
			current = prefix
		}
	}
	appendSection := func(section string) {
		if section == "" {
			return
		}
		separator := "<p>&nbsp;</p>"
		candidate := current + separator + section
		if current == prefix {
			candidate = current + section
		}
		if len(candidate) <= targetBytes {
			current = candidate
			return
		}
		flushCurrent()
		if len(prefix+section) <= targetBytes {
			current += section
			return
		}
		for _, part := range splitOversizedCodexStatsHTMLSection(section, targetBytes-len(prefix)) {
			if part == "" {
				continue
			}
			if len(prefix+part) > targetBytes {
				// A single HTML row/value can be larger than the target. Keep it
				// intact rather than emitting malformed HTML; the hard limit is
				// still enforced by the caller's normal Graph failure handling.
				flushCurrent()
				chunks = append(chunks, prefix+part)
				continue
			}
			chunks = append(chunks, prefix+part)
		}
	}
	for _, section := range sections {
		appendSection(section)
	}
	flushCurrent()
	if len(chunks) == 0 {
		return []string{body}
	}
	return chunks
}

func renderCodexTokenStatsHTMLPrefix() string {
	label := teamsRenderLabel(TeamsRenderHelper, 1, 1)
	return "<p><strong>" + html.EscapeString(label) + ":</strong></p>"
}

func splitOversizedCodexStatsHTMLSection(section string, targetBytes int) []string {
	if targetBytes <= 0 {
		return []string{section}
	}
	if tableStart := strings.Index(section, "<table>"); tableStart >= 0 {
		if tableEndRel := strings.Index(section[tableStart+len("<table>"):], "</table>"); tableEndRel >= 0 {
			tableEnd := tableStart + len("<table>") + tableEndRel
			return splitCodexStatsHTMLTableSection(
				section[:tableStart],
				section[tableStart+len("<table>"):tableEnd],
				section[tableEnd+len("</table>"):],
				targetBytes,
			)
		}
	}
	return splitCodexStatsHTMLParagraphSection(section, targetBytes)
}

func splitCodexStatsHTMLTableSection(before string, tableBody string, after string, targetBytes int) []string {
	rows := splitCodexStatsHTMLTableRows(tableBody)
	if len(rows) <= 1 {
		return []string{before + "<table>" + tableBody + "</table>" + after}
	}
	header := rows[0]
	parts := make([]string, 0, (len(rows)+7)/8)
	currentRows := []string{header}
	flush := func(suffix string) {
		if len(currentRows) == 0 {
			return
		}
		var body strings.Builder
		body.WriteString(before)
		body.WriteString("<table>")
		body.WriteString(strings.Join(currentRows, ""))
		body.WriteString("</table>")
		body.WriteString(suffix)
		parts = append(parts, body.String())
		currentRows = []string{header}
	}
	for _, row := range rows[1:] {
		candidateRows := append(append([]string{}, currentRows...), row)
		candidate := before + "<table>" + strings.Join(candidateRows, "") + "</table>" + after
		if len(candidate) <= targetBytes {
			currentRows = candidateRows
			continue
		}
		flush("")
		candidate = before + "<table>" + header + row + "</table>" + after
		if len(candidate) <= targetBytes {
			currentRows = []string{header, row}
			continue
		}
		// Preserve one oversized row as a valid table rather than cutting
		// through tags or escaped values.
		flush("")
		parts = append(parts, before+"<table>"+header+row+"</table>"+after)
		currentRows = []string{header}
	}
	if len(currentRows) > 1 {
		flush(after)
	} else if len(parts) == 0 {
		flush(after)
	}
	return parts
}

func splitCodexStatsHTMLTableRows(tableBody string) []string {
	var rows []string
	for rest := tableBody; ; {
		start := strings.Index(rest, "<tr>")
		if start < 0 {
			break
		}
		endRel := strings.Index(rest[start:], "</tr>")
		if endRel < 0 {
			break
		}
		end := start + endRel + len("</tr>")
		rows = append(rows, rest[start:end])
		rest = rest[end:]
	}
	return rows
}

func splitCodexStatsHTMLParagraphSection(section string, targetBytes int) []string {
	start := strings.Index(section, "<p>")
	end := strings.LastIndex(section, "</p>")
	if start < 0 || end < start {
		return []string{section}
	}
	open := section[:start+len("<p>")]
	inner := section[start+len("<p>") : end]
	close := section[end:]
	lines := strings.Split(inner, "<br>")
	parts := make([]string, 0, len(lines))
	current := ""
	for _, line := range lines {
		candidateInner := line
		if current != "" {
			candidateInner = current + "<br>" + line
		}
		candidate := open + candidateInner + close
		if current == "" || len(candidate) <= targetBytes {
			current = candidateInner
			continue
		}
		parts = append(parts, open+current+close)
		current = line
	}
	if current != "" {
		parts = append(parts, open+current+close)
	}
	if len(parts) == 0 {
		return []string{section}
	}
	return parts
}

type statsRenderSection struct {
	Header string
	Lines  []string
}

func compactStatsRenderLines(lines []string) []string {
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		out = append(out, line)
	}
	return out
}

func isStatsSectionHeader(line string) bool {
	line = strings.TrimSpace(line)
	if _, ok := parseStatsScopedSectionHeader(line); ok {
		return true
	}
	switch line {
	case "Last recorded model usage:", "Conversation total:", "Model/tier usage:", "Model/effort usage:", "Analysis:", "Rate limits:":
		return true
	default:
		return line == "Main agent token usage:" || strings.HasPrefix(line, "Subagent token usage")
	}
}

func parseStatsScopedSectionHeader(line string) (string, bool) {
	line = strings.TrimSpace(line)
	const mainPrefix = "🧠 MAIN AGENT · "
	if strings.HasPrefix(line, mainPrefix) {
		kind := strings.TrimSuffix(strings.TrimSpace(strings.TrimPrefix(line, mainPrefix)), ":")
		return strings.ToLower(kind), true
	}
	if !strings.HasPrefix(line, "🧩 SUBAGENTS") {
		return "", false
	}
	separator := strings.LastIndex(line, " · ")
	if separator < 0 {
		return "", false
	}
	kind := strings.TrimSuffix(strings.TrimSpace(line[separator+len(" · "):]), ":")
	return strings.ToLower(kind), true
}

type statsModelTierRenderGroup struct {
	Model string
	Tier  string
	Lines []string
}

func renderStatsModelTierUsageHTML(lines []string) string {
	var groups []statsModelTierRenderGroup
	var current *statsModelTierRenderGroup
	for _, line := range compactStatsRenderLines(lines) {
		label, value := splitStatsLineLabelValue(line)
		switch strings.ToLower(label) {
		case "model":
			if current != nil {
				groups = append(groups, *current)
			}
			current = &statsModelTierRenderGroup{Model: value}
		case "tier":
			if current == nil {
				current = &statsModelTierRenderGroup{}
			}
			current.Tier = value
		default:
			if current == nil {
				current = &statsModelTierRenderGroup{}
			}
			current.Lines = append(current.Lines, line)
		}
	}
	if current != nil {
		groups = append(groups, *current)
	}
	if len(groups) == 0 {
		return ""
	}
	var out strings.Builder
	for index, group := range groups {
		if index > 0 {
			out.WriteString("<p>&nbsp;</p>")
		}
		out.WriteString("<p><strong>Model:</strong> ")
		out.WriteString(html.EscapeString(group.Model))
		out.WriteString("<br><strong>Tier:</strong> ")
		out.WriteString(html.EscapeString(group.Tier))
		for _, line := range compactStatsRenderLines(group.Lines) {
			out.WriteString("<br>")
			out.WriteString(renderStatsLineHTML(line))
		}
		out.WriteString("</p>")
	}
	return out.String()
}

type statsModelEffortRenderRow struct {
	Model  string
	Effort string
	Lines  []string
}

func renderStatsModelEffortUsageHTML(lines []string) string {
	var rows []statsModelEffortRenderRow
	var current *statsModelEffortRenderRow
	currentModel := ""
	flush := func() {
		if current == nil {
			return
		}
		rows = append(rows, *current)
		current = nil
	}
	for _, line := range compactStatsRenderLines(lines) {
		label, value := splitStatsLineLabelValue(line)
		switch strings.ToLower(label) {
		case "model":
			flush()
			currentModel = value
		case "overall":
			flush()
			current = &statsModelEffortRenderRow{Model: currentModel, Effort: "overall"}
		case "effort":
			flush()
			current = &statsModelEffortRenderRow{Model: currentModel, Effort: value}
		default:
			if current == nil {
				current = &statsModelEffortRenderRow{Model: currentModel}
			}
			current.Lines = append(current.Lines, line)
		}
	}
	flush()
	if len(rows) == 0 {
		return renderStatsParagraphLinesHTML(lines)
	}
	meaningful := false
	for _, row := range rows {
		if strings.TrimSpace(row.Model) != "" || strings.TrimSpace(row.Effort) != "" {
			meaningful = true
			break
		}
		values := parseStatsUsageValues(row.Lines)
		if values.Input != "" || values.CacheHitRate != "" || values.Output != "" || values.Total != "" {
			meaningful = true
			break
		}
	}
	if !meaningful {
		return renderStatsParagraphLinesHTML(lines)
	}
	var out strings.Builder
	out.WriteString("<table><tr><th>Model</th><th>Effort</th><th>input</th><th>Cache hit rate</th><th>output</th><th>total</th></tr>")
	for _, row := range rows {
		values := parseStatsUsageValues(row.Lines)
		out.WriteString("<tr><td>")
		out.WriteString(html.EscapeString(row.Model))
		out.WriteString("</td><td>")
		out.WriteString(html.EscapeString(row.Effort))
		out.WriteString("</td><td>")
		out.WriteString(renderTeamsInlineMarkdownWithLineBreaks(values.Input))
		out.WriteString("</td><td>")
		out.WriteString(renderTeamsInlineMarkdownWithLineBreaks(values.CacheHitRate))
		out.WriteString("</td><td>")
		out.WriteString(renderTeamsInlineMarkdownWithLineBreaks(values.Output))
		out.WriteString("</td><td>")
		out.WriteString(renderTeamsInlineMarkdownWithLineBreaks(values.Total))
		out.WriteString("</td></tr>")
	}
	out.WriteString("</table>")
	return out.String()
}

// renderStatsSingleUsageTableHTML renders a scope's conversation-wide usage
// without putting it next to another scope. Keeping this as a table makes the
// main-agent and subagent sections visually comparable while their headings
// remain unambiguous.
func renderStatsSingleUsageTableHTML(lines []string) string {
	values := parseStatsUsageValues(lines)
	if values.Input == "" && values.CacheHitRate == "" && values.Output == "" && values.Total == "" {
		return renderStatsParagraphLinesHTML(lines)
	}
	rows := []struct {
		Label string
		Value string
	}{
		{Label: "input", Value: values.Input},
		{Label: "Cache hit rate", Value: values.CacheHitRate},
		{Label: "output", Value: values.Output},
		{Label: "total", Value: values.Total},
	}
	var out strings.Builder
	out.WriteString("<table><tr><th>Metric</th><th>Overall</th></tr>")
	for _, row := range rows {
		if strings.TrimSpace(row.Value) == "" {
			continue
		}
		out.WriteString("<tr><td><strong>")
		out.WriteString(html.EscapeString(row.Label))
		out.WriteString("</strong></td><td>")
		out.WriteString(renderTeamsInlineMarkdownWithLineBreaks(row.Value))
		out.WriteString("</td></tr>")
	}
	out.WriteString("</table>")
	return out.String()
}

// renderStatsUsageSnapshotsHTML keeps the last snapshot and reconstructed
// conversation total together. The markers are intentionally parsed here,
// rather than being treated as independent sections, so the old useful
// snapshot comparison survives inside the new explicit main-agent scope.
func renderStatsUsageSnapshotsHTML(lines []string) string {
	var lastLines, conversationLines []string
	var target *[]string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		switch line {
		case "Last recorded model usage:":
			target = &lastLines
		case "Conversation total:":
			target = &conversationLines
		default:
			if target != nil {
				*target = append(*target, line)
			}
		}
	}
	return renderStatsUsageComparisonTableHTML(lastLines, conversationLines)
}

// renderStatsStatusTableHTML is used for the explicit no-subagent and
// unavailable states. It deliberately does not render an empty token table:
// the status itself is the useful information when there is no child history.
func renderStatsStatusTableHTML(lines []string) string {
	compact := compactStatsRenderLines(lines)
	if len(compact) == 0 {
		return ""
	}
	var out strings.Builder
	out.WriteString("<table><tr><th>Status</th><th>Value</th></tr>")
	for _, line := range compact {
		line = strings.TrimPrefix(line, "- ")
		label, value := splitStatsLineLabelValue(line)
		if label == "" {
			label = "Detail"
		}
		out.WriteString("<tr><td><strong>")
		out.WriteString(html.EscapeString(label))
		out.WriteString("</strong></td><td>")
		out.WriteString(renderTeamsInlineMarkdownWithLineBreaks(value))
		out.WriteString("</td></tr>")
	}
	out.WriteString("</table>")
	return out.String()
}

func renderStatsParagraphLinesHTML(lines []string) string {
	lines = compactStatsRenderLines(lines)
	if len(lines) == 0 {
		return ""
	}
	var out strings.Builder
	out.WriteString("<p>")
	for i, line := range lines {
		if i > 0 {
			out.WriteString("<br>")
		}
		out.WriteString(renderStatsLineHTML(line))
	}
	out.WriteString("</p>")
	return out.String()
}

type statsUsageValues struct {
	Input        string
	CacheHitRate string
	Output       string
	Total        string
}

func parseStatsUsageValues(lines []string) statsUsageValues {
	var out statsUsageValues
	for _, line := range compactStatsRenderLines(lines) {
		line = strings.TrimPrefix(strings.TrimSpace(line), "- ")
		label, value := splitStatsLineLabelValue(line)
		switch strings.ToLower(label) {
		case "input":
			out.Input = value
		case "cache hit rate":
			out.CacheHitRate = value
		case "output":
			out.Output = value
		case "total":
			out.Total = value
		}
	}
	return out
}

func renderStatsUsageComparisonTableHTML(lastLines []string, conversationLines []string) string {
	last := parseStatsUsageValues(lastLines)
	conversation := parseStatsUsageValues(conversationLines)
	type column struct {
		Header string
		Values statsUsageValues
	}
	var columns []column
	if len(compactStatsRenderLines(lastLines)) > 0 {
		columns = append(columns, column{Header: "Last recorded model usage", Values: last})
	}
	if len(compactStatsRenderLines(conversationLines)) > 0 {
		columns = append(columns, column{Header: "Conversation total", Values: conversation})
	}
	if len(columns) == 0 {
		return ""
	}
	rows := []struct {
		Label string
		Value func(statsUsageValues) string
	}{
		{Label: "input", Value: func(v statsUsageValues) string { return v.Input }},
		{Label: "Cache hit rate", Value: func(v statsUsageValues) string { return v.CacheHitRate }},
		{Label: "output", Value: func(v statsUsageValues) string { return v.Output }},
		{Label: "total", Value: func(v statsUsageValues) string { return v.Total }},
	}
	var out strings.Builder
	out.WriteString("<table><tr><th>Metric</th>")
	for _, col := range columns {
		out.WriteString("<th>")
		out.WriteString(html.EscapeString(col.Header))
		out.WriteString("</th>")
	}
	out.WriteString("</tr>")
	for _, row := range rows {
		hasValue := false
		for _, col := range columns {
			if strings.TrimSpace(row.Value(col.Values)) != "" {
				hasValue = true
				break
			}
		}
		if !hasValue {
			continue
		}
		out.WriteString("<tr><td><strong>")
		out.WriteString(html.EscapeString(row.Label))
		out.WriteString("</strong></td>")
		for _, col := range columns {
			out.WriteString("<td>")
			out.WriteString(renderTeamsInlineMarkdownWithLineBreaks(row.Value(col.Values)))
			out.WriteString("</td>")
		}
		out.WriteString("</tr>")
	}
	out.WriteString("</table>")
	return out.String()
}

func renderStatsListHTML(lines []string) string {
	lines = compactStatsRenderLines(lines)
	if len(lines) == 0 {
		return ""
	}
	var out strings.Builder
	out.WriteString("<ul>")
	for _, line := range lines {
		line = strings.TrimPrefix(strings.TrimSpace(line), "- ")
		out.WriteString("<li>")
		out.WriteString(renderStatsLineHTML(line))
		out.WriteString("</li>")
	}
	out.WriteString("</ul>")
	return out.String()
}

func renderStatsLineHTML(line string) string {
	line = strings.TrimSpace(line)
	label, rest := splitStatsLineLabelValue(line)
	if label == "" {
		return renderTeamsInlineMarkdownWithLineBreaks(line)
	}
	var out strings.Builder
	out.WriteString("<strong>")
	out.WriteString(html.EscapeString(label))
	out.WriteString(":</strong>")
	if rest != "" {
		out.WriteString(" ")
		out.WriteString(renderTeamsInlineMarkdownWithLineBreaks(rest))
	}
	return out.String()
}

func splitStatsLineLabelValue(line string) (string, string) {
	label, rest, ok := strings.Cut(strings.TrimSpace(line), ":")
	if !ok {
		return "", strings.TrimSpace(line)
	}
	label = strings.TrimSpace(label)
	if label == "" {
		return "", strings.TrimSpace(line)
	}
	return label, strings.TrimSpace(rest)
}

func formatTokenUsageLines(usage CodexTokenUsage) []string {
	lines := []string{
		"input: " + formatTokenCount(usage.InputTokens),
	}
	if usage.CachedInputTokens > 0 {
		lines[0] += " (cached " + formatTokenCount(usage.CachedInputTokens) + ", non-cached " + formatTokenCount(usage.nonCachedInputTokens()) + ")"
	}
	if pct, ok := usage.cachePercent(); ok {
		lines = append(lines, "")
		lines = append(lines, fmt.Sprintf("Cache hit rate: %.1f%%", pct))
	} else {
		lines = append(lines, "")
		lines = append(lines, "Cache hit rate: N/A")
	}
	output := "output: " + formatTokenCount(usage.OutputTokens)
	if usage.ReasoningOutputTokens > 0 {
		output += " (reasoning " + formatTokenCount(usage.ReasoningOutputTokens) + ")"
	}
	lines = append(lines, "")
	lines = append(lines, output)
	lines = append(lines, "total: "+formatTokenCount(usage.TotalTokens))
	return lines
}

func formatTokenUsageAnalysis(info CodexTokenUsageInfo) []string {
	var lines []string
	if info.ModelContextWindow > 0 {
		usage := info.Last
		if !usage.hasTokens() {
			usage = info.Total
		}
		if usage.TotalTokens > 0 {
			remaining := maxInt64(0, info.ModelContextWindow-usage.TotalTokens)
			usedPct := 100 * float64(usage.TotalTokens) / float64(info.ModelContextWindow)
			lines = append(lines, fmt.Sprintf("model context window: %s; current context uses %.1f%%; approx remaining: %s", formatTokenCount(info.ModelContextWindow), usedPct, formatTokenCount(remaining)))
			if remainingPct, ok := codexContextRemainingPercent(usage.TotalTokens, info.ModelContextWindow); ok {
				lines = append(lines, fmt.Sprintf("Codex baseline-adjusted context remaining: %d%%", remainingPct))
			}
		} else {
			lines = append(lines, "model context window: "+formatTokenCount(info.ModelContextWindow))
		}
	}
	if info.Last.ReasoningOutputTokens > 0 && info.Last.OutputTokens > 0 {
		lines = append(lines, fmt.Sprintf("last reasoning output share: %.1f%% of output tokens", 100*float64(info.Last.ReasoningOutputTokens)/float64(info.Last.OutputTokens)))
	}
	return lines
}

func codexContextRemainingPercent(usedTokens int64, contextWindow int64) (int64, bool) {
	if contextWindow <= codexContextBaselineTokens {
		return 0, false
	}
	effectiveWindow := contextWindow - codexContextBaselineTokens
	used := maxInt64(0, usedTokens-codexContextBaselineTokens)
	remaining := maxInt64(0, effectiveWindow-used)
	return int64((float64(remaining)/float64(effectiveWindow))*100 + 0.5), true
}

func formatCodexRateLimitLines(rateLimits CodexRateLimits) []string {
	if !rateLimits.Present {
		return nil
	}
	var detailLines []string
	if rateLimits.Credits.Present && (rateLimits.Credits.Unlimited || strings.TrimSpace(rateLimits.Credits.Balance) != "") {
		detailLines = append(detailLines, "- credits: "+formatCodexCreditsSnapshot(rateLimits.Credits))
	}
	for _, window := range rateLimits.Windows {
		parts := []string{window.Name}
		if window.HasUsedPercent {
			parts = append(parts, fmt.Sprintf("%.1f%% used", window.UsedPercent))
		}
		if window.HasRemaining {
			parts = append(parts, formatTokenCount(window.Remaining)+" remaining")
		}
		if window.ResetAt != "" {
			parts = append(parts, "reset "+formatRateLimitReset(window.ResetAt))
		}
		if window.HasWindowMinutes {
			parts = append(parts, "window "+formatDurationSeconds(window.WindowMinutes*60))
		}
		if window.HasWindowSeconds {
			parts = append(parts, "window "+formatDurationSeconds(window.WindowSeconds))
		}
		if len(parts) > 1 {
			detailLines = append(detailLines, "- "+strings.Join(parts, "; "))
		}
	}
	if len(detailLines) == 0 {
		return nil
	}
	var lines []string
	if rateLimits.LimitID != "" {
		lines = append(lines, "- limit id: "+rateLimits.LimitID)
	}
	if rateLimits.LimitName != "" {
		lines = append(lines, "- limit name: "+rateLimits.LimitName)
	}
	if rateLimits.PlanType != "" {
		lines = append(lines, "- plan: "+rateLimits.PlanType)
	}
	if rateLimits.ReachedType != "" {
		lines = append(lines, "- reached type: "+rateLimits.ReachedType)
	}
	return append(lines, detailLines...)
}

func formatCodexCreditsSnapshot(credits CodexCreditsSnapshot) string {
	parts := []string{}
	if credits.Unlimited {
		parts = append(parts, "unlimited")
	}
	if credits.HasCredits {
		parts = append(parts, "available")
	} else {
		parts = append(parts, "not available")
	}
	if strings.TrimSpace(credits.Balance) != "" {
		parts = append(parts, "balance "+strings.TrimSpace(credits.Balance))
	}
	return strings.Join(parts, "; ")
}

func formatRateLimitReset(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if unixSeconds, err := strconv.ParseInt(value, 10, 64); err == nil && unixSeconds > 0 {
		return time.Unix(unixSeconds, 0).UTC().Format(time.RFC3339)
	}
	return value
}

func formatTokenStatsDiagnostics(diagnostics []TokenStatsDiagnostic) []string {
	if len(diagnostics) == 0 {
		return nil
	}
	counts := make(map[string]int)
	for _, diagnostic := range diagnostics {
		counts[diagnostic.Kind]++
	}
	kinds := make([]string, 0, len(counts))
	for kind := range counts {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	parts := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		parts = append(parts, fmt.Sprintf("%s=%d", kind, counts[kind]))
	}
	return []string{"Diagnostics: skipped malformed/non-usage transcript lines (" + strings.Join(parts, ", ") + ")."}
}

func formatTokenCount(value int64) string {
	if value == 0 {
		return "0"
	}
	sign := ""
	if value < 0 {
		sign = "-"
		value = -value
	}
	s := strconv.FormatInt(value, 10)
	var groups []string
	for len(s) > 3 {
		groups = append([]string{s[len(s)-3:]}, groups...)
		s = s[:len(s)-3]
	}
	groups = append([]string{s}, groups...)
	return sign + strings.Join(groups, ",")
}

func formatDurationSeconds(seconds int64) string {
	if seconds <= 0 {
		return "0s"
	}
	return (time.Duration(seconds) * time.Second).String()
}

func firstNonZeroInt64Teams(values ...int64) int64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
