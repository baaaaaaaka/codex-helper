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

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const codexContextBaselineTokens int64 = 12000

type CodexTokenStats struct {
	SourcePath               string
	SourceLine               int
	Source                   string
	Info                     CodexTokenUsageInfo
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

// observe returns whether the snapshot advanced usage and therefore consumed a
// pending turn boundary. Non-advancing snapshots can be metadata replays before
// the first real usage of a turn.
func (a *codexTokenUsageAccumulator) observe(info CodexTokenUsageInfo, atTurnStart bool) bool {
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
		return true
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
		return false
	}

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
	return true
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
			return finishCodexTokenStats(usage, fallback, rateLimits, diagnostics, tokenSourceLine, lastTokenCountLine), fmt.Errorf("read Codex token stats: %w", readErr)
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
				if usage.observe(tokenCount.Info, atTurnStart) {
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
	return finishCodexTokenStats(usage, fallback, rateLimits, diagnostics, tokenSourceLine, lastTokenCountLine), nil
}

func finishCodexTokenStats(
	usage codexTokenUsageAccumulator,
	fallback CodexTokenStats,
	rateLimits CodexRateLimits,
	diagnostics []TokenStatsDiagnostic,
	tokenSourceLine int,
	lastTokenCountLine int,
) CodexTokenStats {
	if usage.seen {
		return CodexTokenStats{
			SourceLine:               tokenSourceLine,
			Source:                   "token_count",
			Info:                     usage.info(),
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
		fallback.RateLimits = rateLimits
		fallback.Diagnostics = diagnostics
		return fallback
	}
	if rateLimits.Present {
		return CodexTokenStats{
			SourceLine:  lastTokenCountLine,
			Source:      "token_count",
			RateLimits:  rateLimits,
			Diagnostics: diagnostics,
		}
	}
	fallback.Diagnostics = append(fallback.Diagnostics, diagnostics...)
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
	lines = append(lines, formatCodexTokenStatsLines(stats)...)
	return strings.Join(lines, "\n")
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
	analysis := formatTokenUsageAnalysis(stats.Info)
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
		for idx < len(lines) && !isStatsSectionHeader(lines[idx]) {
			block = append(block, lines[idx])
			idx++
		}
		sections = append(sections, statsRenderSection{Header: header, Lines: block})
	}
	for idx := 0; idx < len(sections); idx++ {
		section := sections[idx]
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
		default:
			out.WriteString(renderStatsParagraphLinesHTML(section.Lines))
		}
	}
	return out.String()
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
	switch strings.TrimSpace(line) {
	case "Last recorded model usage:", "Conversation total:", "Analysis:", "Rate limits:":
		return true
	default:
		return false
	}
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
