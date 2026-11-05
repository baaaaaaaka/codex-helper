package engine

// `helper stats` (F-STA-1, R-42): token usage computed from the Codex
// rollouts, as v0.1.20 did (internal/teams/token_stats.go @ 2ae5e748,
// formatWorkSessionStats and renderCodexTokenStatsHTML), with its texts
// and layout. Additions that SPEC F-STA-1 keeps from 0.1.22 use 0.1.22's
// texts: the model/effort section, the aggregation across a chat's
// threads, and the subagent and total sections (shown only when the chat
// spawned subagents, so a chat without any reads exactly as in v0.1.20).
//
// The rollouts are read through transcript.UsageCache: a request reads
// only what was appended since the last one; nothing runs between
// requests. A command runs on its chat's command worker, never on the
// command loop or the sender.

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
)

const statsTitle = "STATS: Codex tokens"

// usageCache returns the engine's rollout usage cache (created on first
// use, in memory only: SPEC F-STA-1 persists nothing).
func (e *Engine) usageCache() *transcript.UsageCache {
	e.statsOnce.Do(func() { e.statsCache = transcript.NewUsageCache(e.cfg.SessionsDir) })
	return e.statsCache
}

// cmdStats answers `helper stats` in a work chat (cs) and `stats` in the
// control chat (cs nil: every open work chat).
func (e *Engine) cmdStats(ctx context.Context, j cmdJob, cs *convState) {
	var text string
	if cs != nil {
		text = e.workStats(ctx, cs)
	} else {
		text = e.allStats(ctx)
	}
	if ctx.Err() != nil {
		return // stopping: the command runs again after the restart
	}
	e.reply(j, renderStats(text), nil)
}

// chatUsage is one chat's usage, ready to format.
type chatUsage struct {
	label    string
	thread   string
	linked   bool  // a rollout of the chat was found
	readErr  error // every rollout failed to read
	main     transcript.TokenStats
	notes    []string // analysis notes on the main transcripts
	subs     int
	subTotal transcript.TokenUsage
	subMods  []transcript.ModelUsage
	subTiers []transcript.ModelTierUsage
	subProbs []string
}

func (c chatUsage) mainOverall() transcript.TokenUsage { return c.main.Overall() }

// complete reports whether main and subagent usage are both established.
func (c chatUsage) complete() bool {
	return c.main.HasUsage() && len(c.subProbs) == 0 && (c.subs == 0 || c.subTotal.HasTokens())
}

// threadsOf returns a chat's threads, oldest first, the current one last.
func (e *Engine) threadsOf(cs *convState, current string) []string {
	threads, err := e.st.ConvThreads(cs.id)
	if err != nil {
		e.logf("stats: threads of %d: %v", cs.id, err)
	}
	out := make([]string, 0, len(threads)+1)
	for _, t := range threads {
		if t != current {
			out = append(out, t)
		}
	}
	if current != "" {
		out = append(out, current)
	}
	return out
}

// usageOf reads the usage of the chats' threads.
func (e *Engine) usageOf(ctx context.Context, threads [][]string) ([]transcript.ChatUsage, error) {
	if e.cfg.SessionsDir == "" {
		return make([]transcript.ChatUsage, len(threads)), nil
	}
	return e.usageCache().Usage(ctx, threads)
}

// summarize turns a chat's rollouts into its usage.
func summarize(label, thread string, u transcript.ChatUsage) chatUsage {
	c := chatUsage{label: label, thread: thread}
	var stats []transcript.TokenStats
	var firstErr error
	for _, m := range u.Main {
		if m.Err != nil {
			if firstErr == nil {
				firstErr = m.Err
			}
			continue
		}
		c.linked = true
		stats = append(stats, m.Stats)
		if m.Problem != "" {
			c.notes = append(c.notes, "fork-inherited usage could not be separated ("+m.Problem+"); totals include the parent thread's history")
		}
	}
	if len(stats) == 0 && firstErr != nil {
		c.readErr = firstErr
	}
	c.main = transcript.Aggregate(stats)
	if n := len(u.Missing); n > 0 && c.linked {
		c.notes = append(c.notes, fmt.Sprintf("%d linked Codex thread(s) have no local transcript; their usage is not included", n))
	}
	c.subs = len(u.Subagents)
	var subStats []transcript.ModelUsage
	var subTiers [][]transcript.ModelTierUsage
	for _, s := range u.Subagents {
		switch {
		case s.Err != nil:
			c.subProbs = append(c.subProbs, s.Title+": "+s.Err.Error())
		case strings.HasPrefix(s.Problem, "immediate parent transcript"):
			c.subProbs = append(c.subProbs, s.Title+": "+s.Problem)
		case s.Problem != "":
			c.subProbs = append(c.subProbs, s.Title+": token usage could not be attributed to child requests")
		default:
			c.subTotal, _ = transcript.AddUsage(c.subTotal, s.Stats.Overall())
			subStats = append(subStats, s.Stats.Models...)
			subTiers = append(subTiers, s.Stats.ModelTiers)
		}
	}
	if len(c.subProbs) > 0 {
		// A partial sum would be a plausible-looking but incomplete number
		// (0.1.22): fail closed.
		c.subTotal, subStats, subTiers = transcript.TokenUsage{}, nil, nil
	}
	c.subMods = transcript.SumModels(subStats)
	c.subTiers = transcript.SumTiers(subTiers...)
	return c
}

// workStats is v0.1.20's formatWorkSessionStats.
func (e *Engine) workStats(ctx context.Context, cs *convState) string {
	e.mu.Lock()
	label, thread := cs.label, strings.TrimSpace(cs.threadID)
	e.mu.Unlock()
	lines := []string{statsTitle, "", "Session: " + label}
	if thread != "" {
		lines = append(lines, "", "Codex thread: "+thread)
	} else {
		lines = append(lines, "", "Codex thread: not linked yet")
	}
	var latest []store.Turn
	_ = e.update(func(tx *store.Tx) error {
		var err error
		latest, err = tx.RecentTurns(cs.id, 1)
		return err
	})
	if len(latest) > 0 {
		lines = append(lines, "", "Latest request: "+requestStatus(latest[0].State))
		if id := strings.TrimSpace(latest[0].CodexTurnID); id != "" {
			lines = append(lines, "", "Latest Codex turn: "+id)
		}
	}
	if thread == "" {
		return strings.Join(append(lines, "", "Token stats unavailable: this Work chat does not have a linked Codex thread yet."), "\n")
	}
	us, err := e.usageOf(ctx, [][]string{e.threadsOf(cs, thread)})
	if err != nil {
		return strings.Join(append(lines, "", "Token stats unavailable: read local Codex transcript failed: "+err.Error()), "\n")
	}
	c := summarize(label, thread, us[0])
	switch {
	case c.readErr != nil:
		return strings.Join(append(lines, "", "Token stats unavailable: read local Codex transcript failed: "+c.readErr.Error()), "\n")
	case !c.linked:
		return strings.Join(append(lines, "", "Token stats unavailable: no local Codex transcript is linked to this Work chat."), "\n")
	}
	lines = append(lines, "")
	lines = append(lines, tokenStatsLines(c.main, c.notes)...)
	if c.subs > 0 {
		lines = append(lines, "")
		lines = append(lines, subagentLines(c)...)
	}
	return strings.Join(lines, "\n")
}

// requestStatus is v0.1.20's userFacingTurnStatus over the core's turn
// states.
func requestStatus(state string) string {
	switch state {
	case store.TurnQueued:
		return "queued"
	case store.TurnStarting, store.TurnRunning:
		return "running"
	case store.TurnCompleted:
		return "completed"
	case store.TurnFailed, store.TurnNotStarted:
		return "failed"
	case store.TurnInterrupted:
		return "interrupted"
	case "":
		return "unknown"
	}
	return state
}

// subagentLines are 0.1.22's subagent and total sections
// (formatCodexSubagentStatsLines, formatCodexTotalStatsLines).
func subagentLines(c chatUsage) []string {
	title := fmt.Sprintf("🧩 SUBAGENTS (%d) ·", c.subs)
	lines := []string{title + " overall:"}
	if c.subTotal.HasTokens() {
		lines = append(lines, usageLines(c.subTotal)...)
	} else {
		lines = append(lines, "Token usage unavailable: no readable subagent usage event was found.")
	}
	lines = append(lines, "", title+" model/effort detail:")
	if ml := modelUsageLines(c.subMods); len(ml) > 0 {
		lines = append(lines, ml...)
	} else {
		lines = append(lines, "Model/effort breakdown: unavailable")
	}
	if len(c.subProbs) > 0 {
		lines = append(lines, "", title+" diagnostics:")
		for _, p := range c.subProbs {
			lines = append(lines, "- "+p)
		}
	}
	const total = "🧮 TOTAL (MAIN + SUBAGENTS)"
	lines = append(lines, "", total+" · overall:")
	if !c.complete() {
		return append(lines, "Token usage unavailable: complete main and subagent usage could not be established.")
	}
	sum, _ := transcript.AddUsage(c.mainOverall(), c.subTotal)
	lines = append(lines, usageLines(sum)...)
	lines = append(lines, "", total+" · model/effort detail:")
	if ml := modelUsageLines(transcript.SumModels(c.main.Models, c.subMods)); len(ml) > 0 {
		lines = append(lines, ml...)
	} else {
		lines = append(lines, "Model/effort breakdown: unavailable")
	}
	return lines
}

// allStats is `stats` in the control chat: every open work chat. v0.1.20
// had no such report (its control chat answered "Wrong chat"); the core's
// all-sessions stats are kept, computed from the rollouts the same way.
func (e *Engine) allStats(ctx context.Context) string {
	e.mu.Lock()
	type chat struct {
		cs            *convState
		label, thread string
	}
	var chats []chat
	for id, c := range e.convs {
		if c.kind == store.KindWork && id != e.inboxID && c.state != store.ConvClosed {
			chats = append(chats, chat{cs: c, label: c.label, thread: strings.TrimSpace(c.threadID)})
		}
	}
	e.mu.Unlock()
	sort.Slice(chats, func(a, b int) bool { return chats[a].label < chats[b].label })
	lines := []string{statsTitle, "", "Scope: all Work chats"}
	threads := make([][]string, len(chats))
	for i, c := range chats {
		if c.thread != "" {
			threads[i] = e.threadsOf(c.cs, c.thread)
		}
	}
	us, err := e.usageOf(ctx, threads)
	if err != nil {
		return strings.Join(append(lines, "", "Token stats unavailable: read local Codex transcript failed: "+err.Error()), "\n")
	}
	var total transcript.TokenUsage
	var tiers [][]transcript.ModelTierUsage
	var models [][]transcript.ModelUsage
	var per, notes []string
	counted := 0
	for i, ch := range chats {
		c := summarize(ch.label, ch.thread, us[i])
		switch {
		case ch.thread == "" || !c.linked && c.readErr == nil:
			continue
		case c.readErr != nil:
			notes = append(notes, ch.label+": read local Codex transcript failed: "+c.readErr.Error())
			continue
		case !c.main.HasUsage():
			continue
		}
		counted++
		chatTotal := c.mainOverall()
		chatModels, chatTiers := c.main.Models, c.main.ModelTiers
		if c.subs > 0 {
			if c.complete() {
				chatTotal, _ = transcript.AddUsage(chatTotal, c.subTotal)
				chatModels = transcript.SumModels(chatModels, c.subMods)
				chatTiers = transcript.SumTiers(chatTiers, c.subTiers)
			} else {
				notes = append(notes, ch.label+": subagent usage could not be established and is not included")
			}
		}
		for _, n := range c.notes {
			notes = append(notes, ch.label+": "+n)
		}
		total, _ = transcript.AddUsage(total, chatTotal)
		tiers = append(tiers, chatTiers)
		models = append(models, chatModels)
		item := "- " + ch.label + ": total " + tokenCount(chatTotal.Total)
		if pct, ok := chatTotal.CachePercent(); ok {
			item += fmt.Sprintf("; cache hit rate %.1f%%", pct)
		}
		per = append(per, item)
	}
	lines = append(lines, "", fmt.Sprintf("Work chats with token usage: %d of %d", counted, len(chats)))
	if counted == 0 {
		lines = append(lines, "", "Token usage unavailable: no Codex usage event was found in the Work chats' transcripts.")
		if len(notes) > 0 {
			lines = append(lines, "", "Analysis:", "")
			lines = append(lines, notes...)
		}
		return strings.Join(lines, "\n")
	}
	lines = append(lines, "", "Conversation total:", "")
	lines = append(lines, usageLines(total)...)
	sumTiers := transcript.SumTiers(tiers...)
	if tl := modelTierLines(sumTiers); len(tl) > 0 {
		lines = append(lines, "", "Model/tier usage:", "")
		lines = append(lines, tl...)
	}
	sumModels := transcript.SumModels(models...)
	if ml := modelUsageLines(sumModels); len(ml) > 0 {
		lines = append(lines, "", "Model/effort usage:", "")
		lines = append(lines, ml...)
	}
	lines = append(lines, "", "Per Work chat:", "")
	lines = append(lines, per...)
	analysis := append(modelTierAnalysis(sumTiers), modelUsageAnalysis(sumModels)...)
	analysis = append(analysis, notes...)
	if len(analysis) > 0 {
		lines = append(lines, "", "Analysis:", "")
		lines = append(lines, analysis...)
	}
	return strings.Join(lines, "\n")
}

// tokenStatsLines is v0.1.20's formatCodexTokenStatsLines, with 0.1.22's
// source aggregation lines and model/effort section.
func tokenStatsLines(s transcript.TokenStats, notes []string) []string {
	source := strings.TrimSpace(s.Source)
	if source == "" {
		source = "unknown"
	}
	src := "Source: " + source
	if s.SourceLine > 0 {
		src += " at transcript line " + strconv.Itoa(s.SourceLine)
	}
	if strings.TrimSpace(s.SourcePath) != "" {
		src += " (" + s.SourcePath + ")"
	}
	lines := []string{src}
	if len(s.SourcePaths) > 0 {
		lines = append(lines, "", fmt.Sprintf("Sources: %d linked parent transcripts", len(s.SourcePaths)))
		for _, p := range s.SourcePaths {
			lines = append(lines, "- "+p)
		}
	}
	if len(s.SourcePaths) > 1 {
		lines = append(lines, "", "Source aggregation: usage and model/effort details are combined across the linked parent transcripts; latest context and rate-limit metadata comes from the newest transcript.")
	}
	switch {
	case s.FallbackOnly:
		lines = append(lines, "", "Reliability: using runner usage fallback because no `token_count` event was found; conversation totals and context-window analysis may be incomplete.")
	case s.HasUsage():
		lines = append(lines, "", fmt.Sprintf("Reliability: reconstructed conversation usage from %d unique Codex `token_count` update(s) in local history; malformed trailing JSONL lines are ignored and reported below.", s.UsageEvents))
		if s.NonAdvancing > 0 || s.Resets > 0 || s.Recoveries > 0 {
			lines = append(lines, "", fmt.Sprintf("Aggregation: ignored %d non-advancing usage snapshot(s); observed %d native cumulative counter reset(s) and %d recovery event(s).", s.NonAdvancing, s.Resets, s.Recoveries))
		}
	default:
		lines = append(lines, "", "Reliability: Codex `token_count` metadata was found, but it did not contain a usage snapshot.")
	}
	if !s.HasUsage() {
		lines = append(lines, "", "Token usage unavailable: no Codex usage event was found in the linked transcript.")
		if rl := rateLimitLines(s.RateLimits); len(rl) > 0 {
			lines = append(lines, "", "Rate limits:")
			lines = append(lines, rl...)
		}
		if d := diagnosticLines(s.Diagnostics); len(d) > 0 {
			lines = append(lines, "")
			lines = append(lines, d...)
		}
		return lines
	}
	if s.Info.Last.HasTokens() {
		lines = append(lines, "", "Last recorded model usage:", "")
		lines = append(lines, usageLines(s.Info.Last)...)
	}
	if s.Info.Total.HasTokens() && s.Info.Total != s.Info.Last {
		lines = append(lines, "", "Conversation total:", "")
		lines = append(lines, usageLines(s.Info.Total)...)
	}
	if tl := modelTierLines(s.ModelTiers); len(tl) > 0 {
		lines = append(lines, "", "Model/tier usage:", "")
		lines = append(lines, tl...)
	}
	if ml := modelUsageLines(s.Models); len(ml) > 0 {
		lines = append(lines, "", "Model/effort usage:", "")
		lines = append(lines, ml...)
	}
	analysis := usageAnalysis(s.Info)
	analysis = append(analysis, modelTierAnalysis(s.ModelTiers)...)
	analysis = append(analysis, modelUsageAnalysis(s.Models)...)
	analysis = append(analysis, aggregationAnalysis(s)...)
	analysis = append(analysis, notes...)
	if len(analysis) > 0 {
		lines = append(lines, "", "Analysis:", "")
		lines = append(lines, analysis...)
	}
	if rl := rateLimitLines(s.RateLimits); len(rl) > 0 {
		lines = append(lines, "", "Rate limits:")
		lines = append(lines, rl...)
	}
	if d := diagnosticLines(s.Diagnostics); len(d) > 0 {
		lines = append(lines, "")
		lines = append(lines, d...)
	}
	return lines
}

// usageLines is v0.1.20's formatTokenUsageLines.
func usageLines(u transcript.TokenUsage) []string {
	lines := []string{"input: " + tokenCount(u.Input)}
	if u.Cached > 0 {
		lines[0] += " (cached " + tokenCount(u.Cached) + ", non-cached " + tokenCount(u.NonCached()) + ")"
	}
	if pct, ok := u.CachePercent(); ok {
		lines = append(lines, "", fmt.Sprintf("Cache hit rate: %.1f%%", pct))
	}
	out := "output: " + tokenCount(u.Output)
	if u.Reasoning > 0 {
		out += " (reasoning " + tokenCount(u.Reasoning) + ")"
	}
	return append(lines, "", out, "total: "+tokenCount(u.Total))
}

func modelTierLines(us []transcript.ModelTierUsage) []string {
	var lines []string
	for i, u := range us {
		if i > 0 {
			lines = append(lines, "")
		}
		lines = append(lines, "Model: "+u.Model, "Tier: "+u.Tier)
		lines = append(lines, usageLines(u.Usage)...)
	}
	return lines
}

func modelTierAnalysis(us []transcript.ModelTierUsage) []string {
	if len(us) == 0 {
		return nil
	}
	n := 0
	var unknown transcript.TokenUsage
	for _, u := range us {
		if u.Model != "unknown" && u.Tier != "unknown" {
			continue
		}
		n++
		unknown, _ = transcript.AddUsage(unknown, u.Usage)
	}
	lines := []string{fmt.Sprintf("model/tier attribution: %d observed combination(s)", len(us))}
	if n > 0 {
		lines = append(lines, fmt.Sprintf("%d combination(s), %s total, have missing model or service-tier metadata; they remain `unknown` instead of being guessed or merged", n, tokenCount(unknown.EffectiveTotal())))
	}
	return lines
}

// modelUsageLines is 0.1.22's formatCodexModelUsageLines.
func modelUsageLines(us []transcript.ModelUsage) []string {
	var lines []string
	for i, u := range us {
		if i > 0 {
			lines = append(lines, "")
		}
		lines = append(lines, "Model: "+u.Model, "Overall:")
		lines = append(lines, usageLines(u.Overall)...)
		for _, e := range u.Efforts {
			lines = append(lines, "", "Effort: "+e.Effort)
			lines = append(lines, usageLines(e.Usage)...)
		}
	}
	return lines
}

// modelUsageAnalysis is 0.1.22's formatCodexModelUsageAnalysis.
func modelUsageAnalysis(us []transcript.ModelUsage) []string {
	if len(us) == 0 {
		return nil
	}
	groups, n := 0, 0
	var unknown transcript.TokenUsage
	for _, u := range us {
		modelUnknown := u.Model == "unknown"
		if modelUnknown {
			n++
			unknown, _ = transcript.AddUsage(unknown, u.Overall)
		}
		for _, e := range u.Efforts {
			groups++
			if !modelUnknown && e.Effort == "unknown" {
				n++
				unknown, _ = transcript.AddUsage(unknown, e.Usage)
			}
		}
	}
	lines := []string{fmt.Sprintf("model/effort attribution: %d model(s), %d per-effort group(s)", len(us), groups)}
	if n > 0 {
		lines = append(lines, fmt.Sprintf("%d model or effort group(s), %s total, have missing metadata; they remain `unknown` instead of being guessed or merged", n, tokenCount(unknown.EffectiveTotal())))
	}
	return lines
}

// codexContextBaselineTokens is v0.1.20's context baseline.
const codexContextBaselineTokens int64 = 12000

// usageAnalysis is v0.1.20's formatTokenUsageAnalysis.
func usageAnalysis(info transcript.UsageInfo) []string {
	var lines []string
	if info.ContextWindow > 0 {
		u := info.Last
		if !u.HasTokens() {
			u = info.Total
		}
		if u.Total > 0 {
			remaining := max(0, info.ContextWindow-u.Total)
			pct := 100 * float64(u.Total) / float64(info.ContextWindow)
			lines = append(lines, fmt.Sprintf("model context window: %s; current context uses %.1f%%; approx remaining: %s", tokenCount(info.ContextWindow), pct, tokenCount(remaining)))
			if info.ContextWindow > codexContextBaselineTokens {
				eff := info.ContextWindow - codexContextBaselineTokens
				used := max(0, u.Total-codexContextBaselineTokens)
				left := max(0, eff-used)
				lines = append(lines, fmt.Sprintf("Codex baseline-adjusted context remaining: %d%%", int64((float64(left)/float64(eff))*100+0.5)))
			}
		} else {
			lines = append(lines, "model context window: "+tokenCount(info.ContextWindow))
		}
	}
	if info.Last.Reasoning > 0 && info.Last.Output > 0 {
		lines = append(lines, fmt.Sprintf("last reasoning output share: %.1f%% of output tokens", 100*float64(info.Last.Reasoning)/float64(info.Last.Output)))
	}
	return lines
}

// aggregationAnalysis is v0.1.20's formatTokenAggregationAnalysis.
func aggregationAnalysis(s transcript.TokenStats) []string {
	var lines []string
	if s.NativeLatestTotal.HasTokens() && s.NativeLatestTotal != s.Info.Total {
		lines = append(lines, fmt.Sprintf("native latest cumulative total: %s; reconstructed conversation total: %s", tokenCount(s.NativeLatestTotal.Total), tokenCount(s.Info.Total.Total)))
	}
	if s.MissingLast > 0 {
		lines = append(lines, fmt.Sprintf("could not verify native-counter recovery for %d advancing update(s) without `last_token_usage`; totals remain reconstructed as reset epochs", s.MissingLast))
	}
	if s.Overflow {
		lines = append(lines, "usage reconstruction overflowed int64 and was saturated; reported totals are incomplete")
	}
	return lines
}

// rateLimitLines is v0.1.20's formatCodexRateLimitLines.
func rateLimitLines(r transcript.RateLimits) []string {
	if !r.Present {
		return nil
	}
	var detail []string
	if r.Credits.Present && (r.Credits.Unlimited || strings.TrimSpace(r.Credits.Balance) != "") {
		var parts []string
		if r.Credits.Unlimited {
			parts = append(parts, "unlimited")
		}
		if r.Credits.HasCredits {
			parts = append(parts, "available")
		} else {
			parts = append(parts, "not available")
		}
		if b := strings.TrimSpace(r.Credits.Balance); b != "" {
			parts = append(parts, "balance "+b)
		}
		detail = append(detail, "- credits: "+strings.Join(parts, "; "))
	}
	for _, w := range r.Windows {
		parts := []string{w.Name}
		if w.HasUsedPercent {
			parts = append(parts, fmt.Sprintf("%.1f%% used", w.UsedPercent))
		}
		if w.HasRemaining {
			parts = append(parts, tokenCount(w.Remaining)+" remaining")
		}
		if w.ResetAt != "" {
			parts = append(parts, "reset "+rateReset(w.ResetAt))
		}
		if w.HasWindowMinutes {
			parts = append(parts, "window "+durationSeconds(w.WindowMinutes*60))
		}
		if w.HasWindowSeconds {
			parts = append(parts, "window "+durationSeconds(w.WindowSeconds))
		}
		if len(parts) > 1 {
			detail = append(detail, "- "+strings.Join(parts, "; "))
		}
	}
	if len(detail) == 0 {
		return nil
	}
	var lines []string
	if r.LimitID != "" {
		lines = append(lines, "- limit id: "+r.LimitID)
	}
	if r.LimitName != "" {
		lines = append(lines, "- limit name: "+r.LimitName)
	}
	if r.PlanType != "" {
		lines = append(lines, "- plan: "+r.PlanType)
	}
	if r.ReachedType != "" {
		lines = append(lines, "- reached type: "+r.ReachedType)
	}
	return append(lines, detail...)
}

func rateReset(v string) string {
	v = strings.TrimSpace(v)
	if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
		return time.Unix(n, 0).UTC().Format(time.RFC3339)
	}
	return v
}

func durationSeconds(s int64) string {
	if s <= 0 {
		return "0s"
	}
	return (time.Duration(s) * time.Second).String()
}

// diagnosticLines is v0.1.20's formatTokenStatsDiagnostics.
func diagnosticLines(d map[string]int) []string {
	if len(d) == 0 {
		return nil
	}
	kinds := make([]string, 0, len(d))
	for k := range d {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	parts := make([]string, 0, len(kinds))
	for _, k := range kinds {
		parts = append(parts, fmt.Sprintf("%s=%d", k, d[k]))
	}
	return []string{"Diagnostics: skipped malformed/non-usage transcript lines (" + strings.Join(parts, ", ") + ")."}
}

// tokenCount is v0.1.20's formatTokenCount.
func tokenCount(v int64) string {
	if v == 0 {
		return "0"
	}
	sign := ""
	if v < 0 {
		sign, v = "-", -v
	}
	s := strconv.FormatInt(v, 10)
	var groups []string
	for len(s) > 3 {
		groups = append([]string{s[len(s)-3:]}, groups...)
		s = s[:len(s)-3]
	}
	return sign + strings.Join(append([]string{s}, groups...), ",")
}

// Rendering: v0.1.20's renderCodexTokenStatsHTML, as Markdown for the
// channel's renderer (which splits long tables keeping their header).

func isStatsHeader(l string) bool {
	switch l {
	case "Last recorded model usage:", "Conversation total:", "Model/tier usage:", "Model/effort usage:", "Analysis:", "Rate limits:", "Per Work chat:":
		return true
	}
	return (strings.HasPrefix(l, "🧩 SUBAGENTS") || strings.HasPrefix(l, "🧮 TOTAL")) && strings.HasSuffix(l, ":")
}

type statsSection struct {
	header string
	lines  []string
}

// renderStats renders the text lines of a stats report.
func renderStats(text string) string {
	var lines []string
	for _, l := range strings.Split(text, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			lines = append(lines, l)
		}
	}
	var blocks []string
	i := 0
	if len(lines) > 0 && lines[0] == statsTitle {
		blocks = append(blocks, "**"+statsTitle+"**")
		i = 1
	}
	var meta []string
	for ; i < len(lines) && !isStatsHeader(lines[i]); i++ {
		meta = append(meta, lines[i])
	}
	if len(meta) > 0 {
		blocks = append(blocks, statsParagraph(meta))
	}
	var secs []statsSection
	for i < len(lines) {
		s := statsSection{header: lines[i]}
		for i++; i < len(lines) && !isStatsHeader(lines[i]); i++ {
			s.lines = append(s.lines, lines[i])
		}
		secs = append(secs, s)
	}
	for k := 0; k < len(secs); k++ {
		s := secs[k]
		if s.header == "Last recorded model usage:" || s.header == "Conversation total:" {
			var last, conv *statsSection
			pick := func(x *statsSection) {
				if x.header == "Last recorded model usage:" {
					last = x
				} else {
					conv = x
				}
			}
			pick(&secs[k])
			if k+1 < len(secs) && (secs[k+1].header == "Last recorded model usage:" || secs[k+1].header == "Conversation total:") && secs[k+1].header != s.header {
				pick(&secs[k+1])
				k++
			}
			blocks = append(blocks, "**Model usage:**", usageTable(last, conv))
			continue
		}
		blocks = append(blocks, "**"+mdEscapeLine(s.header)+"**")
		switch {
		case s.header == "Analysis:" || s.header == "Rate limits:" || s.header == "Per Work chat:" || strings.HasSuffix(s.header, " diagnostics:"):
			blocks = append(blocks, statsList(s.lines))
		case s.header == "Model/tier usage:" || s.header == "Model/effort usage:" || strings.HasSuffix(s.header, " model/effort detail:"):
			blocks = append(blocks, statsGroups(s.lines)...)
		default:
			blocks = append(blocks, statsParagraph(s.lines))
		}
	}
	return strings.Join(blocks, "\n\n")
}

// statsLine renders "Label: value" with a bold label (v0.1.20
// renderStatsLineHTML); the value stays inline Markdown.
func statsLine(l string) string {
	label, rest, ok := strings.Cut(strings.TrimSpace(l), ":")
	label = strings.TrimSpace(label)
	if !ok || label == "" {
		return mdEscapeLine(l)
	}
	out := "**" + label + ":**"
	if rest = strings.TrimSpace(rest); rest != "" {
		out += " " + rest
	}
	return out
}

// mdEscapeLine keeps a line from starting a Markdown block (list, heading,
// quote) where v0.1.20's HTML showed it as text.
func mdEscapeLine(l string) string {
	if l != "" && strings.ContainsRune("-+*#>|", rune(l[0])) {
		return `\` + l
	}
	return l
}

func statsParagraph(lines []string) string {
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = statsLine(l)
	}
	return strings.Join(out, "\n")
}

func statsList(lines []string) string {
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = "- " + statsLine(strings.TrimPrefix(l, "- "))
	}
	return strings.Join(out, "\n")
}

// statsGroups renders model groups as one paragraph each (v0.1.20
// renderStatsModelTierUsageHTML); an effort starts a paragraph too.
func statsGroups(lines []string) []string {
	var out []string
	var cur []string
	flush := func() {
		if len(cur) > 0 {
			out = append(out, statsParagraph(cur))
			cur = nil
		}
	}
	for _, l := range lines {
		if strings.HasPrefix(l, "Model:") || strings.HasPrefix(l, "Effort:") {
			flush()
		}
		cur = append(cur, l)
	}
	flush()
	return out
}

// usageTable is v0.1.20's renderStatsUsageComparisonTableHTML.
func usageTable(last, conv *statsSection) string {
	type col struct {
		header string
		vals   map[string]string
	}
	values := func(s *statsSection) map[string]string {
		m := map[string]string{}
		for _, l := range s.lines {
			label, v, _ := strings.Cut(strings.TrimPrefix(l, "- "), ":")
			m[strings.ToLower(strings.TrimSpace(label))] = strings.TrimSpace(v)
		}
		return m
	}
	var cols []col
	if last != nil && len(last.lines) > 0 {
		cols = append(cols, col{"Last recorded model usage", values(last)})
	}
	if conv != nil && len(conv.lines) > 0 {
		cols = append(cols, col{"Conversation total", values(conv)})
	}
	if len(cols) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("| Metric |")
	for _, c := range cols {
		b.WriteString(" " + c.header + " |")
	}
	b.WriteString("\n| --- |")
	for range cols {
		b.WriteString(" --- |")
	}
	for _, row := range []struct{ label, key string }{{"input", "input"}, {"Cache hit rate", "cache hit rate"}, {"output", "output"}, {"total", "total"}} {
		has := false
		for _, c := range cols {
			if c.vals[row.key] != "" {
				has = true
			}
		}
		if !has {
			continue
		}
		b.WriteString("\n| **" + row.label + "** |")
		for _, c := range cols {
			b.WriteString(" " + c.vals[row.key] + " |")
		}
	}
	return b.String()
}
