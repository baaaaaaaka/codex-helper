package engine

// `helper stats` (F-STA-1, R-42): token usage computed from the Codex
// rollouts, with v0.1.20's parser and texts and the layout the helper had
// before the rebuilt core (internal/teams/token_stats.go @ 285784d3^,
// formatWorkSessionStats and renderCodexTokenStatsHTML; owner request
// 2026-11-07): the total of the main agent and its subagents, the main
// agent (metadata, usage snapshots, overall, model/effort detail,
// analysis, rate limits, diagnostics) and the subagents, as tables.
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
		}
	}
	if len(c.subProbs) > 0 {
		// A partial sum would be a plausible-looking but incomplete number
		// (0.1.22): fail closed.
		c.subTotal, subStats = transcript.TokenUsage{}, nil
	}
	c.subMods = transcript.SumModels(subStats)
	return c
}

// workStats is formatWorkSessionStats as of 285784d3^: the header, then
// the total of the main agent and its subagents, the main agent, and the
// subagents.
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
	lines = append(lines, totalLines(c)...)
	lines = append(lines, "")
	lines = append(lines, mainAgentLines(c.main, c.notes)...)
	lines = append(lines, "")
	lines = append(lines, subagentLines(c)...)
	return strings.Join(lines, "\n")
}

// The scoped section titles of 285784d3^ (renderStats makes each a table
// or a list).
const (
	totalScope    = "🧮 TOTAL (MAIN + SUBAGENTS) · "
	mainScope     = "🧠 MAIN AGENT · "
	subagentScope = "🧩 SUBAGENTS"
	allChatsScope = "🧮 ALL WORK CHATS · "
)

// totalLines is formatCodexTotalStatsLines.
func totalLines(c chatUsage) []string {
	lines := []string{totalScope + "overall:"}
	sum, _ := transcript.AddUsage(c.mainOverall(), c.subTotal)
	if !c.complete() || !sum.HasTokens() {
		return append(lines, "Token usage unavailable: complete main and subagent usage could not be established.")
	}
	lines = append(lines, usageLines(sum)...)
	lines = append(lines, "", totalScope+"model/effort detail:")
	return append(lines, modelDetail(transcript.SumModels(c.main.Models, c.subMods))...)
}

// mainAgentLines is formatCodexMainAgentStatsLines; problems are the
// chat's source notes (source diagnostics).
func mainAgentLines(s transcript.TokenStats, problems []string) []string {
	lines := []string{mainScope + "metadata:"}
	lines = append(lines, metadataLines(s)...)
	if !s.HasUsage() {
		lines = append(lines, "", mainScope+"UNAVAILABLE:",
			"Status: no Codex usage event was found",
			"Token usage: not reported",
			"Cache hit rate: N/A",
			"Model/effort breakdown: not reported")
	} else {
		overall := s.Overall()
		lines = append(lines, "", mainScope+"snapshots:")
		if s.Info.Last.HasTokens() {
			lines = append(lines, "Last recorded model usage:")
			lines = append(lines, usageLines(s.Info.Last)...)
		}
		if s.Info.Total.HasTokens() {
			lines = append(lines, "Conversation total:")
			lines = append(lines, usageLines(s.Info.Total)...)
		}
		lines = append(lines, "", mainScope+"overall:")
		lines = append(lines, usageLines(overall)...)
		lines = append(lines, "", mainScope+"model/effort detail:")
		lines = append(lines, modelDetail(s.Models)...)
		info := s.Info
		info.Total = overall
		analysis := append(usageAnalysis(info), modelUsageAnalysis(s.Models)...)
		analysis = append(analysis, aggregationAnalysis(s)...)
		if len(analysis) > 0 {
			lines = append(lines, "", mainScope+"analysis:")
			lines = append(lines, analysis...)
		}
	}
	if rl := rateLimitLines(s.RateLimits); len(rl) > 0 {
		lines = append(lines, "", mainScope+"rate limits:")
		lines = append(lines, rl...)
	}
	if d := diagnosticLines(s.Diagnostics); len(d) > 0 {
		lines = append(lines, "", mainScope+"diagnostics:")
		lines = append(lines, d...)
	}
	if len(problems) > 0 {
		lines = append(lines, "", mainScope+"source diagnostics:")
		for _, p := range problems {
			lines = append(lines, "- "+p)
		}
	}
	return lines
}

// metadataLines is formatCodexTokenStatsMetadataLines.
func metadataLines(s transcript.TokenStats) []string {
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
		lines = append(lines, "", "Reliability: Codex `token_count` metadata was found, but it did not contain a usage snapshot.",
			"", "Token usage unavailable: no Codex usage event was found in the linked transcript.")
	}
	return lines
}

// subagentLines is formatCodexSubagentStatsLines, or
// formatCodexNoSubagentLines for a chat without subagents.
func subagentLines(c chatUsage) []string {
	if c.subs == 0 {
		return []string{
			subagentScope + " · NOT USED:",
			"Status: no user-visible subagent transcript was found",
			"Transcript count: 0",
			"Token usage: 0",
			"Cache hit rate: N/A",
			"Model/effort breakdown: not applicable",
		}
	}
	title := fmt.Sprintf("%s (%d) ·", subagentScope, c.subs)
	lines := []string{title + " overall:"}
	if c.subTotal.HasTokens() {
		lines = append(lines, usageLines(c.subTotal)...)
	} else {
		lines = append(lines, "Token usage unavailable: no readable subagent usage event was found.")
	}
	lines = append(lines, "", title+" model/effort detail:")
	lines = append(lines, modelDetail(c.subMods)...)
	if len(c.subProbs) > 0 {
		lines = append(lines, "", title+" diagnostics:")
		for _, p := range c.subProbs {
			lines = append(lines, "- "+p)
		}
	}
	return lines
}

func modelDetail(us []transcript.ModelUsage) []string {
	if ml := modelUsageLines(us); len(ml) > 0 {
		return ml
	}
	return []string{"Model/effort breakdown: unavailable"}
}

// allStats is `stats` in the control chat: every open work chat, in the
// work chats' layout (285784d3^ had no such report; its control chat
// answered "Wrong chat"). The chats are listed by activity, the most
// recent last (activity.go).
func (e *Engine) allStats(ctx context.Context) string {
	at := e.activityTimes()
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
	sort.Slice(chats, func(a, b int) bool { return byActivity(at, chats[a].cs.id, chats[b].cs.id) })
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
		chatTotal, chatModels := c.mainOverall(), c.main.Models
		if c.subs > 0 {
			if c.complete() {
				chatTotal, _ = transcript.AddUsage(chatTotal, c.subTotal)
				chatModels = transcript.SumModels(chatModels, c.subMods)
			} else {
				notes = append(notes, ch.label+": subagent usage could not be established and is not included")
			}
		}
		for _, n := range c.notes {
			notes = append(notes, ch.label+": "+n)
		}
		total, _ = transcript.AddUsage(total, chatTotal)
		models = append(models, chatModels)
		per = append(per, "Chat: "+ch.label)
		per = append(per, usageLines(chatTotal)...)
	}
	lines = append(lines, "", fmt.Sprintf("Work chats with token usage: %d of %d", counted, len(chats)))
	if counted == 0 {
		lines = append(lines, "", "Token usage unavailable: no Codex usage event was found in the Work chats' transcripts.")
	} else {
		sumModels := transcript.SumModels(models...)
		lines = append(lines, "", allChatsScope+"overall:")
		lines = append(lines, usageLines(total)...)
		lines = append(lines, "", allChatsScope+"model/effort detail:")
		lines = append(lines, modelDetail(sumModels)...)
		lines = append(lines, "", allChatsScope+"per chat:")
		lines = append(lines, per...)
		notes = append(modelUsageAnalysis(sumModels), notes...)
	}
	if len(notes) > 0 {
		lines = append(lines, "", allChatsScope+"analysis:")
		lines = append(lines, notes...)
	}
	return strings.Join(lines, "\n")
}

// usageLines is formatTokenUsageLines as of 285784d3^ (the cache hit rate
// row is always there, N/A without input).
func usageLines(u transcript.TokenUsage) []string {
	lines := []string{"input: " + tokenCount(u.Input)}
	if u.Cached > 0 {
		lines[0] += " (cached " + tokenCount(u.Cached) + ", non-cached " + tokenCount(u.NonCached()) + ")"
	}
	if pct, ok := u.CachePercent(); ok {
		lines = append(lines, "", fmt.Sprintf("Cache hit rate: %.1f%%", pct))
	} else {
		lines = append(lines, "", "Cache hit rate: N/A")
	}
	out := "output: " + tokenCount(u.Output)
	if u.Reasoning > 0 {
		out += " (reasoning " + tokenCount(u.Reasoning) + ")"
	}
	return append(lines, "", out, "total: "+tokenCount(u.Total))
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

// Rendering: renderCodexTokenStatsHTML as of 285784d3^, as Markdown for
// the channel's renderer (which splits long tables keeping their header):
// the title, the header paragraph, then each scoped section under its
// bold title as a table (usage, model/effort detail, per chat, status) or
// a list (analysis, rate limits, diagnostics).

// statsScope returns the kind of a scoped section title ("overall",
// "snapshots", "model/effort detail", …), "" for any other line
// (parseStatsScopedSectionHeader).
func statsScope(l string) string {
	if !strings.HasSuffix(l, ":") {
		return ""
	}
	for _, p := range []string{mainScope, totalScope, allChatsScope} {
		if strings.HasPrefix(l, p) {
			return strings.ToLower(strings.TrimSuffix(strings.TrimPrefix(l, p), ":"))
		}
	}
	if i := strings.LastIndex(l, " · "); strings.HasPrefix(l, subagentScope) && i >= 0 {
		return strings.ToLower(strings.TrimSuffix(l[i+len(" · "):], ":"))
	}
	return ""
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
	for ; i < len(lines) && statsScope(lines[i]) == ""; i++ {
		meta = append(meta, lines[i])
	}
	if len(meta) > 0 {
		blocks = append(blocks, statsParagraph(meta))
	}
	for i < len(lines) {
		title, kind := lines[i], statsScope(lines[i])
		var body []string
		for i++; i < len(lines) && statsScope(lines[i]) == ""; i++ {
			body = append(body, lines[i])
		}
		blocks = append(blocks, "**"+title+"**")
		if b := statsSection(kind, body); b != "" {
			blocks = append(blocks, b)
		}
	}
	return strings.Join(blocks, "\n\n")
}

// statsSection renders a section's lines by its kind; lines that do not
// make the table (an "unavailable" line) stay a paragraph.
func statsSection(kind string, body []string) string {
	var table string
	switch kind {
	case "overall":
		table = usageTable([]string{"Overall"}, [][]string{body})
	case "snapshots":
		// renderStatsUsageSnapshotsHTML: the last snapshot next to the
		// conversation total.
		var last, conv []string
		var heads []string
		var cols [][]string
		var cur *[]string
		for _, l := range body {
			switch l {
			case "Last recorded model usage:":
				cur = &last
			case "Conversation total:":
				cur = &conv
			default:
				if cur != nil {
					*cur = append(*cur, l)
				}
			}
		}
		if len(last) > 0 {
			heads, cols = append(heads, "Last recorded model usage"), append(cols, last)
		}
		if len(conv) > 0 {
			heads, cols = append(heads, "Conversation total"), append(cols, conv)
		}
		table = usageTable(heads, cols)
	case "model/effort detail":
		table = rowsTable([]string{"Model", "Effort"}, body, func(label, v string, model *string) []string {
			switch label {
			case "model":
				*model = v
			case "overall":
				return []string{*model, "overall"}
			case "effort":
				return []string{*model, v}
			}
			return nil
		})
	case "per chat":
		table = rowsTable([]string{"Chat"}, body, func(label, v string, _ *string) []string {
			if label == "chat" {
				return []string{v}
			}
			return nil
		})
	case "not used", "unavailable":
		// renderStatsStatusTableHTML.
		rows := []string{"| Status | Value |", "| --- | --- |"}
		for _, l := range body {
			label, v := statsLabel(l)
			rows = append(rows, "| **"+cell(firstNonEmpty(label, "Detail"))+"** | "+cell(v)+" |")
		}
		return strings.Join(rows, "\n")
	case "analysis", "rate limits", "diagnostics", "source diagnostics":
		return statsList(body)
	}
	if table != "" {
		return table
	}
	return statsParagraph(body)
}

var usageMetrics = []struct{ label, key string }{{"input", "input"}, {"Cache hit rate", "cache hit rate"}, {"output", "output"}, {"total", "total"}}

// statsLabel splits "Label: value" (a leading "- " is dropped); the label
// is "" for a line without one.
func statsLabel(l string) (string, string) {
	label, v, ok := strings.Cut(strings.TrimPrefix(strings.TrimSpace(l), "- "), ":")
	if !ok {
		return "", strings.TrimSpace(l)
	}
	return strings.TrimSpace(label), strings.TrimSpace(v)
}

func usageValues(lines []string) map[string]string {
	m := map[string]string{}
	for _, l := range lines {
		label, v := statsLabel(l)
		m[strings.ToLower(label)] = v
	}
	return m
}

// cell keeps a value inside its table cell.
func cell(v string) string { return strings.ReplaceAll(v, "|", `\|`) }

// usageTable is renderStatsSingleUsageTableHTML and
// renderStatsUsageComparisonTableHTML: a Metric column, then one column of
// usage each; "" when no column has a usage value.
func usageTable(heads []string, cols [][]string) string {
	vals := make([]map[string]string, len(cols))
	for i, c := range cols {
		vals[i] = usageValues(c)
	}
	rows := []string{"| Metric | " + strings.Join(heads, " | ") + " |", "|" + strings.Repeat(" --- |", len(heads)+1)}
	for _, m := range usageMetrics {
		row, has := "| **"+m.label+"** |", false
		for _, v := range vals {
			row += " " + cell(v[m.key]) + " |"
			has = has || v[m.key] != ""
		}
		if has {
			rows = append(rows, row)
		}
	}
	if len(rows) == 2 {
		return ""
	}
	return strings.Join(rows, "\n")
}

// rowsTable is renderStatsModelEffortUsageHTML: one row of usage per
// group of lines. start returns the leading cells of a row that starts at
// a line (by its lowercase label), nil for any other line; state carries
// what a line names for the rows after it (the model). "" without rows.
func rowsTable(heads, body []string, start func(label, value string, state *string) []string) string {
	type row struct {
		cells []string
		lines []string
	}
	var rows []row
	state := ""
	for _, l := range body {
		label, v := statsLabel(l)
		if cells := start(strings.ToLower(label), v, &state); cells != nil {
			rows = append(rows, row{cells: cells})
		} else if len(rows) > 0 {
			rows[len(rows)-1].lines = append(rows[len(rows)-1].lines, l)
		}
	}
	if len(rows) == 0 {
		return ""
	}
	out := []string{"| " + strings.Join(heads, " | ") + " | input | Cache hit rate | output | total |", "|" + strings.Repeat(" --- |", len(heads)+4)}
	for _, r := range rows {
		vals := usageValues(r.lines)
		line := "|"
		for _, c := range r.cells {
			line += " " + cell(c) + " |"
		}
		for _, m := range usageMetrics {
			line += " " + cell(vals[m.key]) + " |"
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

// statsLine renders "Label: value" with a bold label (renderStatsLineHTML);
// the value stays inline Markdown.
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
// quote) where the HTML showed it as text.
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
