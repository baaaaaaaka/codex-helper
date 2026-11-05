package transcript

// Token usage of a Codex rollout, as v0.1.20's `helper stats` computed it
// (internal/teams/token_stats.go @ 2ae5e748, ParseCodexTokenStats): the
// cumulative token_count snapshots rebuilt into a conversation total across
// Codex counter resets, the usage per model and service tier with an
// explicit `unknown` bucket, the last call, the context window, the rate
// limits and the malformed-line diagnostics. The per-effort breakdown and
// the attribution of a fork child (inherited prefix removed) are those of
// 0.1.22 (160c670, 4c123af), which SPEC F-STA-1 / R-42 keep.
//
// The state is incremental: the usage cache (usage_cache.go) feeds each
// rollout's appended lines once and keeps the state, so a stats request
// never reads a byte twice. A line is decoded only when it can change the
// result; the large tool and message records are classified from their
// first bytes (see consume).

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"maps"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// TokenUsage is one token count vector.
type TokenUsage struct {
	Input, Cached, Output, Reasoning, Total int64
}

// HasTokens reports whether any component is set.
func (u TokenUsage) HasTokens() bool {
	return u.Input != 0 || u.Cached != 0 || u.Output != 0 || u.Reasoning != 0 || u.Total != 0
}

// NonCached is the input that missed the cache.
func (u TokenUsage) NonCached() int64 {
	if u.Input <= 0 || u.Cached <= 0 {
		if u.Input > 0 {
			return u.Input
		}
		return 0
	}
	return max(0, u.Input-u.Cached)
}

// CachePercent is the cache hit rate of the input (false: not defined).
func (u TokenUsage) CachePercent() (float64, bool) {
	if u.Input <= 0 || u.Cached <= 0 {
		return 0, false
	}
	return 100 * float64(u.Cached) / float64(u.Input), true
}

// EffectiveTotal is Total, or input + output when Codex left it out.
func (u TokenUsage) EffectiveTotal() int64 {
	if u.Total != 0 {
		return u.Total
	}
	t, _ := satAdd(u.Input, u.Output)
	return t
}

// AddUsage adds two vectors, saturating; overflow reports a saturation.
func AddUsage(a, b TokenUsage) (TokenUsage, bool) {
	in, o1 := satAdd(a.Input, b.Input)
	ca, o2 := satAdd(a.Cached, b.Cached)
	out, o3 := satAdd(a.Output, b.Output)
	re, o4 := satAdd(a.Reasoning, b.Reasoning)
	to, o5 := satAdd(a.Total, b.Total)
	return TokenUsage{in, ca, out, re, to}, o1 || o2 || o3 || o4 || o5
}

func satAdd(a, b int64) (int64, bool) {
	if a < 0 || b < 0 {
		return a, true
	}
	if a > math.MaxInt64-b {
		return math.MaxInt64, true
	}
	return a + b, false
}

func subUsage(cur, prev TokenUsage) TokenUsage {
	sub := func(c, p int64) int64 {
		if c < 0 || p < 0 || c < p {
			return max(0, c)
		}
		return c - p
	}
	return TokenUsage{sub(cur.Input, prev.Input), sub(cur.Cached, prev.Cached), sub(cur.Output, prev.Output),
		sub(cur.Reasoning, prev.Reasoning), sub(cur.Total, prev.Total)}
}

func compareTotals(a, b TokenUsage) int {
	l, r := a.EffectiveTotal(), b.EffectiveTotal()
	switch {
	case l < r:
		return -1
	case l > r:
		return 1
	}
	return 0
}

func componentDecreased(cur, prev TokenUsage) bool {
	c := [...]int64{cur.Input, cur.Cached, cur.Output, cur.Reasoning}
	p := [...]int64{prev.Input, prev.Cached, prev.Output, prev.Reasoning}
	for i := range c {
		// A zero can mean that an older event schema omitted the component.
		if c[i] > 0 && p[i] > 0 && c[i] < p[i] {
			return true
		}
	}
	return false
}

// UsageInfo is the conversation total, the last model call and the
// context window.
type UsageInfo struct {
	Total, Last   TokenUsage
	ContextWindow int64
}

// ModelTierUsage is the usage of one model and service tier.
type ModelTierUsage struct {
	Model, Tier string
	Usage       TokenUsage
}

// EffortUsage is the usage of one reasoning effort.
type EffortUsage struct {
	Effort string
	Usage  TokenUsage
}

// ModelUsage is the usage of one model, overall and per effort.
type ModelUsage struct {
	Model   string
	Overall TokenUsage
	Efforts []EffortUsage
}

// RateLimits is the last rate-limit snapshot of a rollout.
type RateLimits struct {
	Present     bool
	LimitID     string
	LimitName   string
	PlanType    string
	ReachedType string
	Credits     Credits
	Windows     []RateWindow
}

// Credits is the credits part of a rate-limit snapshot.
type Credits struct {
	Present, HasCredits, Unlimited bool
	Balance                        string
}

// RateWindow is one rate-limit window.
type RateWindow struct {
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

// ParseTokenStats reads a whole rollout once, like v0.1.20's
// ParseCodexTokenStats, including a last line without a newline (the usage
// cache leaves such a line until it is complete).
func ParseTokenStats(r io.Reader) (TokenStats, error) {
	st := newUsageState(false)
	_, err := feedReader(context.Background(), io.MultiReader(r, strings.NewReader("\n")), st, 0, nil)
	out, _ := st.stats()
	return out, err
}

// TokenStats is the usage report of one rollout (or several, aggregated).
type TokenStats struct {
	SourcePath        string
	SourcePaths       []string // aggregated rollouts (Aggregate)
	SourceLine        int
	Source            string // "token_count", "event usage", or an aggregation note
	Info              UsageInfo
	ModelTiers        []ModelTierUsage
	Models            []ModelUsage
	NativeLatestTotal TokenUsage
	UsageEvents       int
	NonAdvancing      int
	Resets            int
	Recoveries        int
	MissingLast       int
	Overflow          bool
	RateLimits        RateLimits
	Diagnostics       map[string]int // kind -> lines
	FallbackOnly      bool
}

// HasUsage reports whether the rollout recorded any token usage.
func (s TokenStats) HasUsage() bool { return s.Info.Total.HasTokens() || s.Info.Last.HasTokens() }

// Overall is the conversation total (or the last call when there is none).
func (s TokenStats) Overall() TokenUsage {
	if s.Info.Total.HasTokens() {
		return s.Info.Total
	}
	return s.Info.Last
}

// usageAcc rebuilds the conversation total from cumulative snapshots
// (v0.1.20 codexTokenUsageAccumulator).
type usageAcc struct {
	Seen           bool
	Total          TokenUsage
	CompletedEpoch TokenUsage
	Last           TokenUsage
	NativeLatest   TokenUsage
	Previous       TokenUsage
	ContextWindow  int64
	Events         int
	NonAdvancing   int
	Resets         int
	Recoveries     int
	Global         bool
	MissingLast    int
	Overflow       bool
}

// observe returns the newly observed usage and whether the snapshot advanced.
func (a *usageAcc) observe(info UsageInfo, atTurnStart bool) (TokenUsage, bool) {
	native := info.Total
	if !native.HasTokens() {
		native = info.Last
	}
	if !a.Seen {
		// Seeded from the first cumulative snapshot so that a reader starting
		// after earlier model calls still works.
		a.Seen = true
		a.Total = native
		if !a.Total.HasTokens() {
			a.Total = info.Last
		}
		a.Last = info.Last
		a.NativeLatest = native
		a.Previous = native
		a.ContextWindow = info.ContextWindow
		a.Events = 1
		a.Global = true
		return native, true
	}
	progress := compareTotals(native, a.Previous)
	componentReset := atTurnStart && componentDecreased(native, a.Previous)
	if progress == 0 && !componentReset {
		// A repeated cumulative snapshot (last usage or rate metadata refresh).
		a.NonAdvancing++
		if a.Global {
			a.Total = native
		}
		if info.Last.HasTokens() {
			a.Last = info.Last
		}
		if info.ContextWindow > 0 {
			a.ContextWindow = info.ContextWindow
		}
		a.NativeLatest = native
		a.Previous = native
		return TokenUsage{}, false
	}
	prevTotal := a.Total
	a.Events++
	resetAtTurnStart := componentReset || (atTurnStart && progress > 0 && info.Last.HasTokens() && native == info.Last)
	switch {
	case progress < 0 || resetAtTurnStart:
		a.Resets++
		a.CompletedEpoch = a.Total
		// The first snapshot of a reset epoch may already include several
		// model calls: the whole new epoch is added.
		var over bool
		a.Total, over = AddUsage(a.CompletedEpoch, native)
		a.Overflow = a.Overflow || over
		a.Global = false
	case a.Global:
		a.Total = native
	case info.Last.HasTokens():
		// A fixed Codex may restore the conversation-global counter: only on
		// the first snapshot of a turn and only when the native total is
		// exactly the previous total plus the last call.
		cand, over := AddUsage(a.Total, info.Last)
		if atTurnStart && !over && native == cand {
			a.Total = native
			a.CompletedEpoch = TokenUsage{}
			a.Global = true
			a.Recoveries++
		} else {
			var over bool
			a.Total, over = AddUsage(a.CompletedEpoch, native)
			a.Overflow = a.Overflow || over
		}
	default:
		var over bool
		a.Total, over = AddUsage(a.CompletedEpoch, native)
		a.Overflow = a.Overflow || over
		a.MissingLast++
	}
	if info.Last.HasTokens() {
		a.Last = info.Last
	}
	if info.ContextWindow > 0 {
		a.ContextWindow = info.ContextWindow
	}
	a.NativeLatest = native
	a.Previous = native
	return subUsage(a.Total, prevTotal), true
}

// usageCtx is the model, service tier and effort of the running turn.
type usageCtx struct {
	Model, Tier, Effort, TurnID string
}

type groupKey struct {
	Model, Tier, Effort string
}

// modelAcc attributes usage to the model, tier and effort in effect
// (v0.1.20 codexModelTierUsageAccumulator, with 0.1.22's effort).
type modelAcc struct {
	Cur          usageCtx
	CurTurn      string
	HasCtx       bool
	Awaiting     bool
	TurnHasUsage bool
	Fallback     TokenUsage
	Groups       map[groupKey]TokenUsage // all usage
	Own          map[groupKey]TokenUsage // a fork child's own usage
}

func (a *modelAcc) applySettings(c usageCtx) {
	a.finishTurn()
	a.Cur = c
	a.CurTurn = ""
	a.HasCtx = true
	a.Awaiting = true
	a.resetTurn()
}

func (a *modelAcc) applyTurnContext(c usageCtx) {
	if !a.HasCtx {
		a.Cur, a.CurTurn, a.HasCtx, a.Awaiting = c, c.TurnID, true, false
		return
	}
	if c.TurnID != "" && a.CurTurn != "" && c.TurnID != a.CurTurn {
		a.finishTurn()
		a.Cur, a.CurTurn, a.Awaiting = c, c.TurnID, false
		a.resetTurn()
		return
	}
	if c.TurnID != "" && a.CurTurn == "" && !a.Awaiting && (a.TurnHasUsage || a.Fallback.HasTokens()) {
		a.finishTurn()
		a.Cur, a.CurTurn = c, c.TurnID
		a.resetTurn()
	} else {
		if c.Model != "" {
			a.Cur.Model = c.Model
		}
		if c.Tier != "" {
			a.Cur.Tier = c.Tier
		}
		if c.Effort != "" {
			a.Cur.Effort = c.Effort
		}
		if c.TurnID != "" {
			a.CurTurn = c.TurnID
		}
	}
	a.Awaiting = false
}

func (a *modelAcc) key() groupKey {
	return groupKey{Model: unknownIfEmpty(a.Cur.Model), Tier: unknownIfEmpty(a.Cur.Tier), Effort: unknownIfEmpty(a.Cur.Effort)}
}

// observeUsage records an advancing delta; own false marks a delta a fork
// child inherited from its parent.
func (a *modelAcc) observeUsage(delta TokenUsage, advanced, own bool) {
	if !advanced || !delta.HasTokens() {
		return
	}
	a.TurnHasUsage = true
	a.Fallback = TokenUsage{}
	k := a.key()
	a.Groups[k], _ = AddUsage(a.Groups[k], delta)
	if own {
		a.Own[k], _ = AddUsage(a.Own[k], delta)
	}
}

func (a *modelAcc) observeFallback(u TokenUsage) {
	if a.TurnHasUsage || !u.HasTokens() {
		return
	}
	a.Fallback = u
}

func (a *modelAcc) finishTurn() {
	if a.TurnHasUsage || !a.Fallback.HasTokens() {
		a.resetTurn()
		return
	}
	k := a.key()
	a.Groups[k], _ = AddUsage(a.Groups[k], a.Fallback)
	a.Own[k], _ = AddUsage(a.Own[k], a.Fallback)
	a.resetTurn()
}

func (a *modelAcc) resetTurn() {
	a.TurnHasUsage = false
	a.Fallback = TokenUsage{}
}

func unknownIfEmpty(s string) string {
	if s = strings.TrimSpace(s); s == "" {
		return "unknown"
	}
	return s
}

// tierView sums the groups per model and tier, sorted.
func tierView(groups map[groupKey]TokenUsage) []ModelTierUsage {
	type mt struct{ m, t string }
	sum := map[mt]TokenUsage{}
	for k, u := range groups {
		key := mt{k.Model, k.Tier}
		sum[key], _ = AddUsage(sum[key], u)
	}
	out := make([]ModelTierUsage, 0, len(sum))
	for k, u := range sum {
		if u.HasTokens() {
			out = append(out, ModelTierUsage{Model: k.m, Tier: k.t, Usage: u})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Model != out[j].Model {
			return out[i].Model < out[j].Model
		}
		return out[i].Tier < out[j].Tier
	})
	return out
}

// modelView sums the groups per model and effort, sorted (0.1.22 modelSnapshot).
func modelView(groups map[groupKey]TokenUsage) []ModelUsage {
	type mg struct {
		overall TokenUsage
		efforts map[string]TokenUsage
	}
	byModel := map[string]*mg{}
	for k, u := range groups {
		if !u.HasTokens() {
			continue
		}
		g := byModel[k.Model]
		if g == nil {
			g = &mg{efforts: map[string]TokenUsage{}}
			byModel[k.Model] = g
		}
		g.overall, _ = AddUsage(g.overall, u)
		g.efforts[k.Effort], _ = AddUsage(g.efforts[k.Effort], u)
	}
	models := make([]string, 0, len(byModel))
	for m, g := range byModel {
		if g.overall.HasTokens() {
			models = append(models, m)
		}
	}
	sort.Strings(models)
	out := make([]ModelUsage, 0, len(models))
	for _, m := range models {
		g := byModel[m]
		mu := ModelUsage{Model: m, Overall: g.overall}
		for e, u := range g.efforts {
			if u.HasTokens() {
				mu.Efforts = append(mu.Efforts, EffortUsage{Effort: e, Usage: u})
			}
		}
		sort.Slice(mu.Efforts, func(i, j int) bool { return LessEffort(mu.Efforts[i].Effort, mu.Efforts[j].Effort) })
		out = append(out, mu)
	}
	return out
}

// SumModels adds per-model usages (0.1.22 summarizeCodexTokenStats).
func SumModels(lists ...[]ModelUsage) []ModelUsage {
	groups := map[groupKey]TokenUsage{}
	for _, l := range lists {
		for _, m := range l {
			for _, e := range m.Efforts {
				k := groupKey{Model: unknownIfEmpty(m.Model), Effort: unknownIfEmpty(e.Effort)}
				groups[k], _ = AddUsage(groups[k], e.Usage)
			}
		}
	}
	return modelView(groups)
}

// SumTiers adds per-model-and-tier usages.
func SumTiers(lists ...[]ModelTierUsage) []ModelTierUsage {
	groups := map[groupKey]TokenUsage{}
	for _, l := range lists {
		for _, m := range l {
			k := groupKey{Model: unknownIfEmpty(m.Model), Tier: unknownIfEmpty(m.Tier)}
			groups[k], _ = AddUsage(groups[k], m.Usage)
		}
	}
	return tierView(groups)
}

// LessEffort orders efforts low < medium < high < xhigh < max < others < unknown.
func LessEffort(a, b string) bool {
	oa, ob := effortOrder(a), effortOrder(b)
	if oa != ob {
		return oa < ob
	}
	return a < b
}

func effortOrder(e string) int {
	switch strings.ToLower(unknownIfEmpty(e)) {
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
	}
	return 50
}

// TimelineObs is one advancing cumulative usage snapshot of a rollout: the
// ledger a fork child's inherited prefix is matched against (0.1.22
// codexUsageObservation, cumulative entries only).
type TimelineObs struct {
	Native TokenUsage
	Time   int64 // unix nanoseconds; 0 when the record had no timestamp
}

// Attribution modes of a fork child.
const (
	attrMatching = iota // still inside the prefix copied from the parent
	attrOwned           // past the prefix: the child's own usage
	attrFailed          // the inherited prefix cannot be proven
)

// Attribution texts (0.1.22 attributeCodexChildTokenStats).
const (
	errAttrMixed       = "child transcript mixes cumulative and non-cumulative usage events"
	errAttrNoLedger    = "immediate parent transcript has no cumulative token ledger"
	errAttrIncomplete  = "immediate parent transcript has an incomplete cumulative token ledger"
	errAttrNoPrefix    = "child cumulative token ledger has no provable inherited prefix or independent counter start"
	errAttrChildLedger = "child transcript has usage but no cumulative token ledger"
)

// attrState splits a fork child's usage into the prefix inherited from its
// immediate parent and its own requests, streaming (0.1.22 matched the
// whole child ledger against the whole parent ledger; matching record by
// record gives the same prefix, because the copied prefix is written
// before the child's own requests).
type attrState struct {
	Mode    int
	Matched int // parent snapshots matched so far
	Reason  string
}

// parentLedger is what attribution needs of the immediate parent.
type parentLedger struct {
	TL         []TimelineObs
	Incomplete bool // the parent had non-cumulative usage events
}

// own decides one advancing observation of the child.
func (a *attrState) own(native TokenUsage, at int64, cumulative bool, p *parentLedger) bool {
	switch a.Mode {
	case attrOwned:
		if !cumulative {
			a.fail(errAttrMixed)
			return false
		}
		return true
	case attrFailed:
		return false
	}
	if !cumulative {
		a.fail(errAttrMixed)
		return false
	}
	if p == nil || len(p.TL) == 0 {
		a.fail(errAttrNoLedger)
		return false
	}
	if p.Incomplete {
		a.fail(errAttrIncomplete)
		return false
	}
	if a.Matched < len(p.TL) && p.TL[a.Matched].Native == native {
		a.Matched++
		return false
	}
	if a.Matched == 0 && !independentStart(p.TL, native, at) {
		a.fail(errAttrNoPrefix)
		return false
	}
	a.Mode = attrOwned
	return true
}

func (a *attrState) fail(reason string) {
	if a.Mode != attrFailed {
		a.Mode, a.Reason = attrFailed, reason
	}
}

// independentStart: a child that inherited no usage starts its own
// counter below the parent's counter at the child's start (0.1.22
// codexChildHasIndependentCounter).
func independentStart(parent []TimelineObs, native TokenUsage, at int64) bool {
	if at == 0 {
		return false
	}
	var p TokenUsage
	found := false
	for _, o := range parent {
		if o.Time != 0 && o.Time > at {
			break
		}
		p, found = o.Native, true
	}
	return found && native.EffectiveTotal() < p.EffectiveTotal()
}

// usageState is the incremental parse state of one rollout.
type usageState struct {
	Lines         int // lines consumed
	Acc           usageAcc
	Models        modelAcc
	Rate          RateLimits
	Diag          map[string]int
	FallbackLine  int
	FallbackUsage TokenUsage
	TokenLine     int
	LastCountLine int
	AtTurnStart   bool
	// The advancing cumulative snapshots (the ledger a child matches) and
	// whether a non-cumulative usage event occurred.
	TL         []TimelineObs
	NonCum     bool
	OwnEvents  int        // observations attributed to the rollout itself
	OwnTotal   TokenUsage // their sum
	Attr       *attrState // nil unless the rollout is a fork child
	ChildUsage bool       // a fork child recorded usage
}

func newUsageState(child bool) *usageState {
	s := &usageState{Models: modelAcc{Groups: map[groupKey]TokenUsage{}, Own: map[groupKey]TokenUsage{}}, Diag: map[string]int{}}
	if child {
		s.Attr = &attrState{}
	}
	return s
}

func (s *usageState) clone() *usageState {
	c := *s
	c.Models.Groups = maps.Clone(s.Models.Groups)
	c.Models.Own = maps.Clone(s.Models.Own)
	c.Diag = maps.Clone(s.Diag)
	if c.Models.Groups == nil {
		c.Models.Groups = map[groupKey]TokenUsage{}
	}
	if c.Models.Own == nil {
		c.Models.Own = map[groupKey]TokenUsage{}
	}
	if c.Diag == nil {
		c.Diag = map[string]int{}
	}
	if s.Attr != nil {
		a := *s.Attr
		c.Attr = &a
	}
	c.TL = s.TL[:len(s.TL):len(s.TL)]
	return &c
}

var (
	jTokenCount = []byte("token_count")
	jSettings   = []byte("thread_settings_applied")
)

// skippable reports, from the first bytes of a record, that v0.1.20 would
// only have decoded it to look for usage it never carries: the session
// header, a compaction, or a response item or event that is not a token
// count or a settings change (tool output, messages, reasoning). Such a
// record is not decoded; it counts as malformed only when it does not end
// like a JSON object.
func skippable(h []byte) bool {
	t, ok := topType(h)
	if !ok {
		return false
	}
	switch t {
	case "session_meta", "compacted":
		return true
	case "event_msg", "response_item":
		pt := headString(h, jPayloadType)
		return pt != "" && pt != "token_count" && pt != "thread_settings_applied"
	}
	return false
}

// skipped counts a record that was classified from its head. last is its
// last byte that is not white space: a long record that does not end like
// a JSON object is malformed (a long record is not validated further; a
// short one is, by the reader, which passes 0 when it is invalid).
func (s *usageState) skipped(last byte) {
	s.Lines++
	if last != '}' {
		s.Diag["invalid_json"]++
	}
}

// blank counts a line that is not a record (v0.1.20 ignored it).
func (s *usageState) blank() { s.Lines++ }

// consume feeds one complete record line (without its newline). p is the
// immediate parent's ledger of a fork child (nil otherwise).
func (s *usageState) consume(line []byte, p *parentLedger) {
	s.Lines++
	line = bytes.TrimSpace(line)
	if len(line) == 0 || line[0] != '{' {
		return
	}
	var ev struct {
		Type      string          `json:"type"`
		Timestamp string          `json:"timestamp"`
		Payload   json.RawMessage `json:"payload"`
		Usage     rawUsage        `json:"usage"`
	}
	if err := json.Unmarshal(line, &ev); err != nil {
		s.Diag["invalid_json"]++
		return
	}
	if c, ok := parseUsageCtx(ev.Type, ev.Payload); ok {
		if ev.Type == "event_msg" || ev.Type == "thread_settings_applied" {
			s.Models.applySettings(c)
		} else {
			s.Models.applyTurnContext(c)
		}
	}
	if ev.Type == "turn_context" {
		// A fixed Codex restores its global counter only at a turn start.
		s.AtTurnStart = true
	}
	if info, rate, cumulative, ok := parseTokenCount(ev.Type, ev.Payload, line); ok {
		s.LastCountLine = s.Lines
		if rate.Present {
			s.Rate = rate
		}
		if info.Total.HasTokens() || info.Last.HasTokens() {
			delta, advanced := s.Acc.observe(info, s.AtTurnStart)
			own := true
			if advanced && delta.HasTokens() {
				native := info.Total
				if !cumulative {
					native = info.Last
				}
				at := eventTime(ev.Timestamp)
				own = s.observed(native, delta, at, cumulative, p)
			}
			s.Models.observeUsage(delta, advanced, own)
			if advanced {
				s.AtTurnStart = false
			}
			s.TokenLine = s.Lines
		}
		return
	}
	if u := ev.Usage.norm(); u.HasTokens() {
		s.AtTurnStart = false
		s.observed(u, u, eventTime(ev.Timestamp), false, p)
		s.Models.observeFallback(u)
		s.FallbackLine, s.FallbackUsage = s.Lines, u
	}
}

// observed records an advancing observation; it reports whether it is the
// rollout's own usage.
func (s *usageState) observed(native, delta TokenUsage, at int64, cumulative bool, p *parentLedger) bool {
	if cumulative {
		s.TL = append(s.TL, TimelineObs{Native: native, Time: at})
	} else {
		s.NonCum = true
	}
	own := true
	if s.Attr != nil {
		s.ChildUsage = true
		own = s.Attr.own(native, at, cumulative, p)
	}
	if own {
		s.OwnEvents++
		s.OwnTotal, _ = AddUsage(s.OwnTotal, delta)
	}
	return own
}

func eventTime(ts string) int64 {
	t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(ts))
	if err != nil {
		return 0
	}
	return t.UnixNano()
}

// stats finishes the state into a report (v0.1.20 finishCodexTokenStats).
// It consumes s: call it on a clone. For a fork child it returns the
// child's own usage and "" when the inherited prefix was separated, or the
// whole rollout's usage and the reason it could not be.
func (s *usageState) stats() (TokenStats, string) {
	s.Models.finishTurn()
	groups := s.Models.Groups
	reason := ""
	child := s.Attr != nil && s.ChildUsage
	if child {
		if s.Attr.Mode == attrFailed {
			reason = s.Attr.Reason
		} else {
			groups = s.Models.Own
		}
	}
	tiers, models := tierView(groups), modelView(groups)
	diag := s.Diag
	if len(diag) == 0 {
		diag = nil
	}
	var out TokenStats
	switch {
	case s.Acc.Seen:
		a := s.Acc
		out = TokenStats{SourceLine: s.TokenLine, Source: "token_count",
			Info:              UsageInfo{Total: a.Total, Last: a.Last, ContextWindow: a.ContextWindow},
			NativeLatestTotal: a.NativeLatest, UsageEvents: a.Events, NonAdvancing: a.NonAdvancing,
			Resets: a.Resets, Recoveries: a.Recoveries, MissingLast: a.MissingLast, Overflow: a.Overflow}
	case s.FallbackUsage.HasTokens():
		out = TokenStats{SourceLine: s.FallbackLine, Source: "event usage",
			Info: UsageInfo{Total: s.FallbackUsage, Last: s.FallbackUsage}, FallbackOnly: true}
	case s.Rate.Present:
		out = TokenStats{SourceLine: s.LastCountLine, Source: "token_count"}
	}
	out.ModelTiers, out.Models, out.RateLimits, out.Diagnostics = tiers, models, s.Rate, diag
	if child && reason == "" {
		// Only the child's own requests count (0.1.22
		// rebuildCodexTokenStatsFromTimeline); the last call and the native
		// counter stay the rollout's, for the context analysis.
		out.Info.Total = s.OwnTotal
		out.UsageEvents = s.OwnEvents
		out.FallbackOnly = false
	}
	return out, reason
}

type rawUsage struct {
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

func firstNonZero(vs ...int64) int64 {
	for _, v := range vs {
		if v != 0 {
			return v
		}
	}
	return 0
}

func (r rawUsage) norm() TokenUsage {
	return TokenUsage{
		Input:     firstNonZero(r.InputTokens, r.InputTokens2),
		Cached:    firstNonZero(r.CachedInputTokens, r.CachedInputTokens2, r.InputTokensDetails.CachedInputTokens, r.InputTokensDetails.CachedTokens, r.PromptTokensDetails.CachedTokens),
		Output:    firstNonZero(r.OutputTokens, r.OutputTokens2),
		Reasoning: firstNonZero(r.ReasoningOutputTokens, r.ReasoningOutputTokens2),
		Total:     firstNonZero(r.TotalTokens, r.TotalTokens2),
	}
}

func firstUsage(vs ...rawUsage) TokenUsage {
	for _, v := range vs {
		if u := v.norm(); u.HasTokens() {
			return u
		}
	}
	return TokenUsage{}
}

// parseTokenCount decodes a token_count record. cumulative reports that
// it carried a cumulative total (0.1.22 nativeTotalPresent).
func parseTokenCount(typ string, payload json.RawMessage, raw []byte) (UsageInfo, RateLimits, bool, bool) {
	src := []byte(payload)
	switch typ {
	case "token_count":
		src = raw
	case "event_msg", "response_item":
	default:
		return UsageInfo{}, RateLimits{}, false, false
	}
	if len(bytes.TrimSpace(src)) == 0 || !bytes.Contains(src, jTokenCount) {
		return UsageInfo{}, RateLimits{}, false, false
	}
	var p struct {
		Type string `json:"type"`
		Info struct {
			Total               rawUsage `json:"total_token_usage"`
			Total2              rawUsage `json:"totalTokenUsage"`
			Last                rawUsage `json:"last_token_usage"`
			Last2               rawUsage `json:"lastTokenUsage"`
			ModelContextWindow  int64    `json:"model_context_window"`
			ModelContextWindow2 int64    `json:"modelContextWindow"`
		} `json:"info"`
		RateLimits  json.RawMessage `json:"rate_limits"`
		RateLimits2 json.RawMessage `json:"rateLimits"`
		Usage       rawUsage        `json:"usage"`
	}
	if json.Unmarshal(src, &p) != nil || p.Type != "token_count" {
		return UsageInfo{}, RateLimits{}, false, false
	}
	info := UsageInfo{
		Total:         firstUsage(p.Info.Total, p.Info.Total2),
		Last:          firstUsage(p.Info.Last, p.Info.Last2),
		ContextWindow: firstNonZero(p.Info.ModelContextWindow, p.Info.ModelContextWindow2),
	}
	cumulative := info.Total.HasTokens()
	if !info.Total.HasTokens() && !info.Last.HasTokens() {
		u := p.Usage.norm()
		info.Total, info.Last = u, u
	}
	rl := p.RateLimits
	if len(bytes.TrimSpace(rl)) == 0 {
		rl = p.RateLimits2
	}
	return info, parseRateLimits(rl), cumulative, true
}

type ctxSettings struct {
	Model            string `json:"model"`
	ModelSlug        string `json:"model_slug"`
	ModelSlug2       string `json:"modelSlug"`
	ServiceTier      string `json:"service_tier"`
	ServiceTier2     string `json:"serviceTier"`
	Effort           string `json:"effort"`
	ReasoningEffort  string `json:"reasoning_effort"`
	ReasoningEffort2 string `json:"reasoningEffort"`
}

func firstString(vs ...string) string {
	for _, v := range vs {
		if v = strings.TrimSpace(v); v != "" {
			return v
		}
	}
	return ""
}

// parseUsageCtx reads the model, tier and effort of a turn_context or a
// thread_settings_applied record.
func parseUsageCtx(typ string, payload json.RawMessage) (usageCtx, bool) {
	if typ != "turn_context" && typ != "thread_settings_applied" && typ != "event_msg" {
		return usageCtx{}, false
	}
	if len(bytes.TrimSpace(payload)) == 0 || (typ == "event_msg" && !bytes.Contains(payload, jSettings)) {
		return usageCtx{}, false
	}
	var p struct {
		ctxSettings
		Type              string      `json:"type"`
		TurnID            string      `json:"turn_id"`
		TurnID2           string      `json:"turnId"`
		ThreadSettings    ctxSettings `json:"thread_settings"`
		ThreadSettings2   ctxSettings `json:"threadSettings"`
		CollaborationMode struct {
			Settings ctxSettings `json:"settings"`
		} `json:"collaboration_mode"`
	}
	if json.Unmarshal(payload, &p) != nil {
		return usageCtx{}, false
	}
	if typ == "event_msg" && p.Type != "thread_settings_applied" {
		return usageCtx{}, false
	}
	if typ == "thread_settings_applied" && p.Type != "" && p.Type != "thread_settings_applied" {
		return usageCtx{}, false
	}
	a, b, c, d := p.ctxSettings, p.ThreadSettings, p.ThreadSettings2, p.CollaborationMode.Settings
	ctx := usageCtx{
		Model:  firstString(a.Model, a.ModelSlug, a.ModelSlug2, b.Model, b.ModelSlug, b.ModelSlug2, c.Model, c.ModelSlug, c.ModelSlug2, d.Model, d.ModelSlug, d.ModelSlug2),
		Tier:   firstString(a.ServiceTier, a.ServiceTier2, b.ServiceTier, b.ServiceTier2, c.ServiceTier, c.ServiceTier2, d.ServiceTier, d.ServiceTier2),
		Effort: firstString(a.Effort, a.ReasoningEffort, a.ReasoningEffort2, b.Effort, b.ReasoningEffort, b.ReasoningEffort2, c.Effort, c.ReasoningEffort, c.ReasoningEffort2, d.Effort, d.ReasoningEffort, d.ReasoningEffort2),
		TurnID: firstString(p.TurnID, p.TurnID2),
	}
	if ctx == (usageCtx{}) {
		return usageCtx{}, false
	}
	return ctx, true
}

func parseRateLimits(raw json.RawMessage) RateLimits {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return RateLimits{}
	}
	out := RateLimits{Present: true}
	var f map[string]json.RawMessage
	if json.Unmarshal(raw, &f) != nil {
		return out
	}
	out.LimitID = jsonString(f, "limit_id", "limitId")
	out.LimitName = jsonString(f, "limit_name", "limitName")
	out.PlanType = jsonString(f, "plan_type", "planType")
	out.ReachedType = jsonString(f, "rate_limit_reached_type", "rateLimitReachedType")
	out.Credits = parseCredits(f["credits"])
	var names []string
	for name, v := range f {
		if scalarRateField(name) {
			continue
		}
		if v = bytes.TrimSpace(v); len(v) == 0 || v[0] != '{' {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if w, ok := parseRateWindow(name, f[name]); ok {
			out.Windows = append(out.Windows, w)
		}
	}
	return out
}

func parseRateWindow(name string, raw json.RawMessage) (RateWindow, bool) {
	var f map[string]json.RawMessage
	if json.Unmarshal(raw, &f) != nil {
		return RateWindow{}, false
	}
	w := RateWindow{Name: name}
	if v, ok := jsonFloat(f, "used_percent", "usedPercent", "usage_percent", "usagePercent", "percent_used", "percentUsed"); ok {
		w.UsedPercent, w.HasUsedPercent = v, true
	}
	w.ResetAt = jsonStringLike(f, "reset_at", "resetAt", "resets_at", "resetsAt")
	if v, ok := jsonInt(f, "remaining", "remaining_tokens", "remainingTokens"); ok {
		w.Remaining, w.HasRemaining = v, true
	}
	if v, ok := jsonInt(f, "window_minutes", "windowMinutes", "window_duration_mins", "windowDurationMins"); ok {
		w.WindowMinutes, w.HasWindowMinutes = v, true
	}
	if v, ok := jsonInt(f, "window_seconds", "windowSeconds", "seconds"); ok {
		w.WindowSeconds, w.HasWindowSeconds = v, true
	}
	return w, w.HasUsedPercent || w.ResetAt != "" || w.HasRemaining || w.HasWindowMinutes || w.HasWindowSeconds
}

func parseCredits(raw json.RawMessage) Credits {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) || raw[0] != '{' {
		return Credits{}
	}
	var f map[string]json.RawMessage
	if json.Unmarshal(raw, &f) != nil {
		return Credits{Present: true}
	}
	c := Credits{Present: true}
	if v, ok := jsonBool(f, "has_credits", "hasCredits"); ok {
		c.HasCredits = v
	}
	if v, ok := jsonBool(f, "unlimited"); ok {
		c.Unlimited = v
	}
	c.Balance = jsonStringLike(f, "balance")
	return c
}

func scalarRateField(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "limit_id", "limitid", "limit_name", "limitname", "plan_type", "plantype", "rate_limit_reached_type", "ratelimitreachedtype", "credits":
		return true
	}
	return false
}

func jsonString(f map[string]json.RawMessage, keys ...string) string {
	for _, k := range keys {
		var v string
		if raw, ok := f[k]; ok && json.Unmarshal(raw, &v) == nil && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func jsonStringLike(f map[string]json.RawMessage, keys ...string) string {
	for _, k := range keys {
		raw, ok := f[k]
		if !ok {
			continue
		}
		var s string
		if json.Unmarshal(raw, &s) == nil && strings.TrimSpace(s) != "" {
			return strings.TrimSpace(s)
		}
		var n json.Number
		d := json.NewDecoder(bytes.NewReader(raw))
		d.UseNumber()
		if d.Decode(&n) == nil {
			return n.String()
		}
		var b bool
		if json.Unmarshal(raw, &b) == nil {
			return strconv.FormatBool(b)
		}
	}
	return ""
}

func jsonBool(f map[string]json.RawMessage, keys ...string) (bool, bool) {
	for _, k := range keys {
		var v bool
		if raw, ok := f[k]; ok && json.Unmarshal(raw, &v) == nil {
			return v, true
		}
	}
	return false, false
}

func jsonInt(f map[string]json.RawMessage, keys ...string) (int64, bool) {
	for _, k := range keys {
		raw, ok := f[k]
		if !ok {
			continue
		}
		var v int64
		if json.Unmarshal(raw, &v) == nil {
			return v, true
		}
		var fl float64
		if json.Unmarshal(raw, &fl) == nil {
			return int64(fl), true
		}
	}
	return 0, false
}

func jsonFloat(f map[string]json.RawMessage, keys ...string) (float64, bool) {
	for _, k := range keys {
		var v float64
		if raw, ok := f[k]; ok && json.Unmarshal(raw, &v) == nil {
			return v, true
		}
	}
	return 0, false
}
