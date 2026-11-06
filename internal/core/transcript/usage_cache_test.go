package transcript

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Rollout lines in the order current Codex writes them (timestamp first).

var t0 = time.Date(2026, 11, 6, 9, 0, 0, 0, time.UTC)

func uj(u TokenUsage) string {
	return fmt.Sprintf(`{"input_tokens":%d,"cached_input_tokens":%d,"output_tokens":%d,"reasoning_output_tokens":%d,"total_tokens":%d}`,
		u.Input, u.Cached, u.Output, u.Reasoning, u.Total)
}

func ts(i int) string {
	return t0.Add(time.Duration(i) * time.Second).Format("2006-01-02T15:04:05.000Z")
}

func lineMeta(i int, id, source, forked string) string {
	f := ""
	if forked != "" {
		f = `,"forked_from_id":"` + forked + `"`
	}
	return fmt.Sprintf(`{"timestamp":"%s","type":"session_meta","payload":{"id":"%s","timestamp":"%s","cwd":"/w","originator":"codex_cli_rs","cli_version":"0.58.0","source":%s%s}}`, ts(i), id, ts(i), source, f)
}

func lineCtx(i int, turn, model, effort, tier string) string {
	extra := ""
	if model != "" {
		extra += `,"model":"` + model + `"`
	}
	if effort != "" {
		extra += `,"effort":"` + effort + `"`
	}
	if tier != "" {
		extra += `,"service_tier":"` + tier + `"`
	}
	return fmt.Sprintf(`{"timestamp":"%s","type":"turn_context","payload":{"turn_id":"%s","cwd":"/w"%s}}`, ts(i), turn, extra)
}

func lineCount(i int, total, last TokenUsage) string {
	return fmt.Sprintf(`{"timestamp":"%s","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":%s,"last_token_usage":%s,"model_context_window":258400},"rate_limits":null}}`, ts(i), uj(total), uj(last))
}

func lineTool(i, n int) string {
	return fmt.Sprintf(`{"timestamp":"%s","type":"response_item","payload":{"type":"function_call_output","call_id":"call_1","output":"%s"}}`, ts(i), strings.Repeat("x", n))
}

func lineMsg(i int, text string) string {
	return fmt.Sprintf(`{"timestamp":"%s","type":"event_msg","payload":{"type":"agent_message","message":"%s"}}`, ts(i), text)
}

const (
	idParent = "019e0000-0000-7000-8000-000000000001"
	idChild  = "019e0000-0000-7000-8000-000000000002"
	idIndep  = "019e0000-0000-7000-8000-000000000003"
	idBad    = "019e0000-0000-7000-8000-000000000004"
	idOther  = "019e0000-0000-7000-8000-000000000005"
)

func rolloutPath(dir, id string) string {
	return filepath.Join(dir, "2026", "11", "06", "rollout-2026-11-06T09-00-00-"+id+".jsonl")
}

func writeUsageLines(t *testing.T, path string, lines []string, appendTo bool) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	flag := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	if appendTo {
		flag = os.O_CREATE | os.O_WRONLY | os.O_APPEND
	}
	f, err := os.OpenFile(path, flag, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(strings.Join(lines, "\n") + "\n"); err != nil {
		t.Fatal(err)
	}
}

func add(a, b TokenUsage) TokenUsage { s, _ := AddUsage(a, b); return s }

// resetHistory is a parent rollout with 35 native counter reset epochs
// (each turn restarts Codex's cumulative counter, the bug v0.1.20
// reconstructed), tool outputs and messages between the counts. It
// returns the lines, the hand-computed conversation total and the lines
// up to the fork point (after epoch forkAt).
func resetHistory(forkAt int) (lines []string, total TokenUsage, prefix []string, prefixTotal TokenUsage) {
	lines = append(lines, lineMeta(0, idParent, `"cli"`, ""))
	i := 1
	for epoch := 0; epoch < 35; epoch++ {
		model, effort, tier := "gpt-5.5", "high", "priority"
		if epoch%5 == 4 {
			model, effort, tier = "gpt-5.5-mini", "", "" // unknown effort and tier
		}
		lines = append(lines, lineCtx(i, fmt.Sprintf("turn-%d", epoch), model, effort, tier))
		i++
		var native TokenUsage
		for call := 0; call < 3+epoch%4; call++ {
			last := TokenUsage{Input: int64(1000 + 10*epoch + call), Cached: int64(600 + epoch), Output: int64(50 + call), Reasoning: int64(5 + call%3)}
			last.Total = last.Input + last.Output
			native = add(native, last)
			total = add(total, last)
			lines = append(lines, lineTool(i, 6000+epoch*100), lineMsg(i+1, "working"), lineCount(i+2, native, last))
			i += 3
			if call == 1 {
				lines = append(lines, lineCount(i, native, last)) // a non-advancing repeat
				i++
			}
		}
		if epoch == forkAt {
			prefix = append([]string(nil), lines...)
			prefixTotal = total
		}
	}
	return lines, total, prefix, prefixTotal
}

func usageOnce(t *testing.T, c *UsageCache, threads ...string) ChatUsage {
	t.Helper()
	out, err := c.Usage(context.Background(), [][]string{threads})
	if err != nil {
		t.Fatal(err)
	}
	return out[0]
}

// R-42: 35 reset epochs and a fork child; totals equal hand-computed
// values, the child's inherited prefix is not counted twice.
func TestUsageResetEpochsAndForkChild(t *testing.T) {
	dir := t.TempDir()
	lines, total, prefix, _ := resetHistory(20)
	writeUsageLines(t, rolloutPath(dir, idParent), lines, false)

	// The subagent copies the parent's history up to the fork (with its
	// counts), then makes three requests of its own; its counter goes on
	// from the inherited value.
	child := []string{lineMeta(500, idChild, `{"subagent":{"thread_spawn":{"parent_thread_id":"`+idParent+`","depth":1}}}`, idParent)}
	child = append(child, prefix[1:]...)
	var native TokenUsage
	for _, l := range prefix {
		if strings.Contains(l, `"token_count"`) {
			native = TokenStatsOfLine(t, l)
		}
	}
	var own TokenUsage
	child = append(child, lineCtx(600, "child-turn", "gpt-5.5", "low", "priority"))
	for k := 0; k < 3; k++ {
		last := TokenUsage{Input: int64(700 + k), Cached: 100, Output: 20, Reasoning: 3}
		last.Total = last.Input + last.Output
		native = add(native, last)
		own = add(own, last)
		child = append(child, lineCount(601+k, native, last))
	}
	writeUsageLines(t, rolloutPath(dir, idChild), child, false)

	c := NewUsageCache(dir)
	u := usageOnce(t, c, idParent)
	if len(u.Main) != 1 || len(u.Subagents) != 1 {
		t.Fatalf("usage = %+v", u)
	}
	m := u.Main[0].Stats
	if m.Info.Total != total {
		t.Fatalf("parent total = %+v, want %+v", m.Info.Total, total)
	}
	if m.Resets != 34 {
		t.Fatalf("resets = %d, want 34", m.Resets)
	}
	s := u.Subagents[0]
	if s.Problem != "" || s.Err != nil {
		t.Fatalf("child problem %q err %v", s.Problem, s.Err)
	}
	if s.Stats.Info.Total != own || s.Stats.UsageEvents != 3 {
		t.Fatalf("child total = %+v (events %d), want %+v", s.Stats.Info.Total, s.Stats.UsageEvents, own)
	}
	want := []ModelUsage{{Model: "gpt-5.5", Overall: own, Efforts: []EffortUsage{{Effort: "low", Usage: own}}}}
	if !reflect.DeepEqual(s.Stats.Models, want) {
		t.Fatalf("child models = %+v", s.Stats.Models)
	}
	// The unknown bucket: every fifth epoch had no effort or tier.
	var unknownTier TokenUsage
	for _, mt := range m.ModelTiers {
		if mt.Tier == "unknown" {
			if mt.Model != "gpt-5.5-mini" {
				t.Fatalf("unknown tier on %s", mt.Model)
			}
			unknownTier = mt.Usage
		}
	}
	var unknownEffort TokenUsage
	for _, mu := range m.Models {
		for _, e := range mu.Efforts {
			if e.Effort == "unknown" {
				unknownEffort = e.Usage
			}
		}
	}
	if !unknownTier.HasTokens() || unknownTier != unknownEffort {
		t.Fatalf("unknown buckets: tier %+v effort %+v", unknownTier, unknownEffort)
	}
	if sum := add(SumTiers(m.ModelTiers)[0].Usage, SumTiers(m.ModelTiers)[1].Usage); sum != total {
		t.Fatalf("tier sum %+v != total %+v", sum, total)
	}
}

// TokenStatsOfLine returns the cumulative total of one token_count line.
func TokenStatsOfLine(t *testing.T, l string) TokenUsage {
	t.Helper()
	info, _, _, ok := parseTokenCount("event_msg", []byte(l[strings.Index(l, `"payload":`)+len(`"payload":`):len(l)-1]), nil)
	if !ok {
		t.Fatalf("not a token count: %s", l)
	}
	return info.Total
}

// A child that inherited nothing starts its own counter below the
// parent's: all of it is its own. A child whose counter neither repeats
// the parent's nor starts below it cannot be attributed (fail closed).
func TestUsageIndependentAndUnprovableChildren(t *testing.T) {
	dir := t.TempDir()
	lines, _, _, _ := resetHistory(-1)
	writeUsageLines(t, rolloutPath(dir, idParent), lines, false)
	spawn := `{"subagent":{"thread_spawn":{"parent_thread_id":"` + idParent + `","depth":1}}}`
	small := TokenUsage{Input: 10, Output: 2, Total: 12}
	writeUsageLines(t, rolloutPath(dir, idIndep), []string{lineMeta(900, idIndep, spawn, ""), lineCount(901, small, small)}, false)
	huge := TokenUsage{Input: 10_000_000, Output: 2, Total: 10_000_002}
	writeUsageLines(t, rolloutPath(dir, idBad), []string{lineMeta(900, idBad, spawn, ""), lineCount(901, huge, huge)}, false)
	u := usageOnce(t, NewUsageCache(dir), idParent)
	if len(u.Subagents) != 2 {
		t.Fatalf("subagents = %+v", u.Subagents)
	}
	for _, s := range u.Subagents {
		switch s.Thread {
		case idIndep:
			if s.Problem != "" || s.Stats.Info.Total != small {
				t.Fatalf("independent child: %+v", s)
			}
		case idBad:
			if s.Problem != errAttrNoPrefix {
				t.Fatalf("unprovable child problem = %q", s.Problem)
			}
		}
	}
}

// Reading in pieces (a record split across two calls) gives the one-shot
// result, and the second call reads only the appended bytes.
func TestUsageIncrementalEqualsOneShot(t *testing.T) {
	dir := t.TempDir()
	lines, total, _, _ := resetHistory(-1)
	path := rolloutPath(dir, idParent)
	half := len(lines) / 2
	writeUsageLines(t, path, lines[:half], false)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	cut := len(lines[half]) / 2
	if _, err := f.WriteString(lines[half][:cut]); err != nil { // an incomplete record
		t.Fatal(err)
	}
	f.Close()
	c := NewUsageCache(dir)
	usageOnce(t, c, idParent)
	first := c.c.Files[path].Off
	if first <= 0 || first >= int64(len(strings.Join(lines[:half+1], "\n"))) {
		t.Fatalf("first offset %d: the incomplete record must not be consumed", first)
	}
	f, _ = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if _, err := f.WriteString(lines[half][cut:] + "\n" + strings.Join(lines[half+1:], "\n") + "\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()
	got := usageOnce(t, c, idParent).Main[0].Stats
	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := ParseTokenStats(strings.NewReader(string(full)))
	want.SourcePath = path
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("incremental:\n%+v\none-shot:\n%+v", got, want)
	}
	if got.Info.Total != total {
		t.Fatalf("total %+v, want %+v", got.Info.Total, total)
	}
	if e := c.c.Files[path]; e.Off != int64(len(full)) {
		t.Fatalf("offset %d, want %d", e.Off, len(full))
	}
	// Nothing changed: the result is the same.
	if again := usageOnce(t, c, idParent).Main[0].Stats; !reflect.DeepEqual(again, want) {
		t.Fatalf("second call differs:\n%+v", again)
	}
}

// A rollout rewritten in place (another inode or shorter) is read again.
func TestUsageRewrittenRolloutIsReadAgain(t *testing.T) {
	dir := t.TempDir()
	path := rolloutPath(dir, idOther)
	a := TokenUsage{Input: 100, Output: 10, Total: 110}
	writeUsageLines(t, path, []string{lineMeta(0, idOther, `"cli"`, ""), lineCount(1, a, a), lineCount(2, add(a, a), a)}, false)
	c := NewUsageCache(dir)
	if got := usageOnce(t, c, idOther).Main[0].Stats.Info.Total; got != add(a, a) {
		t.Fatalf("total %+v", got)
	}
	os.Remove(path)
	writeUsageLines(t, path, []string{lineMeta(0, idOther, `"cli"`, ""), lineCount(1, a, a)}, false)
	if got := usageOnce(t, c, idOther).Main[0].Stats.Info.Total; got != a {
		t.Fatalf("total after rewrite %+v, want %+v", got, a)
	}
}

// A skipped record is still checked: a short malformed one is reported as
// v0.1.20 reported it, a long one when it does not end like an object.
func TestUsageSkippedRecordDiagnostics(t *testing.T) {
	a := TokenUsage{Input: 1, Output: 1, Total: 2}
	in := strings.Join([]string{
		lineCount(0, a, a),
		`{"timestamp":"x","type":"event_msg","payload":{"type":"agent_message","message":"done"}`,
		lineTool(1, 10000),
		lineTool(2, 10000)[:9000],
		"",
		"not json",
	}, "\n")
	got, err := ParseTokenStats(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if got.Diagnostics["invalid_json"] != 2 || got.SourceLine != 1 {
		t.Fatalf("diagnostics %+v line %d", got.Diagnostics, got.SourceLine)
	}
}

// Thread ids are taken from file names; days are listed again only when
// they change; a deleted rollout leaves the cache.
func TestUsageIndex(t *testing.T) {
	dir := t.TempDir()
	a := TokenUsage{Input: 1, Output: 1, Total: 2}
	writeUsageLines(t, rolloutPath(dir, idOther), []string{lineMeta(0, idOther, `"cli"`, ""), lineCount(1, a, a)}, false)
	c := NewUsageCache(dir)
	u := usageOnce(t, c, idOther, "019e0000-0000-7000-8000-00000000ffff")
	if len(u.Main) != 1 || len(u.Missing) != 1 {
		t.Fatalf("usage %+v", u)
	}
	os.Remove(rolloutPath(dir, idOther))
	u = usageOnce(t, c, idOther)
	if len(u.Main) != 0 || len(c.c.Files) != 0 {
		t.Fatalf("deleted rollout still cached: %+v %d", u, len(c.c.Files))
	}
}
