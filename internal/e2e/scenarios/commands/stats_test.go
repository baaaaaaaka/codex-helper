package commands

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// statsUsage is one token usage snapshot of a rollout token_count event.
type statsUsage struct{ in, cached, out, reasoning, total int64 }

func (u statsUsage) plus(v statsUsage) statsUsage {
	return statsUsage{u.in + v.in, u.cached + v.cached, u.out + v.out, u.reasoning + v.reasoning, u.total + v.total}
}

func (u statsUsage) json() string {
	return fmt.Sprintf(`{"input_tokens":%d,"cached_input_tokens":%d,"output_tokens":%d,"reasoning_output_tokens":%d,"total_tokens":%d}`,
		u.in, u.cached, u.out, u.reasoning, u.total)
}

// thousands formats n as the helper does (1,234,567).
func thousands(n int64) string {
	s := strconv.FormatInt(n, 10)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

// statsRollout appends Codex-shaped records to a rollout file.
type statsRollout struct {
	lines []string
	clock time.Time
}

func (r *statsRollout) ts() string {
	r.clock = r.clock.Add(time.Second)
	return r.clock.Format("2006-01-02T15:04:05.000Z")
}

func (r *statsRollout) turnContext(turn, model, effort, tier string) {
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
	r.lines = append(r.lines, fmt.Sprintf(`{"timestamp":"%s","type":"turn_context","payload":{"turn_id":"%s","cwd":"/w"%s}}`, r.ts(), turn, extra))
}

func (r *statsRollout) tokenCount(native, last statsUsage) {
	r.lines = append(r.lines, fmt.Sprintf(`{"timestamp":"%s","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":%s,"last_token_usage":%s,"model_context_window":258400},"rate_limits":null}}`,
		r.ts(), native.json(), last.json()))
}

func (r *statsRollout) toolOutput(n int) {
	r.lines = append(r.lines, fmt.Sprintf(`{"timestamp":"%s","type":"response_item","payload":{"type":"function_call_output","call_id":"call_1","output":"%s"}}`, r.ts(), strings.Repeat("x", n)))
}

func appendLines(path string, lines []string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(strings.Join(lines, "\n") + "\n"); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// R-42: a Work chat's rollout with 35 native counter reset epochs (Codex
// restarted its cumulative counter at each turn) and a subagent forked
// from it, which copies the parent's whole history before its own
// requests. `helper stats` gives the hand-computed totals: every epoch
// counted once, the subagent's inherited prefix not counted again, and
// the requests without model metadata in an `unknown` bucket.
func TestStatsResetEpochsAndForkChild(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "STATS-RESETS", harness.Options{
		Guarantees: []string{"F-STA-1", "R-42"},
		Title:      "`helper stats` totals across 35 counter resets and a subagent fork equal the hand-computed values",
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "resets")
	call, ok := ask(s, work.ID, "first")
	s.Check("the first request is answered", ok && call.ThreadID != "", "answered on a Codex thread", fmt.Sprintf("ok=%v thread=%q", ok, call.ThreadID))
	if call.ThreadID == "" {
		return
	}
	matches, _ := filepath.Glob(filepath.Join(s.Env.SessionsDir, "*", "*", "*", "rollout-*-"+call.ThreadID+".jsonl"))
	s.Check("the Work chat's thread has one rollout", len(matches) == 1, "1 rollout", fmt.Sprint(len(matches)))
	if len(matches) != 1 {
		return
	}
	parent := matches[0]

	// The fake Codex's turn: 1,280 tokens (1,200 input, 80 output, 20
	// reasoning) under gpt-5-codex / medium.
	main := statsUsage{1200, 0, 80, 20, 1280}
	// 35 epochs; each restarts the native counter. Every fifth epoch runs
	// a model without effort or service tier metadata.
	r := &statsRollout{clock: time.Now().UTC()}
	var native statsUsage
	for e := 0; e < 35; e++ {
		model, effort, tier := "gpt-5.5", "high", "priority"
		if e%5 == 4 {
			model, effort, tier = "gpt-5.5-mini", "", ""
		}
		r.turnContext(fmt.Sprintf("epoch-%d", e), model, effort, tier)
		native = statsUsage{}
		for k := 0; k < 2; k++ {
			last := statsUsage{int64(1000 + e), 400, int64(50 + k), 10, int64(1050 + e + k)}
			native = native.plus(last)
			main = main.plus(last)
			r.toolOutput(5000)
			r.tokenCount(native, last)
			if k == 0 {
				r.tokenCount(native, last) // a non-advancing repeat
			}
		}
	}
	if err := appendLines(parent, r.lines); err != nil {
		t.Fatal(err)
	}

	// The subagent: its rollout copies the parent's history (with its
	// counts), then makes three requests; its counter goes on from the
	// inherited value.
	history, err := os.ReadFile(parent)
	if err != nil {
		t.Fatal(err)
	}
	inherited := strings.SplitN(strings.TrimRight(string(history), "\n"), "\n", 2)[1]
	child := "019e2e00-0000-7000-8000-00000000c41d"
	c := &statsRollout{clock: r.clock}
	meta := fmt.Sprintf(`{"timestamp":"%s","type":"session_meta","payload":{"id":"%s","timestamp":"%s","cwd":"/w","originator":"codex_cli_rs","cli_version":"0.58.0","source":{"subagent":{"thread_spawn":{"parent_thread_id":"%s","depth":1,"agent_nickname":"Scout"}}},"forked_from_id":"%s"}}`,
		c.ts(), child, c.ts(), call.ThreadID, call.ThreadID)
	c.lines = append(c.lines, meta, inherited)
	c.turnContext("child-turn", "gpt-5.5", "low", "priority")
	var own statsUsage
	for k := 0; k < 3; k++ {
		last := statsUsage{int64(700 + k), 100, 20, 3, int64(720 + k)}
		native = native.plus(last)
		own = own.plus(last)
		c.tokenCount(native, last)
	}
	childPath := filepath.Join(filepath.Dir(parent), "rollout-"+time.Now().UTC().Format("2006-01-02T15-04-05")+"-"+child+".jsonl")
	if err := appendLines(childPath, c.lines); err != nil {
		t.Fatal(err)
	}

	st, ok := command(s, work.ID, "helper stats", "STATS: Codex tokens")
	s.Check("`helper stats` answers", ok, "a STATS reply", clip(st, 200))
	s.Check("the main conversation total counts every reset epoch once", strings.Contains(st, "total: "+thousands(main.total)) || strings.Contains(st, thousands(main.total)),
		thousands(main.total)+" (hand-computed)", clip(st, 600))
	s.Check("the counter resets are reported", strings.Contains(st, "35 native cumulative counter reset(s)"), "35 resets", clip(st, 600))
	s.Check("the subagent counts only its own requests", strings.Contains(st, "🧩 SUBAGENTS (1)") && strings.Contains(st, thousands(own.total)),
		"SUBAGENTS (1) with "+thousands(own.total), clip(st, 900))
	s.Check("the total is main plus subagents", strings.Contains(st, "🧮 TOTAL (MAIN + SUBAGENTS)") && strings.Contains(st, thousands(main.total+own.total)),
		"TOTAL "+thousands(main.total+own.total), clip(st, 900))
	// The subagent's raw rollout total (inherited history + own) must not
	// appear anywhere: that would be the inherited prefix counted twice.
	s.Check("the inherited prefix is not counted twice", !strings.Contains(st, thousands(2*main.total+own.total)),
		"no double-counted total", clip(st, 900))
	s.Check("requests without model metadata stay unknown", strings.Contains(st, "Tier: unknown") && strings.Contains(st, "Effort: unknown"),
		"Tier: unknown, Effort: unknown", clip(st, 900))

	all, ok := command(s, control.ID, "stats", "Scope: all Work chats")
	s.Check("control `stats` gives the same Work chat total", ok && strings.Contains(all, "s001: total "+thousands(main.total+own.total)),
		"s001: total "+thousands(main.total+own.total), clip(all, 400))
}
