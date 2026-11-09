package engine

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
)

// goalAgent is fakeAgent with Codex thread goals (GoalAgent): one goal per
// thread, the model settings every thread reports, and a log of the goal
// calls in order with the interrupts.
type goalAgent struct {
	*fakeAgent
	gmu      sync.Mutex
	goals    map[string]GoalState
	settings TurnInfo // Model, Effort, Tier
	selfTurn bool     // Codex runs a turn by itself (StopGoal interrupts it)
	calls    []string
}

func newGoalAgent() *goalAgent {
	g := &goalAgent{fakeAgent: newAgent(), goals: map[string]GoalState{}, settings: TurnInfo{Model: "gpt-test", Effort: "high", Tier: "priority"}}
	g.interrupted = func() { g.log("interrupt") }
	return g
}

var _ GoalAgent = (*goalAgent)(nil)

func (g *goalAgent) log(s string) {
	g.gmu.Lock()
	g.calls = append(g.calls, s)
	g.gmu.Unlock()
}

func (g *goalAgent) callLog() []string {
	g.gmu.Lock()
	defer g.gmu.Unlock()
	return append([]string(nil), g.calls...)
}

func (g *goalAgent) Info(_ int64, threadID string) TurnInfo {
	g.gmu.Lock()
	defer g.gmu.Unlock()
	out := g.settings
	if s := g.goals[threadID]; s.Status == GoalActive {
		out.Goal = s.Objective
	}
	out.SelfTurn = g.selfTurn
	return out
}

func (g *goalAgent) Goal(_ context.Context, _ int64, threadID, _ string, change GoalState) (GoalState, error) {
	g.gmu.Lock()
	defer g.gmu.Unlock()
	cur := g.goals[threadID]
	if change == (GoalState{}) {
		g.calls = append(g.calls, "get")
		return cur, nil
	}
	if change.Objective != "" {
		cur.Objective = change.Objective
	}
	if change.Status != "" {
		cur.Status = change.Status
	}
	g.goals[threadID] = cur
	g.calls = append(g.calls, "set "+cur.Status+" "+cur.Objective)
	return cur, nil
}

func (g *goalAgent) StopGoal(_ context.Context, _ int64, threadID string) (GoalStopped, error) {
	g.gmu.Lock()
	defer g.gmu.Unlock()
	var out GoalStopped
	if cur := g.goals[threadID]; cur.Status == GoalActive {
		cur.Status = GoalPaused
		g.goals[threadID] = cur
		out.Paused, out.Objective = true, cur.Objective
	}
	if g.selfTurn {
		g.selfTurn, out.Interrupted = false, true
	}
	g.calls = append(g.calls, "stop")
	return out, nil
}

func goalRig(t *testing.T) (*rig, *goalAgent) {
	ga := newGoalAgent()
	r := newRig(t, func(c *Config) { c.Agent = ga })
	r.ag = ga.fakeAgent
	return r, ga
}

// lastOwn is the helper's newest message in chat containing sub.
func lastOwn(r *rig, chat, sub string) string {
	m := r.own(chat, sub)
	if len(m) == 0 {
		return ""
	}
	return m[len(m)-1].Text
}

// Owner request 2026-11-07: with an active goal every new Codex message
// (status and answer) closes with "🎯 Active goal: …", and every answer
// with "🔧 Helper: model - …, effort - …, tier - …", both before the
// "Codex finished responding" footer. `helper goal` shows, sets, pauses
// and resumes the goal.
func TestGoalAndModelLinesAndGoalCommand(t *testing.T) {
	r, ga := goalRig(t)
	work := r.newWork(t.TempDir())

	// No thread yet: a hint.
	r.ch.PostUser(work, "helper goal")
	r.wait("no-thread hint", func() bool { return len(r.own(work, textGoalNoThread)) == 1 })

	// An answer without a goal: the model line only.
	r.ch.PostUser(work, "first question")
	r.wait("first answer", func() bool { return len(r.own(work, "ANSWER to first question")) == 1 })
	ans := lastOwn(r, work, "ANSWER to first question")
	if !strings.Contains(ans, "\n---\n🔧 Helper: model - gpt-test, effort - high, tier - fast\n🔧 Helper: ✅ Codex finished responding.") || strings.Contains(ans, "Active goal") || strings.Count(ans, "\n---\n") != 1 {
		t.Fatalf("answer without a goal: %q", ans)
	}

	r.ch.PostUser(work, "helper goal")
	r.wait("no goal", func() bool { return len(r.own(work, "No goal is set for this chat.")) == 1 })
	r.ch.PostUser(work, "helper goal active")
	r.wait("no goal to activate", func() bool { return len(r.own(work, "No goal is set for this chat.")) == 2 })

	long := "Ship the release with every test green and the changelog written " + strings.Repeat("and more ", 20)
	r.ch.PostUser(work, "helper goal "+long)
	r.wait("goal set", func() bool { return len(r.own(work, "🎯 Goal set.")) == 1 })
	if m := lastOwn(r, work, "🎯 Goal set."); !strings.Contains(m, "**Goal status:** active") || !strings.Contains(m, "Ship the release") {
		t.Fatalf("goal set reply: %q", m)
	}

	// Status and answer with the goal: a divider separates the body from
	// the clipped goal line, before the model line and the footer.
	r.ag.mu.Lock()
	r.ag.notes = []string{"looking around"}
	r.ag.mu.Unlock()
	r.ch.PostUser(work, "progress please")
	r.wait("second answer", func() bool { return len(r.own(work, "ANSWER to progress please")) == 1 })
	st := lastOwn(r, work, "looking around")
	if !strings.Contains(st, "\n---\n🎯 Active goal: Ship the release with every test green") || strings.Contains(st, "model - ") {
		t.Fatalf("status with a goal: %q", st)
	}
	ans = lastOwn(r, work, "ANSWER to progress please")
	gl := ans[strings.Index(ans, "🎯 Active goal: "):]
	gl = gl[:strings.Index(gl, "\n")]
	if len([]rune(gl)) > len([]rune("🎯 Active goal: "))+goalNoteMax+1 || !strings.HasSuffix(gl, "…") {
		t.Fatalf("goal line not clipped: %q", gl)
	}
	if divider, goal, model, finished := strings.Index(ans, "\n---\n"), strings.Index(ans, "🎯 Active goal"), strings.Index(ans, "model - gpt-test"), strings.Index(ans, "Codex finished responding"); !(divider > 0 && divider < goal && goal < model && model < finished) {
		t.Fatalf("order of the closing lines: %q", ans)
	}

	r.ch.PostUser(work, "helper goal pause")
	r.wait("paused", func() bool { return len(r.own(work, "⏸️ Goal paused.")) == 1 })
	r.ch.PostUser(work, "helper goal")
	r.wait("shown paused", func() bool { return len(r.own(work, "**Goal status:** paused")) >= 2 })
	r.ch.PostUser(work, "third question")
	r.wait("third answer", func() bool { return len(r.own(work, "ANSWER to third question")) == 1 })
	if ans := lastOwn(r, work, "ANSWER to third question"); strings.Contains(ans, "Active goal") || !strings.Contains(ans, "model - gpt-test") {
		t.Fatalf("answer with a paused goal: %q", ans)
	}
	r.ch.PostUser(work, "/goal active")
	r.wait("active again", func() bool { return len(r.own(work, "▶️ Goal active again.")) == 1 })
	r.ch.PostUser(work, "helper goal active")
	r.wait("already active", func() bool { return len(r.own(work, "The goal is already active.")) == 1 })

	// `codex goal …` is a prompt; `helper goal` in the control chat is
	// the wrong chat.
	r.ch.PostUser(work, "codex goal is to keep this a prompt")
	r.wait("prompt answered", func() bool { return len(r.own(work, "ANSWER to codex goal is to keep this a prompt")) == 1 })
	ctl := r.control()
	r.ch.PostUser(ctl, "helper goal something")
	r.wait("wrong chat", func() bool {
		for _, m := range r.ch.Messages(ctl) {
			if m.Marker != "" && strings.Contains(m.Text, "⚠️ Wrong chat") {
				return true
			}
		}
		return false
	})
	if n := r.ag.calls("helper goal"); n != 0 {
		t.Fatalf("%d goal commands ran as Codex turns", n)
	}
	if got := strings.Join(ga.callLog(), " | "); strings.Count(got, "set ") != 3 { // set, pause, active (`active` on an active goal sets nothing)
		t.Fatalf("goal calls: %s", got)
	}
}

// Codex's interrupt leaves a goal active, so `helper cancel` pauses it
// first (else the next message would start it again) and the helper's
// turn is interrupted after; with no request of the helper's to cancel,
// the turn Codex runs by itself for the goal is stopped and the goal
// paused. A cancel of a queued request leaves the goal alone.
func TestCancelPausesTheActiveGoal(t *testing.T) {
	r, ga := goalRig(t)
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "bind the thread")
	r.wait("first answer", func() bool { return len(r.own(work, "ANSWER to bind the thread")) == 1 })
	r.ch.PostUser(work, "helper goal keep going")
	r.wait("goal set", func() bool { return len(r.own(work, "🎯 Goal set.")) == 1 })

	// A queued request is cancelled: the goal stays active.
	release := make(chan struct{})
	r.ag.mu.Lock()
	r.ag.block["long running"] = release
	r.ag.mu.Unlock()
	r.ch.PostUser(work, "long running")
	r.wait("running", func() bool { return r.ag.calls("long running") == 1 })
	r.ch.PostUser(work, "queued one")
	r.wait("queued", func() bool { return len(r.own(work, "**Queued.**")) == 1 })
	r.ch.PostUser(work, "helper cancel queued")
	r.wait("queued cancelled", func() bool { return len(r.own(work, "**Canceled prompt:**")) == 1 })
	if m := lastOwn(r, work, "**Canceled prompt:**"); strings.Contains(m, "goal") {
		t.Fatalf("a queued cancel mentions the goal: %q", m)
	}
	for _, c := range ga.callLog() {
		if c == "stop" {
			t.Fatal("a queued cancel stopped the goal")
		}
	}

	// The running turn is cancelled: the goal is paused, then the turn
	// interrupted.
	r.ch.PostUser(work, "helper cancel")
	r.wait("running cancelled", func() bool { return len(r.own(work, textCancelGoal)) == 1 })
	r.wait("interrupted", func() bool {
		log := strings.Join(ga.callLog(), " | ")
		return strings.Contains(log, "stop | interrupt")
	})
	if g := ga.Info(0, r.e.threadOf(convID(t, r, "s001"))); g.Goal != "" {
		t.Fatalf("goal still active after the cancel: %+v", g)
	}

	// Nothing of the helper's runs, but Codex runs a goal turn by itself.
	r.waitIdle(work)
	ga.gmu.Lock()
	ga.goals[r.e.threadOf(convID(t, r, "s001"))] = GoalState{Objective: "keep going", Status: GoalActive}
	ga.selfTurn = true
	ga.gmu.Unlock()
	r.ch.PostUser(work, "helper cancel")
	r.wait("self turn stopped", func() bool { return len(r.own(work, textCancelGoalTurn)) == 1 })
	r.wait("stopped", func() bool {
		ga.gmu.Lock()
		defer ga.gmu.Unlock()
		return !ga.selfTurn
	})
	// Nothing at all: the plain reply.
	r.ch.PostUser(work, "helper cancel")
	r.wait("nothing", func() bool { return len(r.own(work, "Nothing to cancel")) == 1 })
}

// waitIdle waits until conv has no running turn.
func (r *rig) waitIdle(chat string) {
	r.wait("no running turn", func() bool {
		r.e.mu.Lock()
		defer r.e.mu.Unlock()
		for _, cs := range r.e.convs {
			if cs.chat == chat {
				return cs.running == nil
			}
		}
		return false
	})
}

// A local session's live records close with the goal and model lines the
// rollout recorded (turn_context, thread_settings_applied,
// thread_goal_updated); the lines survive a restart that resumes the
// rollout after the records that said them.
func TestMirroredRecordsCarryGoalAndModelLines(t *testing.T) {
	r, sessions := mirrorRig(t)
	r.mirrorInitialised()
	w, err := rollout.New(sessions, t.TempDir(), rollout.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendRecord("event_msg", map[string]any{"type": "thread_settings_applied", "thread_id": w.SessionID,
		"thread_settings": map[string]any{"model": "gpt-local", "service_tier": "flex", "reasoning_effort": "xhigh"}}); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendRecord("event_msg", map[string]any{"type": "thread_goal_updated", "threadId": w.SessionID,
		"goal": map[string]any{"threadId": w.SessionID, "objective": "Make the build green", "status": "active", "tokensUsed": 1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendTurn(rollout.Turn{Prompt: "local prompt", Commentary: "local status", Answer: "local answer"}); err != nil {
		t.Fatal(err)
	}
	chat := r.localChat()
	r.wait("answer", func() bool { return len(r.own(chat, "local answer")) == 1 })
	ans := lastOwn(r, chat, "local answer")
	// turn_context (written by the turn) said gpt-5-codex / medium last.
	if !strings.Contains(ans, "\n---\n🎯 Active goal: Make the build green\n🔧 Helper: model - gpt-5-codex, effort - medium, tier - flex\n🔧 Helper: ✅ Codex finished responding.") {
		t.Fatalf("mirrored answer: %q", ans)
	}
	if st := lastOwn(r, chat, "local status"); !strings.Contains(st, "\n---\n🎯 Active goal: Make the build green") || strings.Contains(st, "model - ") {
		t.Fatalf("mirrored status: %q", st)
	}
	if p := lastOwn(r, chat, "local prompt"); strings.Contains(p, "Active goal") {
		t.Fatalf("mirrored prompt: %q", p)
	}

	// A restart resumes after those records: the persisted state says it.
	r.stop()
	if err := w.AppendRecord("event_msg", map[string]any{"type": "token_count", "info": nil}); err != nil {
		t.Fatal(err)
	}
	r.start()
	r.control()
	// A turn the restarted mirror may still count as catch-up (no lines);
	// once it is posted, the rollout's catch-up end is known.
	if _, err := w.AppendTurn(rollout.Turn{Prompt: "between prompt", Answer: "between answer"}); err != nil {
		t.Fatal(err)
	}
	r.wait("between answer", func() bool { return len(r.own(chat, "between answer")) == 1 })
	if _, err := w.AppendTurn(rollout.Turn{Prompt: "second prompt", Answer: "second answer"}); err != nil {
		t.Fatal(err)
	}
	r.wait("second answer", func() bool { return len(r.own(chat, "second answer")) == 1 })
	if ans := lastOwn(r, chat, "second answer"); !strings.Contains(ans, "🎯 Active goal: Make the build green") || !strings.Contains(ans, "tier - flex") {
		t.Fatalf("after a restart: %q", ans)
	}

	// The goal paused in the rollout: no goal line any more.
	if err := w.AppendRecord("event_msg", map[string]any{"type": "thread_goal_updated", "threadId": w.SessionID,
		"goal": map[string]any{"objective": "Make the build green", "status": "paused"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendTurn(rollout.Turn{Prompt: "third prompt", Answer: "third answer"}); err != nil {
		t.Fatal(err)
	}
	r.wait("third answer", func() bool { return len(r.own(chat, "third answer")) == 1 })
	if ans := lastOwn(r, chat, "third answer"); strings.Contains(ans, "Active goal") || !strings.Contains(ans, "model - gpt-5-codex") {
		t.Fatalf("with the goal paused: %q", ans)
	}
}
