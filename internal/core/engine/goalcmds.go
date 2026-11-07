package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// `helper goal` (owner request 2026-11-07): the Codex goal of a work
// chat's thread (Codex 0.16x thread goals, the TUI's /goal).
//
//	helper goal              shows the goal: none, active or paused (other
//	                         Codex statuses as they are) and its objective
//	helper goal <objective>  sets the goal and makes it active
//	helper goal active       makes the goal active again
//	helper goal pause        pauses it
//
// An active goal makes Codex keep working on the thread by itself whenever
// it is idle: those turns reach the chat through the mirror, like a local
// session's. Codex's interrupt leaves a goal active, so `helper cancel`
// pauses it (cmdCancel): otherwise the next message would restart it.

const (
	cmdGoalName = "goal"
	// goalCallTimeout bounds a goal call, an app-server start and
	// thread/resume included.
	goalCallTimeout = 2 * time.Minute

	textGoalUsage = "`helper goal` shows the goal of this chat's Codex thread; `helper goal <objective>` sets it and makes it active; " +
		"`helper goal active` makes it active again; `helper goal pause` pauses it."
	textGoalNoThread    = "This chat has no Codex thread yet. Send a first request, then set its goal."
	textGoalUnsupported = "Goals are not available: this helper's Codex agent does not support them."
	textGoalNone        = "No goal is set for this chat.\n\n" + textGoalUsage
	textGoalPausedHint  = "Codex does not continue it by itself until `helper goal active`."
	textGoalActiveHint  = "Codex keeps working on it by itself whenever this chat is idle; its messages appear here. `helper goal pause` or `helper cancel` stops it."
	textCancelGoal      = "⏸️ The goal is paused too, so your next message does not start it again. `helper goal active` resumes it."
	textCancelGoalTurn  = "⏹️ Stopping the turn Codex is running by itself for the goal, and pausing the goal so your next message does not start it again. `helper goal active` resumes it."
	textCancelGoalOnly  = "⏸️ No request is running or queued; the active goal is paused, so your next message does not start it again. `helper goal active` resumes it."
)

// goalArg is what `helper goal <arg>` asks for (change zero: show).
func goalArg(arg string) GoalState {
	arg = strings.TrimSpace(arg)
	switch strings.ToLower(arg) {
	case "":
		return GoalState{}
	case "active", "activate", "resume":
		return GoalState{Status: GoalActive}
	case "pause", "paused":
		return GoalState{Status: GoalPaused}
	}
	return GoalState{Objective: arg, Status: GoalActive}
}

func (e *Engine) cmdGoal(ctx context.Context, j cmdJob, cs *convState, arg string) {
	ga, ok := e.ag.(GoalAgent)
	if !ok {
		e.reply(j, textGoalUnsupported, nil)
		return
	}
	e.mu.Lock()
	threadID, cwd := cs.threadID, cs.cwd
	e.mu.Unlock()
	if threadID == "" {
		e.reply(j, textGoalNoThread, nil)
		return
	}
	change := goalArg(arg)
	cctx, cancel := context.WithTimeout(ctx, goalCallTimeout)
	defer cancel()
	if change.Objective == "" && change.Status == GoalActive {
		// `active` needs a goal: Codex has nothing to make active.
		cur, err := ga.Goal(cctx, cs.id, threadID, cwd, GoalState{})
		if err != nil {
			e.reply(j, goalFailed(err), nil)
			return
		}
		if cur.Status == "" {
			e.reply(j, textGoalNone, nil)
			return
		}
		if cur.Status == GoalActive {
			e.reply(j, goalText("The goal is already active.", cur)+"\n\n"+textGoalActiveHint, nil)
			return
		}
	}
	g, err := ga.Goal(cctx, cs.id, threadID, cwd, change)
	if err != nil {
		e.reply(j, goalFailed(err), nil)
		return
	}
	switch {
	case change == (GoalState{}):
		if g.Status == "" {
			e.reply(j, textGoalNone, nil)
			return
		}
		e.reply(j, goalText("", g), nil)
	case change.Objective != "":
		e.reply(j, goalText("🎯 Goal set.", g)+"\n\n"+textGoalActiveHint, nil)
	case change.Status == GoalActive:
		e.reply(j, goalText("▶️ Goal active again.", g)+"\n\n"+textGoalActiveHint, nil)
	default:
		e.reply(j, goalText("⏸️ Goal paused. A turn Codex is already running finishes unless you `helper cancel` it.", g)+"\n\n"+textGoalPausedHint, nil)
	}
	e.heat(cs.id) // its turns show up in the rollout soon
}

// goalText describes g after head.
func goalText(head string, g GoalState) string {
	var s strings.Builder
	if head != "" {
		s.WriteString(head + "\n\n")
	}
	status := g.Status
	if status == "" {
		status = "none"
	}
	fmt.Fprintf(&s, "**Goal status:** %s\n\n**Goal:** %s", status, strings.TrimSpace(g.Objective))
	return s.String()
}

func goalFailed(err error) string {
	if errors.Is(err, ErrActiveWriter) {
		return "The goal was not changed: " + ErrActiveWriter.Error() + ". Close it there (or wait until it is idle) and try again."
	}
	return "The goal could not be read or changed: " + userError(err.Error())
}

// stopGoal is the goal part of `helper cancel`, after its reply: it pauses
// the active goal and interrupts a turn Codex runs by itself, before the
// helper's own running turn is interrupted (so its end does not start
// the goal again).
func (e *Engine) stopGoal(ctx context.Context, cs *convState) {
	ga, ok := e.ag.(GoalAgent)
	if !ok {
		return
	}
	threadID := e.threadOf(cs.id)
	if threadID == "" {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	res, err := ga.StopGoal(cctx, cs.id, threadID)
	if err != nil {
		e.logf("cancel in %d: stop the goal: %v", cs.id, err)
		if err := e.update(func(tx *store.Tx) error {
			return e.enqueueNotice(tx, cs.id, fmt.Sprintf("goal-stop:%d", e.now().UnixNano()), "⚠️ The goal could not be paused: "+userError(err.Error())+". Try `helper goal pause`.")
		}); err != nil {
			e.logf("cancel in %d: %v", cs.id, err)
		}
		e.wakeSender()
		return
	}
	if res.Paused || res.Interrupted {
		e.logf("cancel in %d: goal paused %v, Codex turn interrupted %v", cs.id, res.Paused, res.Interrupted)
	}
}
