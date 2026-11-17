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
	if change != (GoalState{}) {
		cs.goalMu.Lock()
		defer cs.goalMu.Unlock()
		superseded := false
		err := e.update(func(tx *store.Tx) error {
			cur, ok, err := tx.GoalControl(cs.id)
			if err != nil {
				return err
			}
			if ok && cur.Inbound > j.inbound {
				superseded = true
				return nil
			}
			next := store.GoalControl{Conv: cs.id, Inbound: j.inbound, Thread: threadID, Cwd: cwd}
			if ok && cur.Pending && change.Status != GoalActive {
				next.Pending = true
				next.Turn = cur.Turn
				next.NativeTurn = cur.NativeTurn
			}
			return tx.SetGoalControl(next)
		})
		if err != nil {
			e.reply(j, goalFailed(err), nil)
			return
		}
		if superseded {
			e.reply(j, "This goal change was superseded by a newer goal control or cancellation.", nil)
			return
		}
	}
	cctx, cancel := context.WithTimeout(ctx, goalCallTimeout)
	defer cancel()
	alreadyActive := false
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
		alreadyActive = cur.Status == GoalActive
	}
	g, err := ga.Goal(cctx, cs.id, threadID, cwd, change)
	if err != nil {
		e.reply(j, goalFailed(err), nil)
		return
	}
	if change != (GoalState{}) {
		superseded := false
		x := e.update(func(tx *store.Tx) error {
			cur, ok, x := tx.GoalControl(cs.id)
			if x != nil {
				return x
			}
			if ok && cur.Inbound > j.inbound {
				superseded = true
				return nil
			}
			return tx.FinishGoalControl(cs.id, j.inbound)
		})
		if x != nil {
			e.reply(j, goalFailed(x), nil)
			return
		}
		if superseded {
			e.reply(j, "This goal change was superseded by a newer goal control or cancellation.", nil)
			return
		}
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
	case alreadyActive && g.Status == GoalActive:
		e.reply(j, goalText("The goal is already active.", g)+"\n\n"+textGoalActiveHint, nil)
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
		return "The goal operation was not confirmed: " + ErrActiveWriter.Error() + ". Close it there (or wait until it is idle) and try again."
	}
	return "The goal operation was not confirmed: " + userError(err.Error())
}

// finishGoalStop reconciles durable intent. A failure leaves it pending and
// blocks new helper input until a confirmed retry or newer explicit control.
func (e *Engine) finishGoalStop(ctx context.Context, g store.GoalControl) {
	cs := e.conv(g.Conv)
	if cs == nil {
		return
	}
	cs.goalMu.Lock()
	defer cs.goalMu.Unlock()
	var cur store.GoalControl
	if err := e.st.Read(func(tx *store.Tx) error { var err error; cur, _, err = tx.GoalControl(g.Conv); return err }); err != nil {
		return
	}
	if cur.Inbound != g.Inbound || !cur.Pending {
		return
	}
	ga, ok := e.ag.(GoalAgent)
	if !ok {
		return
	}
	if g.Turn != 0 {
		// Fence steering before releasing the durable stop barrier. Interrupt
		// only after its goal result is queued, so a fast execution completion
		// cannot put the cancellation notice ahead of the goal confirmation.
		e.mu.Lock()
		if rs := cs.running; rs != nil && rs.turnID == g.Turn {
			rs.cancelled = true
		}
		e.mu.Unlock()
		// A failed native pause still stops the specifically selected helper
		// execution. Never retarget a later helper request.
		defer e.cancelRunningFor(ctx, g.Conv, g.Turn)
	}
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	res, stopErr := ga.StopGoal(cctx, g.Conv, g.Thread, g.Cwd, g.NativeTurn)
	if ctx.Err() != nil {
		return
	} // replay on startup; no premature terminal reply
	text := "Goal state checked: no active goal remains."
	if res.Paused {
		text = textCancelGoal
	}
	if res.Interrupted {
		text += " The selected native turn has stopped."
	}
	suffix := "confirmed"
	if stopErr != nil {
		suffix = "unconfirmed"
		text = "⚠️ Goal stop is unconfirmed: " + userError(stopErr.Error()) + ". New helper input is held. Retry `helper cancel running` or use `helper goal pause`."
	}
	err := e.update(func(tx *store.Tx) error {
		cur, ok, err := tx.GoalControl(g.Conv)
		if err != nil {
			return err
		}
		if !ok || cur.Inbound != g.Inbound {
			return nil
		}
		if stopErr == nil {
			if err := tx.FinishGoalControl(g.Conv, g.Inbound); err != nil {
				return err
			}
		}
		return e.enqueueNotice(tx, g.Conv, fmt.Sprintf("goal-stop:%d:%s", g.Inbound, suffix), text)
	})
	if err != nil {
		e.logf("goal stop %d: %v", g.Inbound, err)
	}
	e.wakeSender()
	e.wakeRunner(g.Conv)
}
func (e *Engine) resumeGoalStops(ctx context.Context) error {
	var rows []store.GoalControl
	if err := e.st.Read(func(tx *store.Tx) error { var err error; rows, err = tx.PendingGoalControls(); return err }); err != nil {
		return err
	}
	for _, g := range rows {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		e.finishGoalStop(ctx, g)
	}
	return nil
}

// Recovery is a finite worker, not an execution scheduler. Pending stops already
// gate their own conversation, so a slow native process cannot stall startup
// and input in every other chat.
func (e *Engine) recoverGoalStops(ctx context.Context) {
	if err := e.resumeGoalStops(ctx); err != nil && ctx.Err() == nil {
		e.logf("recover goal stops: %v", err)
	}
}
