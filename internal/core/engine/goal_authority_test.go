package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/goal"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
)

func TestGoalStopUnknownSnapshotWaitsForConfirmation(t *testing.T) {
	r, ga := goalRig(t)
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "bind")
	r.waitIdle(work)
	r.wait("bound", func() bool { return r.e.threadOf(convID(t, r, "s001")) != "" })
	th := r.e.threadOf(convID(t, r, "s001"))
	entered, release := make(chan struct{}), make(chan struct{})
	ga.gmu.Lock()
	ga.goals[th] = GoalState{Objective: "work", Status: GoalActive}
	ga.hideGoal = true
	ga.stopHook = func(ctx context.Context) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	ga.gmu.Unlock()
	r.ch.PostUser(work, "helper cancel running")
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("stop not invoked")
	}
	if len(r.own(work, textCancelGoal)) != 0 {
		t.Fatal("pause success predicted before native response")
	}
	r.ch.PostUser(work, "after cancellation")
	id := convID(t, r, "s001")
	r.wait("input queued", func() bool {
		queued := false
		_ = r.st.Read(func(tx *store.Tx) error {
			rows, err := tx.OpenTurnsOf(id)
			for _, tr := range rows {
				if tr.State == store.TurnQueued {
					queued = true
				}
			}
			return err
		})
		return queued
	})
	if r.ag.calls("after cancellation") != 0 {
		t.Fatal("input started through pending stop")
	}
	close(release)
	r.wait("pause confirmed", func() bool { return len(r.own(work, textCancelGoal)) > 0 })
	r.wait("input released", func() bool { return r.ag.calls("after cancellation") == 1 })
	ga.gmu.Lock()
	defer ga.gmu.Unlock()
	if ga.goals[th].Status != GoalPaused {
		t.Fatal("unknown snapshot skipped pause")
	}
}
func TestGoalStopFailureSurvivesRestart(t *testing.T) {
	r, ga := goalRig(t)
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "bind")
	r.wait("bound", func() bool { return r.e.threadOf(convID(t, r, "s001")) != "" })
	r.waitIdle(work)
	ga.gmu.Lock()
	ga.stopHook = func(context.Context) error { return errors.New("native unavailable") }
	ga.gmu.Unlock()
	r.ch.PostUser(work, "helper cancel running")
	r.wait("unconfirmed stop", func() bool { return len(r.own(work, "Goal stop is unconfirmed")) > 0 })
	if len(r.own(work, textCancelGoal)) > 0 {
		t.Fatal("failed stop reported paused")
	}
	var pending []store.GoalControl
	if err := r.st.Read(func(tx *store.Tx) error { var err error; pending, err = tx.PendingGoalControls(); return err }); err != nil || len(pending) != 1 {
		t.Fatalf("pending=%v err=%v", pending, err)
	}
	r.stop()
	ga.gmu.Lock()
	ga.stopHook = nil
	ga.gmu.Unlock()
	r.start()
	r.wait("stop replayed", func() bool { return len(r.own(work, "no active goal remains")) > 0 })
	if err := r.st.Read(func(tx *store.Tx) error { var err error; pending, err = tx.PendingGoalControls(); return err }); err != nil || len(pending) != 0 {
		t.Fatalf("pending=%v err=%v", pending, err)
	}
}
func TestGoalCurrentSnapshotOverridesHistoricalMirror(t *testing.T) {
	r, ga := goalRig(t)
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "bind")
	r.wait("bound", func() bool { return r.e.threadOf(convID(t, r, "s001")) != "" })
	r.waitIdle(work)
	th := r.e.threadOf(convID(t, r, "s001"))
	ga.gmu.Lock()
	ga.goals[th] = GoalState{Objective: "native objective", Status: GoalActive}
	ga.gmu.Unlock()
	out, err := r.e.renderMirror(context.Background(), th, transcript.Output{RecordID: "native-goal-no-log", Kind: transcript.OutputFinal, Text: "progress"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) == 0 || !strings.Contains(out[0].Body, "native objective") {
		t.Fatalf("missing native goal: %+v", out)
	}
	ga.gmu.Lock()
	delete(ga.goals, th)
	ga.gmu.Unlock()
	out, err = r.e.renderMirror(context.Background(), th, transcript.Output{RecordID: "stale-goal-log", Kind: transcript.OutputFinal, Text: "progress", Info: transcript.Info{Goal: &transcript.Goal{Objective: "stale objective", Status: GoalActive}}}, true)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out[0].Body, "stale objective") {
		t.Fatal("history overrode native clear")
	}
}
func TestGoalStatusPresentation(t *testing.T) {
	for _, status := range []string{"active", "paused", "blocked", "usageLimited", "budgetLimited", "complete"} {
		s := goal.Snapshot{Known: true, State: goal.State{Objective: "task", Status: status}}
		notes := infoNotes(TurnInfo{Goal: s}, true)
		if len(notes) == 0 {
			t.Fatal(status)
		}
		if status != "active" && strings.Contains(notes[0].Label, "Active goal") {
			t.Fatal(status)
		}
	}
}

func TestGoalCancellationSupersedesQueuedActivation(t *testing.T) {
	r, ga := goalRig(t)
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "bind")
	r.wait("bound", func() bool { return r.e.threadOf(convID(t, r, "s001")) != "" })
	r.waitIdle(work)
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	ga.gmu.Lock()
	ga.modelsHook = func(ctx context.Context) ([]Model, error) {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, errors.New("test catalog unavailable")
	}
	ga.gmu.Unlock()
	r.ch.PostUser(work, "helper model list")
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("catalog not entered")
	}
	r.ch.PostUser(work, "helper goal older activation")
	id := convID(t, r, "s001")
	r.wait("activation admitted", func() bool {
		n := 0
		_ = r.st.Read(func(tx *store.Tx) error { rows, err := tx.PendingCommands(id); n = len(rows); return err })
		return n >= 2
	})
	r.ch.PostUser(work, "helper cancel running")
	r.wait("cancel bypassed slow command", func() bool { return len(r.own(work, "no active goal remains")) > 0 })
	// Release only after the urgent cancellation has completed.
	ga.gmu.Lock()
	ga.modelsHook = nil
	ga.gmu.Unlock()
	release <- struct{}{}
	r.wait("older activation rejected", func() bool { return len(r.own(work, "superseded by a newer")) > 0 })
	if strings.Contains(strings.Join(ga.callLog(), "|"), "set active") {
		t.Fatal("old activation overtook cancel")
	}
}

func TestGoalCancellationWaitsForInflightMutation(t *testing.T) {
	r, ga := goalRig(t)
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "bind")
	r.wait("bound", func() bool { return r.e.threadOf(convID(t, r, "s001")) != "" })
	r.waitIdle(work)
	entered, release := make(chan struct{}), make(chan struct{})
	ga.gmu.Lock()
	ga.goalHook = func(ctx context.Context, g GoalState) error {
		if g.Objective != "inflight" {
			return nil
		}
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	ga.gmu.Unlock()
	r.ch.PostUser(work, "helper goal inflight")
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("mutation not entered")
	}
	r.ch.PostUser(work, "helper cancel running")
	r.wait("cancel intent durable", func() bool { return len(r.own(work, "Checking and stopping the goal")) > 0 })
	if len(r.own(work, textCancelGoal)) != 0 {
		t.Fatal("cancel confirmed while mutation still running")
	}
	close(release)
	r.wait("pause confirmed", func() bool { return len(r.own(work, textCancelGoal)) > 0 })
	r.wait("old mutation superseded", func() bool { return len(r.own(work, "superseded by a newer")) > 0 })
	ga.gmu.Lock()
	defer ga.gmu.Unlock()
	if ga.goals[r.e.threadOf(convID(t, r, "s001"))].Status != GoalPaused {
		t.Fatal("cancel lost to in-flight activation")
	}
}

func TestGoalRecoveryDoesNotBlockOtherChatsOrRetargetNativeTurn(t *testing.T) {
	r, ga := goalRig(t)
	work := r.newWork(t.TempDir())
	other := r.newWork(t.TempDir())
	r.ch.PostUser(work, "bind")
	r.wait("bound", func() bool { return r.e.threadOf(convID(t, r, "s001")) != "" })
	r.waitIdle(work)
	ga.gmu.Lock()
	ga.selfTurn = true
	ga.nativeTurn = "old-native"
	ga.stopHook = func(context.Context) error { return errors.New("offline") }
	ga.gmu.Unlock()
	r.ch.PostUser(work, "helper cancel running")
	r.wait("stop failed", func() bool { return len(r.own(work, "Goal stop is unconfirmed")) > 0 })
	r.stop()
	entered, release := make(chan struct{}), make(chan struct{})
	ga.gmu.Lock()
	ga.nativeTurn = "new-native"
	ga.stopHook = func(ctx context.Context) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	ga.gmu.Unlock()
	r.start()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery not entered")
	}
	r.ch.PostUser(other, "unrelated question")
	r.wait("other chat works during recovery", func() bool { return len(r.own(other, "ANSWER to unrelated question")) == 1 })
	close(release)
	r.wait("recovery done", func() bool { return len(r.own(work, "no active goal remains")) > 0 })
	ga.gmu.Lock()
	defer ga.gmu.Unlock()
	if !ga.selfTurn {
		t.Fatal("recovery interrupted a newer native turn")
	}
}

func TestGoalUnknownDoesNotAuthorizeModelSwitch(t *testing.T) {
	r, ga := goalRig(t)
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "bind")
	r.wait("bound", func() bool { return r.e.threadOf(convID(t, r, "s001")) != "" })
	r.waitIdle(work)
	th := r.e.threadOf(convID(t, r, "s001"))
	ga.gmu.Lock()
	ga.hideGoal = true
	ga.goals[th] = GoalState{Objective: "work", Status: GoalActive}
	ga.modelsHook = func(context.Context) ([]Model, error) { return switchModels, nil }
	ga.gmu.Unlock()
	r.ch.PostUser(work, "helper model use model-b")
	r.wait("switch rejected on current goal", func() bool { return len(r.own(work, "independently continuing Codex goal")) > 0 })
	if !strings.Contains(strings.Join(ga.callLog(), "|"), "get") {
		t.Fatal("switch used only the unknown cached state")
	}
}

func TestGoalTriggerWithMissingSnapshotIsExplicitlyUnknown(t *testing.T) {
	notes := infoNotes(TurnInfo{GoalTriggered: true}, true)
	if len(notes) != 1 || notes[0].Text != "unconfirmed" {
		t.Fatal(notes)
	}
	if len(infoNotes(TurnInfo{}, true)) != 0 {
		t.Fatal("ordinary messages acquired a goal annotation")
	}
}

func TestGoalStopInterruptedAfterAcknowledgmentReplays(t *testing.T) {
	r, ga := goalRig(t)
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "bind")
	r.wait("bound", func() bool { return r.e.threadOf(convID(t, r, "s001")) != "" })
	r.waitIdle(work)
	th := r.e.threadOf(convID(t, r, "s001"))
	entered := make(chan struct{})
	ga.gmu.Lock()
	ga.goals[th] = GoalState{Objective: "persist stop", Status: GoalActive}
	ga.stopHook = func(ctx context.Context) error { close(entered); <-ctx.Done(); return ctx.Err() }
	ga.gmu.Unlock()
	r.ch.PostUser(work, "helper cancel running")
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("stop not entered")
	}
	r.wait("acknowledgment sent", func() bool { return len(r.own(work, "Checking and stopping the goal")) > 0 })
	r.stop()
	ga.gmu.Lock()
	ga.stopHook = nil
	ga.gmu.Unlock()
	r.start()
	r.wait("pending stop replayed", func() bool { return len(r.own(work, textCancelGoal)) == 1 })
	if len(r.own(work, "Goal stop is unconfirmed")) != 0 {
		t.Fatal("shutdown was reported as terminal failure")
	}
}

func TestGoalAlreadyActiveStillReconcilesExecution(t *testing.T) {
	r, ga := goalRig(t)
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "bind")
	r.wait("bound", func() bool { return r.e.threadOf(convID(t, r, "s001")) != "" })
	r.waitIdle(work)
	th := r.e.threadOf(convID(t, r, "s001"))
	ga.gmu.Lock()
	ga.goals[th] = GoalState{Objective: "cold active", Status: GoalActive}
	ga.gmu.Unlock()
	r.ch.PostUser(work, "helper goal active")
	r.wait("already active reconciled", func() bool { return len(r.own(work, "The goal is already active")) > 0 })
	if !strings.Contains(strings.Join(ga.callLog(), "|"), "activate") {
		t.Fatal("already-active shortcut skipped explicit activation")
	}
}
