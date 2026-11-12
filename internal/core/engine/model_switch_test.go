package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

type switchTestAgent struct {
	*fakeAgent
	modelsGate    <-chan struct{}
	interruptGate <-chan struct{}
	interruptSeen chan struct{}
	once          sync.Once
	interrupts    atomic.Int32
}

func (a *switchTestAgent) Models(ctx context.Context) ([]Model, error) {
	if a.modelsGate != nil {
		select {
		case <-a.modelsGate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return switchModels, nil
}
func (a *switchTestAgent) InterruptExecution(ctx context.Context, id int64, key string) error {
	a.interrupts.Add(1)
	if a.interruptSeen != nil {
		a.once.Do(func() { close(a.interruptSeen) })
	}
	if a.interruptGate != nil {
		select {
		case <-a.interruptGate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	a.mu.Lock()
	latest := ""
	for _, run := range a.runs {
		if run.ConvID == id {
			latest = run.TurnKey
		}
	}
	if latest != key {
		a.mu.Unlock()
		return nil
	}
	irq, hook := a.irq[id], a.interrupted
	if irq != nil {
		close(irq)
		delete(a.irq, id)
	}
	a.mu.Unlock()
	if irq != nil && hook != nil {
		hook()
	}
	return nil
}

var switchModels = []Model{
	{ID: "model-a", DisplayName: "Model A", Default: true, Effort: "medium", Efforts: []string{"low", "medium", "high"}},
	{ID: "model-b", DisplayName: "Model B", Effort: "low", Efforts: []string{"low", "high"}},
	{ID: "model-c", DisplayName: "Model C", Effort: "medium", Efforts: []string{"medium", "high"}},
}

func TestModelAndEffortParsing(t *testing.T) {
	c := modelChat{models: switchModels}
	for _, tc := range []struct {
		input, model, effort string
		valid                bool
	}{
		{"model-b high", "model-b", "high", true}, {"Model B high", "model-b", "high", true}, {"Model B", "model-b", "", true}, {"model-b invalid", "", "", false}, {"missing high", "", "", false}, {"model-b high extra", "", "", false},
	} {
		m, _, eff, failed := c.modelAndEffort(tc.input)
		if (failed == "") != tc.valid || tc.valid && (m != tc.model || eff != tc.effort) {
			t.Fatalf("%s: %s/%s %s", tc.input, m, eff, failed)
		}
	}
}
func TestBusyModelSwitchAutomaticallyContinues(t *testing.T) {
	a := &switchTestAgent{fakeAgent: newAgent()}
	a.block["original task"] = make(chan struct{})
	r := newRig(t, func(c *Config) { c.Agent = a; c.FilesDir = t.TempDir() })
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "original task")
	r.wait("old execution", func() bool { return len(a.runLog()) == 1 })
	r.ch.PostUser(work, "model use model-b high")
	r.wait("automatic continuation", func() bool { return len(a.runLog()) == 2 })
	runs := a.runLog()
	if runs[1].Model != "model-b" || runs[1].Effort != "high" || runs[1].ThreadID == "" || runs[1].Text != continueText || runs[1].TurnKey == runs[0].TurnKey {
		t.Fatalf("continuation: %+v", runs[1])
	}
	r.wait("continuation answer", func() bool { return len(r.own(work, "ANSWER to "+continueText)) == 1 })
	r.e.mu.Lock()
	convID := r.e.byChat[work]
	r.e.mu.Unlock()
	var old, continued store.Turn
	if err := r.st.Update(func(tx *store.Tx) error {
		rows, err := tx.RecentTurns(convID, 10)
		if err != nil {
			return err
		}
		for _, tr := range rows {
			if tr.ResumeFrom != 0 {
				continued = tr
			} else {
				old = tr
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if continued.ResumeFrom != old.ID || continued.OutputKey != old.TurnKey {
		t.Fatalf("task binding: old=%+v continuation=%+v", old, continued)
	}
}
func TestSwitchCoalescesAndUsesPendingInput(t *testing.T) {
	gate := make(chan struct{})
	a := &switchTestAgent{fakeAgent: newAgent(), interruptGate: gate, interruptSeen: make(chan struct{})}
	a.block["original task"] = make(chan struct{})
	r := newRig(t, func(c *Config) { c.Agent = a; c.FilesDir = t.TempDir() })
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "original task")
	r.wait("old execution", func() bool { return len(a.runLog()) == 1 })
	r.ch.PostUser(work, "model use model-b high")
	select {
	case <-a.interruptSeen:
	case <-r.done:
		t.Fatal("engine stopped")
	}
	r.ch.PostUser(work, "use staging")
	r.ch.PostUser(work, "model use model-c high")
	r.ch.PostUser(work, "keep the original scope")
	r.wait("latest setting committed", func() bool { return len(r.own(work, "Target model: Codex Official (`model-c`)")) > 0 })
	close(gate)
	r.wait("continuation with updates", func() bool { return len(a.runLog()) == 2 })
	run := a.runLog()[1]
	if run.Model != "model-c" || run.Effort != "high" || !strings.Contains(run.Text, "use staging") || !strings.Contains(run.Text, "keep the original scope") || !strings.Contains(run.Text, teamsSteerSafety) || strings.Contains(run.Text, teamsRequestSafety) || strings.Contains(run.Text, "original task") {
		t.Fatalf("continuation: %+v", run)
	}
	r.wait("answer", func() bool { return len(r.own(work, "ANSWER to")) > 0 })
}

func TestSwitchInvalidCombinedEffortDoesNotInterrupt(t *testing.T) {
	a := &switchTestAgent{fakeAgent: newAgent(), interruptSeen: make(chan struct{})}
	a.block["original task"] = make(chan struct{})
	r := newRig(t, func(c *Config) { c.Agent = a })
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "original task")
	r.wait("old execution", func() bool { return len(a.runLog()) == 1 })
	r.ch.PostUser(work, "model use model-b unsupported")
	r.wait("invalid effort rejected", func() bool { return len(r.own(work, "not supported by model")) > 0 })
	select {
	case <-a.interruptSeen:
		t.Fatal("invalid pair interrupted work")
	default:
	}
	cs := r.e.findLabel("s001")
	if err := r.st.Read(func(tx *store.Tx) error {
		th, _, err := tx.Thread(cs.id)
		if th.Model != "" || th.SettingsVersion != 0 {
			t.Errorf("partially changed settings: %+v", th)
		}
		_, yes, x := tx.ModelSwitch(cs.id)
		if yes {
			t.Error("invalid pair created handoff")
		}
		if err != nil {
			return err
		}
		return x
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSwitchCancelWhileStoppingDoesNotResume(t *testing.T) {
	gate := make(chan struct{})
	a := &switchTestAgent{fakeAgent: newAgent(), interruptGate: gate, interruptSeen: make(chan struct{})}
	a.block["original task"] = make(chan struct{})
	r := newRig(t, func(c *Config) { c.Agent = a })
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "original task")
	r.wait("old execution", func() bool { return len(a.runLog()) == 1 })
	r.ch.PostUser(work, "model use model-b high")
	select {
	case <-a.interruptSeen:
	case <-r.done:
		t.Fatal("engine stopped")
	}
	r.ch.PostUser(work, "helper cancel running")
	r.wait("cancellation committed", func() bool { return len(r.own(work, "cancel requested for running turn")) > 0 })
	close(gate)
	r.wait("cancelled terminal", func() bool { return len(r.own(work, textCancelledRunning)) > 0 })
	if len(a.runLog()) != 1 {
		t.Fatalf("cancel revived task: %+v", a.runLog())
	}
}

type switchSteerTestAgent struct {
	*switchTestAgent
	steerMu sync.Mutex
	steers  int
	retain  bool
}

func (a *switchSteerTestAgent) CurrentTurn(conv int64, _ string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.irq[conv] != nil {
		return "turn-1"
	}
	return ""
}
func (a *switchSteerTestAgent) Steer(_ context.Context, req TurnRequest, native string) error {
	a.steerMu.Lock()
	defer a.steerMu.Unlock()
	if a.retain {
		a.mu.Lock()
		a.lookup[req.TurnKey] = TurnOutcome{CodexTurnID: native, Status: StatusInterrupted}
		a.mu.Unlock()
	}
	a.steers++
	return nil
}
func (a *switchSteerTestAgent) steerCount() int {
	a.steerMu.Lock()
	defer a.steerMu.Unlock()
	return a.steers
}

func TestSwitchValidationBlocksLaterSteerAndInvalidReleasesIt(t *testing.T) {
	gate := make(chan struct{})
	a := &switchSteerTestAgent{switchTestAgent: &switchTestAgent{fakeAgent: newAgent(), modelsGate: gate}}
	a.block["original task"] = make(chan struct{})
	r := newRig(t, func(c *Config) { c.Agent = a })
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "original task")
	r.wait("old execution", func() bool { return len(a.runLog()) == 1 })
	r.ch.PostUser(work, "model use missing high")
	r.ch.PostUser(work, "later correction")
	r.wait("correction admitted", func() bool { return len(r.own(work, "Received. Adding this input")) > 0 })
	if a.steerCount() != 0 {
		t.Fatal("input crossed setting validation barrier")
	}
	close(gate)
	r.wait("invalid command rejected", func() bool { return len(r.own(work, "Unknown model")) > 0 })
	r.wait("correction released", func() bool { return a.steerCount() == 1 })
	if len(a.runLog()) != 1 {
		t.Fatal("invalid setting interrupted original")
	}
}

func TestSwitchUnretainedSteerPauses(t *testing.T) {
	a := &switchSteerTestAgent{switchTestAgent: &switchTestAgent{fakeAgent: newAgent()}}
	a.block["original task"] = make(chan struct{})
	r := newRig(t, func(c *Config) { c.Agent = a })
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "original task")
	r.wait("old execution", func() bool { return len(a.runLog()) == 1 })
	r.ch.PostUser(work, "steer accepted but not saved")
	r.wait("steer accepted", func() bool { return a.steerCount() == 1 })
	r.ch.PostUser(work, "model use model-b high")
	r.wait("uncertain input pauses switch", func() bool { return len(r.own(work, "additional input could not be confirmed")) > 0 })
	if len(a.runLog()) != 1 {
		t.Fatal("continued without confirming input")
	}
}

func TestSwitchRetainedSteerContinuesWithoutReplayingInput(t *testing.T) {
	a := &switchSteerTestAgent{switchTestAgent: &switchTestAgent{fakeAgent: newAgent()}, retain: true}
	a.block["original task"] = make(chan struct{})
	r := newRig(t, func(c *Config) { c.Agent = a })
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "original task")
	r.wait("old execution", func() bool { return len(a.runLog()) == 1 })
	r.ch.PostUser(work, "retained correction")
	r.wait("steer accepted and retained", func() bool { return a.steerCount() == 1 })
	r.ch.PostUser(work, "model use model-b high")
	r.wait("continuation", func() bool { return len(a.runLog()) == 2 })
	if a.runLog()[1].Text != continueText || a.steerCount() != 1 {
		t.Fatal("retained input was replayed")
	}
}

type unconfirmedInterruptAgent struct {
	*switchTestAgent
	reject bool
	kills  atomic.Int32
}

func (a *unconfirmedInterruptAgent) InterruptExecution(context.Context, int64, string) error {
	a.interrupts.Add(1)
	if a.reject {
		return fmt.Errorf("interrupt transport failed")
	}
	return nil // RPC accepted, but the native execution has not stopped.
}
func (a *unconfirmedInterruptAgent) Kill(int64) { a.kills.Add(1) }

func TestSwitchUnconfirmedStopPausesWithoutKillOrContinuation(t *testing.T) {
	for _, reject := range []bool{true, false} {
		t.Run(fmt.Sprint(reject), func(t *testing.T) {
			a := &unconfirmedInterruptAgent{switchTestAgent: &switchTestAgent{fakeAgent: newAgent()}, reject: reject}
			a.block["original task"] = make(chan struct{})
			r := newRig(t, func(c *Config) { c.Agent = a; c.Timing.CancelWait = 100 * time.Millisecond })
			r.ag = a.fakeAgent
			work := r.newWork(t.TempDir())
			r.ch.PostUser(work, "original task")
			r.wait("old execution", func() bool { return len(a.runLog()) == 1 })
			r.ch.PostUser(work, "model use model-b high")
			r.wait("unconfirmed stop pauses", func() bool { return len(r.own(work, "Model switch paused:")) > 0 })
			cs := r.e.findLabel("s001")
			r.e.mu.Lock()
			paused, active := cs.paused, cs.running != nil
			r.e.mu.Unlock()
			if !paused || !active || len(a.runLog()) != 1 || a.interrupts.Load() != 1 || a.kills.Load() != 0 {
				t.Fatalf("unsafe stop: paused=%v active=%v runs=%d interrupts=%d kills=%d", paused, active, len(a.runLog()), a.interrupts.Load(), a.kills.Load())
			}
		})
	}
}

func (a *switchTestAgent) Lookup(ctx context.Context, conv int64, thread, key string) (TurnOutcome, bool, error) {
	if out, found, err := a.fakeAgent.Lookup(ctx, conv, thread, key); err != nil || found {
		return out, found, err
	}
	for i, run := range a.runLog() {
		if run.TurnKey == key {
			return TurnOutcome{CodexTurnID: fmt.Sprintf("turn-%d", i+1), Status: StatusInterrupted}, true, nil
		}
	}
	return TurnOutcome{}, false, nil
}

func TestSwitchCommandsCoalesceBeforeAdmission(t *testing.T) {
	for _, tc := range []struct {
		name          string
		commands      []string
		model, effort string
	}{
		{"duplicate", []string{"model use model-b high"}, "model-b", "high"},
		{"back to A", []string{"model use model-a medium"}, "model-a", "medium"},
		{"invalid latest", []string{"model use missing high"}, "model-b", "low"},
		{"effort patch", []string{"effort high"}, "model-b", "high"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gate := make(chan struct{})
			a := &switchTestAgent{fakeAgent: newAgent(), interruptGate: gate, interruptSeen: make(chan struct{})}
			a.block["original task"] = make(chan struct{})
			r := newRig(t, func(c *Config) { c.Agent = a })
			r.ag = a.fakeAgent
			work := r.newWork(t.TempDir())
			r.ch.PostUser(work, "original task")
			r.wait("old execution", func() bool { return len(a.runLog()) == 1 })
			first := "model use model-b low"
			if tc.name == "duplicate" {
				first = "model use model-b high"
			}
			r.ch.PostUser(work, first)
			select {
			case <-a.interruptSeen:
			case <-r.done:
				t.Fatal("engine stopped")
			}
			for _, cmd := range tc.commands {
				r.ch.PostUser(work, cmd)
			}
			r.ch.PostUser(work, "model status")
			r.wait("commands processed in order", func() bool { return len(r.own(work, "Model profile: Codex Official")) > 0 })
			close(gate)
			cs := r.e.findLabel("s001")
			r.wait("old stopped", func() bool { r.e.mu.Lock(); defer r.e.mu.Unlock(); return cs.running == nil })
			r.wait("one continuation", func() bool { return len(a.runLog()) == 2 })
			got := a.runLog()[1]
			if got.Model != tc.model || got.Effort != tc.effort || a.interrupts.Load() != 1 {
				t.Fatalf("new run=%+v interrupts=%d", got, a.interrupts.Load())
			}
		})
	}
}

func TestSwitchWaitsForVoiceAndCancellationCanStopWaitingTask(t *testing.T) {
	for _, cancelTask := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelTask), func(t *testing.T) {
			irqGate := make(chan struct{})
			asr := &fakeASR{release: make(chan struct{})}
			a := &switchTestAgent{fakeAgent: newAgent(), interruptGate: irqGate, interruptSeen: make(chan struct{})}
			a.block["original task"] = make(chan struct{})
			r := newRig(t, func(c *Config) { c.Agent = a; c.Transcriber = asr; c.FilesDir = t.TempDir() })
			r.ag = a.fakeAgent
			work := r.newWork(t.TempDir())
			r.ch.PostUser(work, "original task")
			r.wait("old execution", func() bool { return len(a.runLog()) == 1 })
			r.ch.PostUser(work, "model use model-b high")
			select {
			case <-a.interruptSeen:
			case <-r.done:
				t.Fatal("engine stopped")
			}
			ref := r.ch.AddFile("voice.ogg", []byte("OggS..."))
			ref.Kind = channel.FileVoice
			r.ch.PostUser(work, "", ref)
			r.wait("transcription started", func() bool { asr.mu.Lock(); defer asr.mu.Unlock(); return len(asr.paths) == 1 })
			close(irqGate)
			cs := r.e.findLabel("s001")
			r.wait("old execution fully stopped", func() bool { r.e.mu.Lock(); defer r.e.mu.Unlock(); return cs.running == nil })
			if len(a.runLog()) != 1 {
				t.Fatal("continued before pending voice input")
			}
			if cancelTask {
				r.ch.PostUser(work, "helper cancel running")
				r.wait("waiting logical task cancelled", func() bool { return len(r.own(work, "cancel requested for running turn")) > 0 })
			}
			close(asr.release)
			if !cancelTask {
				r.wait("voice continuation", func() bool { return len(a.runLog()) == 2 })
				got := a.runLog()[1]
				if !strings.Contains(got.Text, "hello from voice") || got.Text == continueText {
					t.Fatal("voice lost to generic continuation")
				}
			} else {
				r.wait("cancellation remains committed", func() bool {
					yes := true
					_ = r.st.Read(func(tx *store.Tx) error { _, pending, err := tx.ModelSwitch(cs.id); yes = !pending; return err })
					return yes
				})
				if len(a.runLog()) != 1 {
					t.Fatal("cancelled task revived")
				}
			}
		})
	}
}

func TestSwitchContinuationCapacityRetryDoesNotReplayInput(t *testing.T) {
	gate := make(chan struct{})
	a := &switchTestAgent{fakeAgent: newAgent(), interruptGate: gate, interruptSeen: make(chan struct{})}
	a.block["original task"] = make(chan struct{})
	var continuations atomic.Int32
	a.outcome = func(req TurnRequest) (TurnOutcome, error) {
		if req.Model == "model-b" && continuations.Add(1) == 1 {
			return TurnOutcome{CodexTurnID: "turn-2", Status: StatusFailed, Capacity: true, Error: "capacity"}, nil
		}
		return TurnOutcome{Status: StatusCompleted, Final: "recovered"}, nil
	}
	r := newRig(t, func(c *Config) { c.Agent = a })
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "original task")
	r.wait("old execution", func() bool { return len(a.runLog()) == 1 })
	r.ch.PostUser(work, "model use model-b high")
	select {
	case <-a.interruptSeen:
	case <-r.done:
		t.Fatal("engine stopped")
	}
	r.ch.PostUser(work, "use staging")
	r.wait("correction admitted", func() bool { return len(r.own(work, "Queued")) > 0 })
	close(gate)
	r.wait("recovered answer", func() bool { return len(r.own(work, "recovered")) > 0 })
	runs := a.runLog()
	if len(runs) != 3 || !strings.Contains(runs[1].Text, "use staging") || runs[2].Text != continueText {
		t.Fatalf("retry replayed input: %+v", runs)
	}
}

func TestSwitchNaturalCompletionDoesNotReviveTask(t *testing.T) {
	gate, completed := make(chan struct{}), make(chan struct{})
	a := &switchTestAgent{fakeAgent: newAgent(), interruptGate: gate, interruptSeen: make(chan struct{})}
	a.block["original task"] = completed
	r := newRig(t, func(c *Config) { c.Agent = a })
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "original task")
	r.wait("old execution", func() bool { return len(a.runLog()) == 1 })
	r.ch.PostUser(work, "model use model-b high")
	select {
	case <-a.interruptSeen:
	case <-r.done:
		t.Fatal("engine stopped")
	}
	close(completed)
	r.wait("natural answer delivered", func() bool { return len(r.own(work, "ANSWER to original task")) > 0 })
	close(gate)
	cs := r.e.findLabel("s001")
	r.wait("handoff cleared", func() bool {
		pending := true
		_ = r.st.Read(func(tx *store.Tx) error { _, pending, _ = tx.ModelSwitch(cs.id); return nil })
		return !pending
	})
	if len(a.runLog()) != 1 {
		t.Fatal("completed task revived")
	}
}

func TestSwitchRestartBeforeContinuationUsesSavedTarget(t *testing.T) {
	a := &switchTestAgent{fakeAgent: newAgent()}
	r := newRig(t, func(c *Config) { c.Agent = a })
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	cs := r.e.findLabel("s001")
	// Seed the exact durable cut point after the old terminal, before any
	// continuation reservation. No queued turn exists to wake the runner.
	r.stop()
	s, err := store.Open(r.path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"})
	if err != nil {
		t.Fatal(err)
	}
	err = s.Update(func(tx *store.Tx) error {
		th, _, x := tx.Thread(cs.id)
		if x != nil {
			return x
		}
		th.ThreadID, th.Model, th.Effort, th.SettingsVersion = "saved-thread", "model-c", "high", 3
		if x = tx.Bind(th); x != nil {
			return x
		}
		rows, x := tx.AdmitWindow(work, cs.id, []store.InboundMsg{{ExtMsgID: "seed", ExtTime: 1, Sender: store.SenderOwner, Kind: store.InPrompt, Body: "original task"}}, "seed")
		if x != nil {
			return x
		}
		if x = tx.MarkStarting(rows[0].TurnID, "saved-thread"); x != nil {
			return x
		}
		if x = tx.MarkRunning(rows[0].TurnID, "old-native"); x != nil {
			return x
		}
		old, _, x := tx.Turn(rows[0].TurnID)
		if x != nil {
			return x
		}
		if x = tx.BeginModelSwitch(old); x != nil {
			return x
		}
		return tx.FinishTurn(old.ID, store.TurnInterrupted, "model settings changed", nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	s.Release()
	s.Close()
	r.start()
	r.wait("recovered continuation", func() bool { return len(a.runLog()) == 1 })
	got := a.runLog()[0]
	if got.Text != continueText || got.ThreadID != "saved-thread" || got.Model != "model-c" || got.Effort != "high" {
		t.Fatalf("recovery replayed task or lost target: %+v", got)
	}
}

func TestSwitchAfterAdmissionCreatesNextHandoff(t *testing.T) {
	a := &switchTestAgent{fakeAgent: newAgent()}
	a.block["original task"] = make(chan struct{})
	a.block[continueText] = make(chan struct{})
	r := newRig(t, func(c *Config) { c.Agent = a })
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "original task")
	r.wait("first execution", func() bool { return len(a.runLog()) == 1 })
	r.ch.PostUser(work, "model use model-b high")
	r.wait("second execution", func() bool { return len(a.runLog()) == 2 })
	r.ch.PostUser(work, "model use model-c medium")
	r.wait("third execution", func() bool { return len(a.runLog()) == 3 })
	runs := a.runLog()
	if runs[2].Model != "model-c" || runs[2].Effort != "medium" || runs[2].ThreadID != runs[1].ThreadID || a.interrupts.Load() != 2 {
		t.Fatalf("later switch lost: %+v, interrupts=%d", runs, a.interrupts.Load())
	}
}

func TestSwitchRecoveryAfterSubmissionDoesNotReplay(t *testing.T) {
	for _, known := range []bool{false, true} {
		t.Run(fmt.Sprint(known), func(t *testing.T) {
			a := &switchTestAgent{fakeAgent: newAgent()}
			r := newRig(t, func(c *Config) { c.Agent = a })
			r.ag = a.fakeAgent
			work := r.newWork(t.TempDir())
			cs := r.e.findLabel("s001")
			r.stop()
			s, err := store.Open(r.path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"}); err != nil {
				t.Fatal(err)
			}
			var submitted store.Turn
			var supplement int64
			err = s.Update(func(tx *store.Tx) error {
				th, _, err := tx.Thread(cs.id)
				if err != nil {
					return err
				}
				th.ThreadID = "saved-thread"
				if err = tx.Bind(th); err != nil {
					return err
				}
				rows, err := tx.AdmitWindow(work, cs.id, []store.InboundMsg{{ExtMsgID: "seed", ExtTime: 1, Sender: store.SenderOwner, Kind: store.InPrompt, Body: "original task"}, {ExtMsgID: "correction", ExtTime: 2, Sender: store.SenderOwner, Kind: store.InPrompt, Body: "correction"}}, "seed")
				if err != nil {
					return err
				}
				supplement = rows[1].TurnID
				if err = tx.MarkStarting(rows[0].TurnID, "saved-thread"); err != nil {
					return err
				}
				old, _, err := tx.Turn(rows[0].TurnID)
				if err != nil {
					return err
				}
				if err = tx.BeginModelSwitch(old); err != nil {
					return err
				}
				if err = tx.FinishTurn(old.ID, store.TurnInterrupted, "model switch", nil); err != nil {
					return err
				}
				sw, _, err := tx.ModelSwitch(cs.id)
				if err != nil {
					return err
				}
				submitted, err = tx.NewContinuation(sw, old, nil)
				if err != nil {
					return err
				}
				if err = tx.AddContinuationInput(submitted.ID, supplement); err != nil {
					return err
				}
				if err = tx.MarkStarting(submitted.ID, "saved-thread"); err != nil {
					return err
				}
				return tx.EndModelSwitch(cs.id, old.ID, old.Attempt)
			})
			if err != nil {
				t.Fatal(err)
			}
			s.Release()
			s.Close()
			if known {
				a.lookup[submitted.TurnKey] = TurnOutcome{CodexTurnID: "saved-native", Status: StatusCompleted, Final: "saved answer"}
			}
			r.start()
			if known {
				r.wait("saved answer reconciled", func() bool { return len(r.own(work, "saved answer")) > 0 })
				_ = r.st.Read(func(tx *store.Tx) error {
					input, _, err := tx.Turn(supplement)
					if input.State != store.TurnCompleted {
						t.Errorf("accepted supplement remains open: %+v", input)
					}
					return err
				})
			} else {
				r.wait("unknown submission paused", func() bool { return len(r.own(work, "recovery could not confirm the submitted continuation")) > 0 })
			}
			if len(a.runLog()) != 0 {
				t.Fatal("recovery resubmitted uncertain or completed input")
			}
		})
	}
}

func TestSwitchCancelDuringValidationDoesNotRunSupplements(t *testing.T) {
	gate := make(chan struct{})
	a := &switchSteerTestAgent{switchTestAgent: &switchTestAgent{fakeAgent: newAgent(), modelsGate: gate}}
	a.block["original task"] = make(chan struct{})
	r := newRig(t, func(c *Config) { c.Agent = a })
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "original task")
	r.wait("original execution", func() bool { return len(a.runLog()) == 1 })
	r.ch.PostUser(work, "model use model-b high")
	r.ch.PostUser(work, "correction for active task")
	r.wait("correction admitted", func() bool { return len(r.own(work, "Received. Adding this input")) > 0 })
	r.ch.PostUser(work, "helper cancel running")
	r.wait("cancelled while catalog blocked", func() bool { return len(r.own(work, "Codex request canceled")) > 0 })
	close(gate)
	r.wait("target saved after cancellation", func() bool { return len(r.own(work, "Target model:")) > 0 })
	if len(a.runLog()) != 1 || a.steerCount() != 0 {
		t.Fatal("cancelled active task revived through a buffered supplement")
	}
}

func TestSwitchRetryPreservesInitialBackoff(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "retry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"}); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	e, err := New(Config{Store: s, Agent: newAgent(), Channel: channeltest.New(), Timing: FastTiming(20 * time.Millisecond), Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	var id, turn int64
	err = s.Update(func(tx *store.Tx) error {
		var err error
		id, err = tx.CreateConversation(store.NewConversation{Label: "retry", Kind: store.KindWork, Channel: "teams", ExtChatID: "retry"})
		if err != nil {
			return err
		}
		rows, err := tx.AdmitWindow("retry", id, []store.InboundMsg{{ExtMsgID: "task", ExtTime: 1, Sender: store.SenderOwner, Kind: store.InPrompt, Body: "task"}}, "1")
		if err != nil {
			return err
		}
		turn = rows[0].TurnID
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	cs := &convState{id: id, label: "retry"}
	for attempt := 1; attempt <= 3; attempt++ {
		var tr store.Turn
		err = s.Update(func(tx *store.Tx) error {
			if err := tx.MarkStarting(turn, ""); err != nil {
				return err
			}
			var err error
			tr, _, err = tx.Turn(turn)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		if !e.failureRetry(cs, tr, TurnOutcome{Status: StatusFailed, Capacity: true, NotStarted: true}) {
			t.Fatal("retry not scheduled")
		}
		want := e.t.CapacityBackoff << (attempt - 1)
		if back := cs.capWake.Sub(now); back != want {
			t.Fatalf("attempt %d retry delay changed: %s want %s", attempt, back, want)
		}
	}
}

type uncertainSwitchAgent struct {
	*switchTestAgent
	release chan struct{}
}

func (a *uncertainSwitchAgent) Run(ctx context.Context, req TurnRequest, h TurnHooks) (TurnOutcome, error) {
	a.mu.Lock()
	a.runs = append(a.runs, req)
	a.mu.Unlock()
	if err := h.ThreadStarted("uncertain-thread"); err != nil {
		return TurnOutcome{}, err
	}
	select {
	case <-a.release:
		return TurnOutcome{}, ErrUnknownOutcome
	case <-ctx.Done():
		return TurnOutcome{}, ctx.Err()
	}
}
func (a *uncertainSwitchAgent) InterruptExecution(context.Context, int64, string) error {
	close(a.release)
	return nil
}
func (a *uncertainSwitchAgent) Lookup(context.Context, int64, string, string) (TurnOutcome, bool, error) {
	return TurnOutcome{}, false, nil
}
func TestSwitchUnknownStartDoesNotResubmitOriginal(t *testing.T) {
	a := &uncertainSwitchAgent{switchTestAgent: &switchTestAgent{fakeAgent: newAgent()}, release: make(chan struct{})}
	r := newRig(t, func(c *Config) { c.Agent = a })
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "original task")
	r.wait("unconfirmed start in flight", func() bool { return len(a.runLog()) == 1 })
	r.ch.PostUser(work, "model use model-b high")
	r.wait("unknown start paused", func() bool { return len(r.own(work, "the start outcome could not be confirmed")) > 0 })
	if len(a.runLog()) != 1 {
		t.Fatal("unconfirmed original request replayed")
	}
}

func TestSwitchControlBeforeFirstQuestionCreatesBinding(t *testing.T) {
	a := &switchTestAgent{fakeAgent: newAgent()}
	r := newRig(t, func(c *Config) { c.Agent = a })
	r.ag = a.fakeAgent
	control := r.control()
	r.ch.PostUser(control, "model use model-b high")
	r.wait("control target saved", func() bool { return len(r.own(control, "Target model:")) > 0 })
	r.ch.PostUser(control, "ask test control model")
	r.wait("control question", func() bool { return len(a.runLog()) == 1 })
	got := a.runLog()[0]
	if got.Model != "model-b" || got.Effort != "high" {
		t.Fatalf("first control setting lost: %+v", got)
	}
}

func TestSwitchControlKeepsNewQuestionsSeparate(t *testing.T) {
	gate := make(chan struct{})
	a := &switchTestAgent{fakeAgent: newAgent(), interruptGate: gate, interruptSeen: make(chan struct{})}
	a.block[userMessageLead+"\nfirst-control-active"] = make(chan struct{})
	r := newRig(t, func(c *Config) { c.Agent = a })
	r.ag = a.fakeAgent
	control := r.control()
	r.ch.PostUser(control, "ask first-control-active")
	r.wait("first question", func() bool { return len(a.runLog()) == 1 })
	r.ch.PostUser(control, "model use model-b high")
	select {
	case <-a.interruptSeen:
	case <-r.done:
		t.Fatal("engine stopped")
	}
	r.ch.PostUser(control, "ask next-control-question")
	r.wait("new question admitted", func() bool { return len(r.own(control, "Codex received your control-chat question")) >= 2 })
	close(gate)
	r.wait("separate next question", func() bool { return len(a.runLog()) == 3 })
	runs := a.runLog()
	if runs[1].Text != continueText || runs[1].Cwd != runs[0].Cwd || !strings.Contains(runs[2].Text, "next-control-question") {
		t.Fatalf("control task boundary changed: %+v", runs)
	}
}

func TestSwitchDuringCapacityRetryTransfersUnsubmittedInput(t *testing.T) {
	a := &switchTestAgent{fakeAgent: newAgent()}
	a.block["original task"] = make(chan struct{})
	a.outcome = func(req TurnRequest) (TurnOutcome, error) {
		if req.Model == "model-b" {
			return TurnOutcome{Status: StatusFailed, Capacity: true, CodexTurnID: "turn-2"}, nil
		}
		return TurnOutcome{Status: StatusCompleted, Final: "transfer completed"}, nil
	}
	a.refuse = func(req TurnRequest) (TurnOutcome, bool) {
		if req.Model == "model-b" && strings.Contains(req.Text, "fresh correction") {
			return TurnOutcome{Status: StatusFailed, Capacity: true, NotStarted: true}, true
		}
		return TurnOutcome{}, false
	}
	r := newRig(t, func(c *Config) { c.Agent = a })
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "original task")
	r.wait("original execution", func() bool { return len(a.runLog()) == 1 })
	r.ch.PostUser(work, "model use model-b high")
	r.wait("accepted continuation waiting for capacity", func() bool { return len(r.own(work, "Codex is at capacity")) > 0 })
	r.ch.PostUser(work, "fresh correction")
	cs := r.e.findLabel("s001")
	r.wait("second attempt rejected before acceptance", func() bool {
		ready := false
		_ = r.st.Read(func(tx *store.Tx) error {
			rows, err := tx.OpenTurnsOf(cs.id)
			for _, tr := range rows {
				if tr.ResumeFrom != 0 && tr.State == store.TurnQueued && tr.Attempt == 2 {
					ready = true
				}
			}
			return err
		})
		return ready
	})
	r.ch.PostUser(work, "model use model-c high")
	r.wait("new model completes transferred input", func() bool { return len(r.own(work, "transfer completed")) > 0 })
	runs := a.runLog()
	if len(runs) != 4 || runs[3].Model != "model-c" || !strings.Contains(runs[3].Text, "fresh correction") || strings.Contains(runs[3].Text, "original task") {
		t.Fatalf("lost or replayed input after switch: %+v", runs)
	}
}

func TestSwitchDuplicateTargetDoesNotBypassRetryBackoff(t *testing.T) {
	a := &switchTestAgent{fakeAgent: newAgent()}
	a.outcome = func(req TurnRequest) (TurnOutcome, error) {
		return TurnOutcome{Status: StatusFailed, Capacity: true, CodexTurnID: "turn-1"}, nil
	}
	r := newRig(t, func(c *Config) { c.Agent = a; c.Timing.CapacityBackoff = 30 * time.Second })
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "model use model-b high")
	r.wait("target saved", func() bool { return len(r.own(work, "Target model:")) > 0 })
	r.ch.PostUser(work, "original task")
	r.wait("capacity backoff", func() bool { return len(r.own(work, "Codex is at capacity")) > 0 })
	cs := r.e.findLabel("s001")
	r.e.mu.Lock()
	before := cs.capWake
	r.e.mu.Unlock()
	r.ch.PostUser(work, "model use model-b high")
	r.ch.PostUser(work, "model status")
	r.wait("duplicate processed", func() bool { return len(r.own(work, "Model profile:")) > 0 })
	r.e.mu.Lock()
	after := cs.capWake
	r.e.mu.Unlock()
	if !after.Equal(before) || len(a.runLog()) != 1 {
		t.Fatalf("duplicate settings bypassed retry delay: %s -> %s, runs=%d", before, after, len(a.runLog()))
	}
}

func TestSwitchCancelledAfterAdmissionBeforeRunDoesNotStart(t *testing.T) {
	r := newRig(t)
	r.newWork(t.TempDir())
	cs := r.e.findLabel("s001")
	r.e.mu.Lock()
	cs.paused = true // keep the real runner out of this controlled cut point
	r.e.mu.Unlock()
	var admitted store.Turn
	err := r.e.update(func(tx *store.Tx) error {
		rows, err := tx.AdmitWindow("admission-boundary", cs.id, []store.InboundMsg{{ExtMsgID: "original", ExtTime: 1, Sender: store.SenderOwner, Kind: store.InPrompt, Body: "original task"}}, "1")
		if err != nil {
			return err
		}
		if err = tx.MarkStarting(rows[0].TurnID, "saved-thread"); err != nil {
			return err
		}
		old, _, err := tx.Turn(rows[0].TurnID)
		if err != nil {
			return err
		}
		if err = tx.BeginModelSwitch(old); err != nil {
			return err
		}
		if err = tx.FinishTurn(old.ID, store.TurnInterrupted, "model settings changed", nil); err != nil {
			return err
		}
		sw, _, err := tx.ModelSwitch(cs.id)
		if err != nil {
			return err
		}
		continued, err := tx.NewContinuation(sw, old, nil)
		if err != nil {
			return err
		}
		if err = tx.MarkStarting(continued.ID, "saved-thread"); err != nil {
			return err
		}
		if err = tx.EndModelSwitch(cs.id, old.ID, old.Attempt); err != nil {
			return err
		}
		admitted, _, err = tx.Turn(continued.ID)
		if err != nil {
			return err
		}
		_, err = r.e.cancelModelHandoff(tx, cs, admitted)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	// The cancellation above committed while cs.running was still nil.
	rs := &runState{turnID: admitted.ID, clientKey: admitted.TurnKey, done: make(chan struct{})}
	r.e.mu.Lock()
	cs.running = rs
	r.e.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r.e.execTurn(ctx, cs, admitted, TurnRequest{ConvID: cs.id, ThreadID: "saved-thread", TurnKey: admitted.TurnKey, Text: continueText}, rs)
	if len(r.ag.runLog()) != 0 {
		t.Fatal("cancelled admitted continuation reached the backend")
	}
}
