package engine

import (
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

func TestRecoveryCommittedCancelSurvivesBeforeInterrupt(t *testing.T) {
	r := newRig(t)
	r.ag.mu.Lock()
	r.ag.recoveryStopped = true
	r.ag.block["cancel before interrupt"] = make(chan struct{})
	r.ag.mu.Unlock()
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "cancel before interrupt")
	var tr store.Turn
	r.wait("accepted", func() bool {
		rows, _ := r.st.OpenTurns()
		if len(rows) == 1 && rows[0].State == store.TurnRunning {
			tr = rows[0]
			return true
		}
		return false
	})
	// Commit the command's cancellation, then crash before native interrupt.
	if err := r.e.update(func(tx *store.Tx) error {
		_, err := r.e.cancelModelHandoff(tx, r.e.conv(tr.ConvID), tr)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	r.stop()
	r.ag.mu.Lock()
	r.ag.lookup[tr.TurnKey] = TurnOutcome{Status: StatusInProgress, CodexTurnID: tr.CodexTurnID}
	r.ag.mu.Unlock()
	r.start()
	r.wait("cancelled after restart", func() bool {
		var got store.Turn
		_ = r.st.Read(func(tx *store.Tx) error { got, _, _ = tx.Turn(tr.ID); return nil })
		return got.State == store.TurnCancelled
	})
	if len(r.ag.texts()) != 1 {
		t.Fatal("committed cancellation was lost before native interruption")
	}
}

// The managed executor is gone after a helper/OOM interruption, while its
// durable native turn still says inProgress. Resume in that exact thread.
func TestRecoveryStoppedExecutorContinuesWithWarning(t *testing.T) {
	r := newRig(t, func(c *Config) { c.Timing.CapacityBackoff = 500 * time.Millisecond })
	work := r.newWork(t.TempDir())
	r.ag.mu.Lock()
	r.ag.recoveryStopped = true
	r.ag.block["original OOM experiment"] = make(chan struct{})
	r.ag.mu.Unlock()
	r.ch.PostUser(work, "original OOM experiment")
	var tr store.Turn
	r.wait("original accepted", func() bool {
		rows, _ := r.st.OpenTurns()
		if len(rows) == 1 && rows[0].State == store.TurnRunning {
			tr = rows[0]
			return true
		}
		return false
	})
	r.stop()
	r.ag.mu.Lock()
	r.ag.lookup[tr.TurnKey] = TurnOutcome{Status: StatusInProgress, CodexTurnID: tr.CodexTurnID}
	r.ag.mu.Unlock()
	r.start()
	r.wait("recovery request", func() bool { return len(r.ag.texts()) == 2 })
	r.ag.mu.Lock()
	runs := append([]TurnRequest(nil), r.ag.runs...)
	r.ag.mu.Unlock()
	recovered := runs[1]
	for _, part := range []string{recoveryPromptPrefix, "underlying cause is unconfirmed", "memory/OOM", "local or remote jobs", "verify uncertain operation results"} {
		if !strings.Contains(recovered.Text, part) {
			t.Errorf("missing %q in %q", part, recovered.Text)
		}
	}
	if recovered.ThreadID != tr.ThreadID || recovered.TurnKey != tr.TurnKey || strings.Contains(recovered.Text, "original OOM experiment") || len(recovered.Images) != 0 {
		t.Fatalf("replayed original instead of continuing: %+v", recovered)
	}
	r.wait("final settled", func() bool { rows, _ := r.st.OpenTurns(); return len(rows) == 0 })
}

func TestRecoveryInterruptionBudgetStopsRepeatedCrashes(t *testing.T) {
	r := newRig(t, func(c *Config) { c.Timing.CapacityBackoff = 10 * time.Millisecond })
	r.ag.mu.Lock()
	r.ag.recoveryStopped = true
	r.ag.outcome = func(req TurnRequest) (TurnOutcome, error) {
		return TurnOutcome{Status: StatusInterrupted, ResumeAfterInterruption: true, Error: "process exited: signal: killed; OOM not confirmed"}, nil
	}
	r.ag.mu.Unlock()
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "crashing experiment")
	r.wait("bounded interruption notice", func() bool { return len(r.own(work, "Codex request interrupted")) == 1 })
	if got := len(r.ag.texts()); got != maxInterruptionResumes+1 {
		t.Fatalf("attempts=%d want=%d", got, maxInterruptionResumes+1)
	}
	var snapshot store.RecoveryEvidence
	if err := r.st.Update(func(tx *store.Tx) error { var err error; snapshot, _, err = tx.Recovery(1); return err }); err != nil {
		t.Fatal(err)
	}
	if snapshot.ResumeCount != maxInterruptionResumes || snapshot.ResumeDeadline == 0 {
		t.Fatalf("budget=%+v", snapshot)
	}
	if snapshot.RetryDeadline == 0 || snapshot.RetryDeadline >= snapshot.ResumeDeadline {
		t.Fatalf("interruption budget replaced the capacity retry window: %+v", snapshot)
	}
	before := len(r.ag.texts())
	r.stop()
	r.start()
	r.wait("restart ready", func() bool { return r.e.conv(1) != nil })
	if len(r.ag.texts()) != before {
		t.Fatal("restart reset exhausted budget")
	}
}

func TestRecoveryQueuedContinuationHonorsCancelAndThreadChange(t *testing.T) {
	for _, mode := range []string{"cancel", "thread_change"} {
		t.Run(mode, func(t *testing.T) {
			r := newRig(t, func(c *Config) { c.Timing.CapacityBackoff = 1500 * time.Millisecond })
			r.ag.mu.Lock()
			r.ag.recoveryStopped = true
			r.ag.outcome = func(req TurnRequest) (TurnOutcome, error) {
				return TurnOutcome{Status: StatusInterrupted, ResumeAfterInterruption: true, Error: "executor exited"}, nil
			}
			r.ag.mu.Unlock()
			work := r.newWork(t.TempDir())
			r.ch.PostUser(work, "interrupted work")
			var tr store.Turn
			r.wait("queued continuation", func() bool {
				rows, _ := r.st.OpenTurns()
				if len(rows) == 1 && rows[0].State == store.TurnQueued && rows[0].RetryDeadline != 0 {
					tr = rows[0]
					return true
				}
				return false
			})
			if err := r.st.Update(func(tx *store.Tx) error {
				if mode == "cancel" {
					return tx.RequestRecoveryCancel(tr.ID)
				}
				binding, _, err := tx.Thread(tr.ConvID)
				if err != nil {
					return err
				}
				binding.ThreadID = "different-thread"
				return tx.Bind(binding)
			}); err != nil {
				t.Fatal(err)
			}
			r.wait("stopped without executing", func() bool { rows, _ := r.st.OpenTurns(); return len(rows) == 0 })
			if got := len(r.ag.texts()); got != 1 {
				t.Fatalf("unexpected retry after %s: %d", mode, got)
			}
		})
	}
}
