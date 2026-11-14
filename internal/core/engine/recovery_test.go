package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

func TestRecoveryCancelDuringLookupIsRetained(t *testing.T) {
	r := newRig(t)
	work := r.newWork(t.TempDir())
	r.ag.mu.Lock()
	r.ag.block["cancel recovery"] = make(chan struct{})
	r.ag.mu.Unlock()
	r.ch.PostUser(work, "cancel recovery")
	var tr store.Turn
	r.wait("running", func() bool {
		rows, _ := r.st.OpenTurns()
		if len(rows) == 1 && rows[0].State == store.TurnRunning {
			tr = rows[0]
			return true
		}
		return false
	})
	r.stop()
	gate := make(chan struct{})
	r.ag.mu.Lock()
	r.ag.lookupGate = gate
	r.ag.lookup[tr.TurnKey] = TurnOutcome{Status: StatusFailed, Capacity: true, CodexTurnID: tr.CodexTurnID}
	r.ag.mu.Unlock()
	r.start()
	r.wait("reconciler owns request", func() bool {
		r.e.mu.Lock()
		defer r.e.mu.Unlock()
		cs := r.e.convs[tr.ConvID]
		return cs != nil && cs.running != nil
	})
	if !r.e.cancelRunning(context.Background(), tr.ConvID) {
		t.Fatal("cancel did not reach reconciliation")
	}
	close(gate)
	r.wait("cancel persisted", func() bool {
		var got store.Turn
		_ = r.st.Update(func(tx *store.Tx) error { got, _, _ = tx.Turn(tr.ID); return nil })
		return got.State == store.TurnCancelled
	})
	if n := r.ag.calls("cancel recovery"); n != 1 {
		t.Fatalf("cancelled request retried %d times", n)
	}
}

func TestRecoveryCodexOwnsRetryUntilTerminal(t *testing.T) {
	r := newRig(t)
	work := r.newWork(t.TempDir())
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	r.ag.mu.Lock()
	r.ag.during = func(_ TurnRequest, h TurnHooks) {
		h.Retrying("reconnecting", "responseStreamDisconnected")
		close(entered)
		<-release
	}
	r.ag.mu.Unlock()
	r.ch.PostUser(work, "native retry")
	<-entered
	var evidence store.RecoveryEvidence
	rows, err := r.st.OpenTurns()
	if err != nil || len(rows) != 1 {
		t.Fatalf("turns=%v err=%v", rows, err)
	}
	if err := r.st.Update(func(tx *store.Tx) error { var err error; evidence, _, err = tx.Recovery(rows[0].ID); return err }); err != nil {
		t.Fatal(err)
	}
	if evidence.RetryOwner != "codex" || evidence.Decision != "observe" || evidence.ErrorCode != "responseStreamDisconnected" || evidence.CodexTurnID == "" {
		t.Fatalf("evidence=%+v", evidence)
	}
	if n := r.ag.calls("native retry"); n != 1 {
		t.Fatalf("overlapping execution: %d", n)
	}
	close(release)
	r.wait("native answer", func() bool { return len(r.own(work, "ANSWER to native retry")) == 1 })
}

func TestRecoveryRestartRetainsRejectedBackoff(t *testing.T) {
	r := newRig(t, func(c *Config) { c.Timing.CapacityBackoff = 2 * time.Second; c.Timing.CapacityRetry = 10 * time.Second })
	work := r.newWork(t.TempDir())
	r.ag.mu.Lock()
	r.ag.refuse = func(req TurnRequest) (TurnOutcome, bool) {
		return TurnOutcome{NotStarted: true, Status: StatusFailed, Capacity: true, Error: "capacity"}, true
	}
	r.ag.mu.Unlock()
	r.ch.PostUser(work, "rejected capacity")
	var before store.RecoveryEvidence
	r.wait("durable retry", func() bool {
		turns, _ := r.st.OpenTurns()
		if len(turns) != 1 || turns[0].State != store.TurnQueued {
			return false
		}
		_ = r.st.Update(func(tx *store.Tx) error { before, _, _ = tx.Recovery(turns[0].ID); return nil })
		return before.Decision == "retry"
	})
	r.stop()
	r.ag.mu.Lock()
	r.ag.refuse = nil
	r.ag.mu.Unlock()
	r.start()
	time.Sleep(250 * time.Millisecond)
	if n := r.ag.calls("rejected capacity"); n != 1 {
		t.Fatalf("restart bypassed backoff: %d calls", n)
	}
	r.wait("answer after due time", func() bool { return len(r.own(work, "ANSWER to rejected capacity")) == 1 })
	if time.Now().UnixMilli() < before.NextAttemptAt {
		t.Fatal("retried before persisted due time")
	}
	if before.Acceptance != "rejected" || before.RetryDeadline == 0 || before.NextAttemptAt == 0 {
		t.Fatalf("evidence=%+v", before)
	}
}

func TestRecoveryRestartSettlesByEvidence(t *testing.T) {
	for _, status := range []string{StatusFailed, StatusCompleted, StatusInProgress, "not_found"} {
		t.Run(status, func(t *testing.T) {
			r := newRig(t)
			work := r.newWork(t.TempDir())
			r.ag.mu.Lock()
			r.ag.block["held recovery"] = make(chan struct{})
			r.ag.mu.Unlock()
			r.ch.PostUser(work, "held recovery")
			var tr store.Turn
			r.wait("accepted turn", func() bool {
				rows, _ := r.st.OpenTurns()
				if len(rows) == 1 && rows[0].State == store.TurnRunning {
					tr = rows[0]
					return true
				}
				return false
			})
			r.stop()
			r.ag.mu.Lock()
			if status != "not_found" {
				r.ag.lookup[tr.TurnKey] = TurnOutcome{CodexTurnID: tr.CodexTurnID, Status: status, Capacity: status == StatusFailed, Final: "saved recovery answer"}
			}
			r.ag.mu.Unlock()
			r.start()
			r.wait("recovery settled", func() bool { rows, _ := r.st.OpenTurns(); return len(rows) == 0 })
			if n := r.ag.calls("held recovery"); n != 1 {
				t.Fatalf("replayed accepted request: %d", n)
			}
			r.ag.mu.Lock()
			for _, req := range r.ag.runs {
				if req.ThreadID != "" && status != StatusFailed {
					t.Error("unexpected execution after restart")
				}
				if req.ThreadID != "" && status == StatusFailed && !strings.HasPrefix(req.Text, recoveryPromptPrefix) {
					t.Error("missing recovery warning")
				}
			}
			r.ag.mu.Unlock()
			var evidence store.RecoveryEvidence
			if err := r.st.Update(func(tx *store.Tx) error { var err error; evidence, _, err = tx.Recovery(tr.ID); return err }); err != nil {
				t.Fatal(err)
			}
			if status == "not_found" && evidence.Observation != "not_found" {
				t.Fatalf("lost missing-turn evidence: %+v", evidence)
			}
			if status == StatusInProgress && (evidence.Observation != StatusInProgress || evidence.RetryOwner != "codex") {
				t.Fatalf("lost active owner: %+v", evidence)
			}
			if evidence.Acceptance != "accepted" {
				t.Fatalf("evidence=%+v", evidence)
			}
			if status == StatusCompleted {
				r.wait("saved final delivered", func() bool { return len(r.own(work, "saved recovery answer")) == 1 })
			}
		})
	}
}

func TestRecoveryUnconfirmedNeverRetries(t *testing.T) {
	for _, out := range []TurnOutcome{
		{NotStarted: true, Unconfirmed: true, Capacity: true, Status: StatusFailed},
		{NotStarted: true, ActiveWriter: true, Capacity: true, Status: StatusFailed},
		{Status: StatusInProgress, Capacity: true, ReconnectExhausted: true},
	} {
		if retryableFailure(out) {
			t.Fatalf("unsafe retry: %+v", out)
		}
	}
}
