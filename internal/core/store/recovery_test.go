package store

import (
	"errors"
	"strings"
	"testing"
)

func TestRecoveryPointReadsDoNotWrite(t *testing.T) {
	s, _ := openTest(t)
	conv := newWork(t, s, "large")
	mustT(t, s.Update(func(tx *Tx) error {
		_, err := tx.exec(`WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<10000)
   INSERT INTO turns(id,conv_id,turn_key,state,queued_at) SELECT x,?, 'large-'||x,'completed',1 FROM n`, conv)
		if err != nil {
			return err
		}
		_, err = tx.exec(`INSERT INTO turn_recovery SELECT id,1,1,'turn_lookup','','completed','accepted','none','stop','','thread','native',0,0,0,0,'',0 FROM turns`)
		return err
	}))
	var before, after int64
	mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&before))
	mustT(t, s.Update(func(tx *Tx) error {
		for i := 0; i < 50; i++ {
			got, ok, err := tx.Recovery(9999)
			if err != nil {
				return err
			}
			if !ok || got.TurnID != 9999 {
				t.Fatalf("point read: %+v", got)
			}
		}
		return nil
	}))
	mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&after))
	if after != before {
		t.Fatalf("read wrote %d rows", after-before)
	}
	var id, parent, unused int
	var plan string
	mustT(t, s.r.QueryRow(`EXPLAIN QUERY PLAN SELECT * FROM turn_recovery WHERE turn_id=?`, 9999).Scan(&id, &parent, &unused, &plan))
	if !strings.Contains(plan, "INTEGER PRIMARY KEY") {
		t.Fatalf("recovery scans history: %s", plan)
	}
}

func TestRecoveryEvidenceBoundedAtomicAndDurable(t *testing.T) {
	s, c := openTest(t)
	conv := newWork(t, s, "a")
	var id int64
	mustT(t, s.Update(func(tx *Tx) error {
		rows, err := tx.AdmitWindow("work", conv, []InboundMsg{prompt("p", 1)}, "cursor")
		if err != nil {
			return err
		}
		id = rows[0].TurnID
		return nil
	}))
	r := RecoveryEvidence{TurnID: id, Source: "turn_lookup", Observation: "not_found", Acceptance: "unknown", Decision: "stop", RetryOwner: "unknown"}
	for i := 0; i < 50; i++ {
		c.add(1)
		mustT(t, s.Update(func(tx *Tx) error { return tx.RecordRecovery(r) }))
	}
	if n := countWhere(t, s, `SELECT count(*) FROM turn_recovery`); n != 1 {
		t.Fatalf("rows=%d", n)
	}
	boom := errors.New("rollback")
	wantErr(t, s.Update(func(tx *Tx) error {
		r.Decision = "retry"
		if err := tx.RecordRecovery(r); err != nil {
			return err
		}
		return boom
	}), boom)
	// A read must not write; the failed decision must not survive a reopen.
	mustT(t, s.Release())
	mustT(t, s.Close())
	reopened := openAt(t, s.Path(), c)
	var got RecoveryEvidence
	mustT(t, reopened.r.QueryRow(`SELECT decision,acceptance,observed_at FROM turn_recovery WHERE turn_id=?`, id).Scan(&got.Decision, &got.Acceptance, &got.ObservedAt))
	if got.Decision != "stop" || got.Acceptance != "unknown" || got.ObservedAt != c.now().UnixMilli() {
		t.Fatalf("evidence=%+v", got)
	}
}

func TestRecoveryBudgetAndCancelSurviveProbeAndReopen(t *testing.T) {
	s, c := openTest(t)
	conv := newWork(t, s, "recovery")
	var id int64
	mustT(t, s.Update(func(tx *Tx) error {
		rows, err := tx.AdmitWindow("work", conv, []InboundMsg{prompt("p", 1)}, "cursor")
		if err != nil {
			return err
		}
		id = rows[0].TurnID
		if err := tx.RecordRecovery(RecoveryEvidence{TurnID: id, Source: "executor_stopped", Decision: "retry"}); err != nil {
			return err
		}
		if err := tx.PlanRecoveryContinuation(id, 3, 12345, "saved recovery instruction"); err != nil {
			return err
		}
		if err := tx.RequestRecoveryCancel(id); err != nil {
			return err
		}
		return tx.RecordRecovery(RecoveryEvidence{TurnID: id, Source: "turn_lookup", Decision: "probe"})
	}))
	mustT(t, s.Release())
	mustT(t, s.Close())
	reopened := openAt(t, s.Path(), c)
	_, err := reopened.Acquire(testIdent(101))
	mustT(t, err)
	mustT(t, reopened.Update(func(tx *Tx) error {
		got, ok, err := tx.Recovery(id)
		if err != nil {
			return err
		}
		if !ok || got.ResumeCount != 3 || got.ResumeDeadline != 12345 || got.ResumePrompt != "saved recovery instruction" || !got.CancelRequested {
			t.Fatalf("lost durable recovery state: %+v", got)
		}
		return nil
	}))
}
