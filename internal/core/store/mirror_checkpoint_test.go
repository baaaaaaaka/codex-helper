package store

import (
	"errors"
	"testing"
	"time"
)

func TestMirrorRecoveryUpgradePreservesUncertainBatches(t *testing.T) {
	s, clock := openTest(t)
	conv := newWork(t, s, "legacy")
	var before []Outbox
	mustT(t, s.Update(func(tx *Tx) error {
		var msgs []OutMsg
		for _, key := range []string{"queued", "frozen-head", "frozen-member", "unknown-head", "unknown-member", "sending", "sent", "live"} {
			m := out(key, OutMirror)
			if key != "live" {
				m.ProgressGroup = RecoveryGroupPrefix + key
			}
			msgs = append(msgs, m)
		}
		msgs[2].ProgressGroup, msgs[4].ProgressGroup = msgs[1].ProgressGroup, msgs[3].ProgressGroup
		if _, err := tx.Enqueue(conv, msgs); err != nil {
			return err
		}
		rows, err := tx.PendingOutbox(conv)
		if err != nil {
			return err
		}
		for _, i := range []int{1, 3} {
			if err := tx.FreezeProgressBatch(rows[i:i+2], "exact frozen body", []byte{1, 2, 3}); err != nil {
				return err
			}
		}
		// Model all persisted states independently of which row would be
		// eligible to send in this test conversation.
		for _, i := range []int{3, 4, 5, 6} {
			state := OutUnknown
			if i == 5 {
				state = OutSending
			} else if i == 6 {
				state = OutSent
			}
			if _, err := tx.exec("UPDATE outbox SET state=?,attempt_token='legacy-token',note='previous status' WHERE id=?", state, rows[i].ID); err != nil {
				return err
			}
		}
		if _, err := tx.exec("UPDATE outbox SET ext_msg_id='already-published' WHERE id=?", rows[6].ID); err != nil {
			return err
		}
		for _, row := range rows {
			current, _, err := tx.Outbox(row.ID)
			if err != nil {
				return err
			}
			before = append(before, current)
		}
		return nil
	}))
	removeMirrorCheckpointSchema(t, s)
	_, err := s.w.Exec("PRAGMA user_version=13")
	mustT(t, err)
	path := s.path
	mustT(t, s.Release())
	mustT(t, s.Close())
	s = openAt(t, path, clock)
	_, err = s.Acquire(testIdent(100))
	mustT(t, err)

	check := func(tx *Tx, i int, wantState string) error {
		t.Helper()
		old := before[i]
		got, _, err := tx.Outbox(old.ID)
		if err != nil {
			return err
		}
		if got.State != wantState || got.DeliveryKey != old.DeliveryKey || got.Body != old.Body ||
			got.Seq != old.Seq || got.BatchHead != old.BatchHead || got.AttemptToken != old.AttemptToken ||
			got.ExtMsgID != old.ExtMsgID || got.Note != old.Note {
			t.Fatalf("%q migrated as %+v, state %q and previous delivery evidence required: %+v", old.DeliveryKey, got, wantState, old)
		}
		return nil
	}
	mustT(t, s.Update(func(tx *Tx) error {
		want := []string{OutDropped, OutDropped, OutDropped, OutUnknown, OutUnknown, OutSending, OutSent, OutQueued}
		for i, state := range want {
			if err := check(tx, i, state); err != nil {
				return err
			}
		}
		if err := tx.Requeue(before[3].ID, 0); err != nil { // authoritative absence
			return err
		}
		for _, i := range []int{3, 4} {
			if err := check(tx, i, OutDropped); err != nil {
				return err
			}
		}
		n, err := tx.MirrorRecoveryReviewCount(conv)
		if n != 5 || err != nil {
			t.Fatalf("review count %d, err %v", n, err)
		}
		if err := tx.ApproveMirrorRecovery(conv); err != nil {
			return err
		}
		for i := range 5 {
			if err := check(tx, i, OutQueued); err != nil {
				return err
			}
		}
		return check(tx, 5, OutSending)
	}))
}

func TestFinishProgressWithGapKeepsOutputAndRequiresDurableOutcome(t *testing.T) {
	s, clock := openTest(t)
	conv := newWork(t, s, "gap")
	var id int64
	mustT(t, s.Update(func(tx *Tx) error {
		var err error
		id, err = tx.newTurn(conv, 0)
		if err != nil {
			return err
		}
		if err := tx.MarkStarting(id, "th-gap"); err != nil {
			return err
		}
		if err := tx.SetTranscriptProgress(id); err != nil {
			return err
		}
		if err := tx.MarkRunning(id, "accepted"); err != nil {
			return err
		}
		return tx.FinishProgressWithGap(id, "accepted") // still running
	}))
	done, err := s.TranscriptProgressDone(id)
	mustT(t, err)
	if done {
		t.Fatal("released a running turn")
	}
	mustT(t, s.Update(func(tx *Tx) error {
		return tx.FinishTurn(id, TurnCompleted, "", []OutMsg{out("original-final", OutFinal)})
	}))
	clock.add(10 * time.Second)
	held, err := s.BlockedProgressTurns(conv, s.ms()-10_000)
	mustT(t, err)
	if len(held) != 1 || held[0].ID != id {
		t.Fatalf("held outcome = %+v", held)
	}
	abort := errors.New("abort release transaction")
	wantErr(t, s.Update(func(tx *Tx) error {
		if err := tx.FinishProgressWithGap(id, "accepted"); err != nil {
			return err
		}
		return abort
	}), abort)
	mustT(t, s.Update(func(tx *Tx) error {
		if err := tx.FinishProgressWithGap(id, "previous-attempt"); err != nil {
			return err
		}
		head, err := tx.NextSendable(tx.Now())
		if err != nil || len(head) != 0 {
			t.Fatalf("wrong attempt or rollback made outcome sendable: %+v, %v", head, err)
		}
		for range 2 {
			if err := tx.FinishProgressWithGap(id, "accepted"); err != nil {
				return err
			}
		}
		head, err = tx.NextSendable(tx.Now())
		if err != nil || len(head) != 1 || head[0].DeliveryKey != "original-final" || head[0].HoldTurn != 0 {
			t.Fatalf("released outcome = %+v, %v", head, err)
		}
		gap, err := tx.TranscriptProgressGap(id)
		if err != nil || !gap {
			t.Fatalf("persisted gap = %t, %v", gap, err)
		}
		return nil
	}))
}

// removeMirrorCheckpointSchema restores the schema objects present before v14.
// Migration tests which start further back can then remove their own objects.
func removeMirrorCheckpointSchema(t *testing.T, s *Store) {
	t.Helper()
	for _, ddl := range []string{
		"DROP TABLE mirror_checkpoints", "DROP INDEX outbox_held_turn",
		"ALTER TABLE turns DROP COLUMN transcript_gap", "ALTER TABLE outbox DROP COLUMN recovery_review",
	} {
		_, err := s.w.Exec(ddl)
		mustT(t, err)
	}
}
