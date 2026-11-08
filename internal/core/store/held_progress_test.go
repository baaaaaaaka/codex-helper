package store

import (
	"errors"
	"testing"
)

func nativeProgressTurn(t *testing.T, s *Store, conv int64) int64 {
	t.Helper()
	var id int64
	mustT(t, s.Update(func(tx *Tx) error {
		ad, err := tx.AdmitWindow("held", conv, []InboundMsg{prompt("native", 1)}, "cursor")
		if err != nil {
			return err
		}
		id = ad[0].TurnID
		if err := tx.MarkStarting(id, "thread"); err != nil {
			return err
		}
		if err := tx.SetTranscriptProgress(id); err != nil {
			return err
		}
		return tx.MarkRunning(id, "native-one")
	}))
	return id
}

func TestHeldFinalReleasesAtomicallyAtSourcePosition(t *testing.T) {
	s, _ := openTest(t)
	conv := newWork(t, s, "held")
	turn := nativeProgressTurn(t, s, conv)
	parts := []OutMsg{out("answer:1", OutFinal), out("answer:2", OutFinal)}
	parts[0].Part, parts[0].Parts = 1, 2
	parts[1].Part, parts[1].Parts = 2, 2
	mustT(t, s.Update(func(tx *Tx) error { return tx.FinishTurn(turn, TurnCompleted, "", parts) }))
	var held []Outbox
	mustT(t, s.Update(func(tx *Tx) error {
		var err error
		held, err = tx.PendingOutbox(conv)
		if err != nil {
			return err
		}
		if len(held) != 2 || held[0].HoldTurn != turn || held[1].HoldTurn != turn {
			t.Fatalf("final not durably held: %+v", held)
		}
		heads, err := tx.NextSendable(tx.Now())
		if err != nil || len(heads) != 0 {
			t.Fatalf("held final became sendable: %+v, %v", heads, err)
		}
		if _, err := tx.BeginSend(held[0].ID); err == nil {
			t.Fatal("direct send bypassed hold")
		}
		return nil
	}))
	if yes, err := s.PendingAtLeast(conv, 1); err != nil || yes {
		t.Fatalf("held final blocked its own transcript reader: %v %v", yes, err)
	}
	commitSource := func(tx *Tx) error {
		if _, err := tx.Enqueue(conv, []OutMsg{out("preceding-progress", OutProgress)}); err != nil {
			return err
		}
		if err := tx.CompleteTranscriptProgress(turn, "native-one"); err != nil {
			return err
		}
		_, err := tx.Enqueue(conv, []OutMsg{out("following-local-turn", OutMirror)})
		return err
	}
	crash := errors.New("before source cursor commit")
	wantErr(t, s.Update(func(tx *Tx) error {
		if err := commitSource(tx); err != nil {
			return err
		}
		return crash
	}), crash)
	if done, err := s.TranscriptProgressDone(turn); err != nil || done {
		t.Fatalf("rolled-back terminal committed: %v %v", done, err)
	}
	mustT(t, s.Update(commitSource))
	mustT(t, s.Update(func(tx *Tx) error {
		rows, err := tx.PendingOutbox(conv)
		if err != nil {
			return err
		}
		keys := []string{"preceding-progress", "answer:1", "answer:2", "following-local-turn"}
		if len(rows) != len(keys) {
			t.Fatalf("release lost content: %+v", rows)
		}
		for i, key := range keys {
			if rows[i].DeliveryKey != key || rows[i].HoldTurn != 0 {
				t.Fatalf("wrong source order: %+v", rows)
			}
		}
		for i, original := range held {
			if rows[i+1].ID != original.ID || rows[i+1].Body != original.Body || rows[i+1].Part != original.Part {
				t.Fatal("release changed delivery identity or payload")
			}
		}
		return nil
	}))
}

func TestNativeTerminalWaitsForDurableOutcome(t *testing.T) {
	s, _ := openTest(t)
	conv := newWork(t, s, "hold-retry")
	turn := nativeProgressTurn(t, s, conv)
	parts := []OutMsg{out("answer", OutFinal)}
	if err := s.Update(func(tx *Tx) error { return tx.CompleteTranscriptProgress(turn, "native-one") }); err == nil {
		t.Fatal("source terminal allowed a later turn to overtake an unstored final")
	}
	rollback := errors.New("local write failure")
	wantErr(t, s.Update(func(tx *Tx) error {
		if err := tx.FinishTurn(turn, TurnCompleted, "", parts); err != nil {
			return err
		}
		return rollback
	}), rollback)
	mustT(t, s.Update(func(tx *Tx) error {
		if err := tx.FinishTurn(turn, TurnCompleted, "", parts); err != nil {
			return err
		}
		if err := tx.CompleteTranscriptProgress(turn, "native-one"); err != nil {
			return err
		}
		heads, err := tx.NextSendable(tx.Now())
		if err != nil || len(heads) != 1 || heads[0].HoldTurn != 0 {
			t.Fatalf("finish retry stuck behind old hold: %+v %v", heads, err)
		}
		return nil
	}))
}

func TestCapacityAttemptTerminalCannotReleaseSuccessor(t *testing.T) {
	s, _ := openTest(t)
	conv := newWork(t, s, "capacity")
	turn := nativeProgressTurn(t, s, conv)
	mustT(t, s.Update(func(tx *Tx) error {
		if err := tx.RequeueTurn(turn, tx.Now()+1000, "thread", "native-one"); err != nil {
			return err
		}
		return tx.MarkStarting(turn, "thread")
	}))
	if err := s.Update(func(tx *Tx) error { return tx.CompleteTranscriptProgress(turn, "native-one") }); err == nil {
		t.Fatal("source advanced past native acceptance race")
	}
	mustT(t, s.Update(func(tx *Tx) error {
		if err := tx.MarkRunning(turn, "native-two"); err != nil {
			return err
		}
		if err := tx.CompleteTranscriptProgress(turn, "native-one"); err != nil {
			return err
		}
		return tx.FinishTurn(turn, TurnCompleted, "", []OutMsg{out("successor-final", OutFinal)})
	}))
	if done, err := s.TranscriptProgressDone(turn); err != nil || done {
		t.Fatalf("earlier attempt completed successor: %v %v", done, err)
	}
	mustT(t, s.Update(func(tx *Tx) error { return tx.CompleteTranscriptProgress(turn, "native-two") }))
	if done, err := s.TranscriptProgressDone(turn); err != nil || !done {
		t.Fatalf("successor terminal not committed: %v %v", done, err)
	}
}
