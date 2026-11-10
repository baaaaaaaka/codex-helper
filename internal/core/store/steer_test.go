package store

import (
	"testing"
	"time"
)

func TestSteerSelectionPreservesExplicitQueueAndPendingFiles(t *testing.T) {
	s, _ := openTest(t)
	id := newWork(t, s, "work")
	var admitted []Admitted
	mustT(t, s.Update(func(tx *Tx) (err error) {
		admitted, err = tx.AdmitWindow("test", id, []InboundMsg{
			{ExtMsgID: "1", ExtTime: 1, Sender: SenderOwner, Kind: InPrompt, Body: "first"},
			{ExtMsgID: "2", ExtTime: 2, Sender: SenderOwner, Kind: InPrompt, Body: "queued", Queue: true},
			{ExtMsgID: "3", ExtTime: 3, Sender: SenderOwner, Kind: InPrompt, Body: "image", Files: []InboundFile{{Name: "image", Kind: "image", Ref: FileReference{Ref: "ref"}, State: "pending"}}},
			{ExtMsgID: "4", ExtTime: 4, Sender: SenderOwner, Kind: InPrompt, Body: "last"},
		}, "cursor")
		return
	}))
	mustT(t, s.Update(func(tx *Tx) error {
		if err := tx.MarkStarting(admitted[0].TurnID, "thread"); err != nil {
			return err
		}
		if _, ok, err := tx.NextSteer(id, 0); err != nil || ok {
			t.Fatalf("pending ordinary input must hold later steer: %v %v", ok, err)
		}
		return nil
	}))
}

func TestSteerSelectionReadsDoNotWrite(t *testing.T) {
	s, _ := openTest(t)
	id := newWork(t, s, "work")
	var before, after int64
	mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&before))
	for i := 0; i < 50; i++ {
		mustT(t, s.Update(func(tx *Tx) error {
			_, ok, err := tx.NextSteer(id, 0)
			if ok {
				t.Fatal("unexpected input")
			}
			return err
		}))
	}
	mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&after))
	if after != before {
		t.Fatalf("unchanged selection wrote %d rows", after-before)
	}
}

func TestSteerSelectionKeepsBacklogBehindActiveTurn(t *testing.T) {
	s, c := openTest(t)
	id := newWork(t, s, "work")
	var admitted []Admitted
	mustT(t, s.Update(func(tx *Tx) (err error) {
		admitted, err = tx.AdmitWindow("test", id, []InboundMsg{
			{ExtMsgID: "first", ExtTime: 1, Sender: SenderOwner, Kind: InPrompt, Body: "first"},
			{ExtMsgID: "backlog", ExtTime: 2, Sender: SenderOwner, Kind: InPrompt, Body: "arrived while stopped"},
		}, "cursor-1")
		return err
	}))
	if len(admitted) != 2 {
		t.Fatalf("admitted %d prompts, want 2", len(admitted))
	}
	mustT(t, s.Update(func(tx *Tx) error { return tx.MarkStarting(admitted[0].TurnID, "thread") }))

	var got bool
	mustT(t, s.Update(func(tx *Tx) error {
		_, ok, err := tx.NextSteer(id, admitted[0].TurnID)
		got = ok
		return err
	}))
	if got {
		t.Fatal("a prompt queued before the active turn started was steered into it")
	}

	c.add(time.Millisecond)
	var live []Admitted
	mustT(t, s.Update(func(tx *Tx) (err error) {
		live, err = tx.AdmitWindow("test", id, []InboundMsg{{ExtMsgID: "live", ExtTime: 3, Sender: SenderOwner, Kind: InPrompt, Body: "arrived during the turn"}}, "cursor-2")
		return err
	}))
	if len(live) != 1 {
		t.Fatalf("admitted %d live prompts, want 1", len(live))
	}
	mustT(t, s.Update(func(tx *Tx) error {
		tr, ok, err := tx.NextSteer(id, admitted[0].TurnID)
		if err != nil {
			return err
		}
		if ok || tr.ID != admitted[1].TurnID {
			t.Fatalf("steer candidate = turn %d, ok=%v; want older backlog turn %d held ahead of live input", tr.ID, ok, admitted[1].TurnID)
		}
		return nil
	}))
}

func TestSteerSelectionAllowsInputReceivedAfterActiveTurnStarted(t *testing.T) {
	s, c := openTest(t)
	id := newWork(t, s, "work")
	var first []Admitted
	mustT(t, s.Update(func(tx *Tx) (err error) {
		first, err = tx.AdmitWindow("test", id, []InboundMsg{{ExtMsgID: "first", ExtTime: 1, Sender: SenderOwner, Kind: InPrompt, Body: "first"}}, "cursor-1")
		return err
	}))
	mustT(t, s.Update(func(tx *Tx) error { return tx.MarkStarting(first[0].TurnID, "thread") }))
	c.add(time.Millisecond)
	var live []Admitted
	mustT(t, s.Update(func(tx *Tx) (err error) {
		live, err = tx.AdmitWindow("test", id, []InboundMsg{{ExtMsgID: "live", ExtTime: 2, Sender: SenderOwner, Kind: InPrompt, Body: "live"}}, "cursor-2")
		return err
	}))
	if len(live) != 1 {
		t.Fatalf("admitted %d live prompts, want 1", len(live))
	}
	mustT(t, s.Update(func(tx *Tx) error {
		tr, ok, err := tx.NextSteer(id, first[0].TurnID)
		if err != nil {
			return err
		}
		if !ok || tr.ID != live[0].TurnID {
			t.Fatalf("steer candidate = turn %d, ok=%v; want live turn %d", tr.ID, ok, live[0].TurnID)
		}
		return nil
	}))
}
