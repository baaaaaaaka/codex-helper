package store

import (
	"testing"
)

func TestSteerSelectionPreservesExplicitQueueAndPendingFiles(t *testing.T) {
	s, _ := openTest(t)
	id := newWork(t, s, "work")
	var admitted []Admitted
	mustT(t, s.Update(func(tx *Tx) (err error) {
		admitted, err = tx.AdmitWindow("test", id, []InboundMsg{
			{ExtMsgID: "1", ExtTime: 1, Sender: SenderOwner, Kind: InPrompt, Body: "first"},
			{ExtMsgID: "2", ExtTime: 2, Sender: SenderOwner, Kind: InPrompt, Body: "queued", Queue: true},
			{ExtMsgID: "3", ExtTime: 3, Sender: SenderOwner, Kind: InPrompt, Body: "image", Files: []InboundFile{{Name: "image", Kind: "image", ExtRef: "ref", State: "pending"}}},
			{ExtMsgID: "4", ExtTime: 4, Sender: SenderOwner, Kind: InPrompt, Body: "last"},
		}, "cursor")
		return
	}))
	mustT(t, s.Update(func(tx *Tx) error {
		if err := tx.MarkStarting(admitted[0].TurnID, "thread"); err != nil {
			return err
		}
		if _, ok, err := tx.NextSteer(id); err != nil || ok {
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
			_, ok, err := tx.NextSteer(id)
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
