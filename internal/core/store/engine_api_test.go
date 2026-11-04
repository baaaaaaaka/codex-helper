package store

import (
	"fmt"
	"strings"
	"testing"
)

func TestEngineAdditions(t *testing.T) {
	s, _ := openTest(t)
	var conv, in, turn int64
	mustT(t, s.Update(func(tx *Tx) error {
		var err error
		if conv, err = tx.CreateConversation(NewConversation{Label: "s001", Kind: KindWork, Channel: "teams", ExtChatID: "c1"}); err != nil {
			return err
		}
		ad, err := tx.AdmitWindow("c1", conv, []InboundMsg{{ExtMsgID: "m1", ExtTime: 1, Sender: SenderOwner, Kind: InPrompt, Body: "hi"}}, "cur")
		if err != nil {
			return err
		}
		in, turn = ad[0].InboundID, ad[0].TurnID
		if err := tx.FinishTurn(turn, TurnCancelled, "duplicate", []OutMsg{
			{DeliveryKey: "mirror:th:msg_1:1", Kind: OutMirror, Body: "x", BodyHash: []byte{1, 2}},
			{DeliveryKey: "mirror:th:msg_10:1", Kind: OutMirror, Body: "y"},
		}); err != nil {
			return err
		}
		return nil
	}))
	mustT(t, s.Update(func(tx *Tx) error {
		for prefix, want := range map[string]bool{"mirror:th:msg_1:": true, "mirror:th:msg_10:": true, "mirror:th:msg_2:": false, "mirror:th:msg_1": true} {
			got, err := tx.Delivered(prefix)
			if err != nil || got != want {
				t.Errorf("Delivered(%q) = %v %v, want %v", prefix, got, err, want)
			}
		}
		rows, err := tx.PendingOutbox(conv)
		if err != nil || len(rows) != 2 {
			t.Fatalf("pending %d %v", len(rows), err)
		}
		tok, err := tx.BeginSend(rows[0].ID)
		if err != nil {
			return err
		}
		if o, ok, err := tx.OutboxByToken(conv, tok); err != nil || !ok || o.ID != rows[0].ID {
			t.Errorf("OutboxByToken %v %v %v", o.ID, ok, err)
		}
		if hs, err := tx.OutboxByHash(conv, []byte{1, 2}, 0); err != nil || len(hs) != 1 {
			t.Errorf("OutboxByHash %d %v", len(hs), err)
		}
		if lo, ok, err := tx.LastOutbox(conv, OutMirror); err != nil || !ok || lo.DeliveryKey != "mirror:th:msg_10:1" {
			t.Errorf("LastOutbox %q %v %v", lo.DeliveryKey, ok, err)
		}
		id, err := tx.RetryTurn(conv, in)
		if err != nil {
			return err
		}
		tr, _, _ := tx.Turn(id)
		if tr.State != TurnQueued || tr.InboundID != in || id == turn {
			t.Errorf("retry turn %+v", tr)
		}
		recent, err := tx.RecentTurns(conv, 5)
		if err != nil || len(recent) != 2 || recent[0].ID != id {
			t.Errorf("RecentTurns %v %v", recent, err)
		}
		return nil
	}))
	counts, err := s.TurnCounts()
	if err != nil || counts[TurnQueued] != 1 || counts[TurnCancelled] != 1 {
		t.Errorf("TurnCounts %v %v", counts, err)
	}
	if oc, err := s.OutboxCounts(); err != nil || oc[OutSending] != 1 || oc[OutQueued] != 1 || len(oc) != 2 {
		t.Errorf("OutboxCounts %v %v", oc, err)
	}
	if ids, err := s.CodexTurnIDs(); err != nil || len(ids) != 0 {
		t.Errorf("CodexTurnIDs %v %v", ids, err)
	}
}

func TestQueuedTurnConvs(t *testing.T) {
	s, _ := openTest(t)
	var a, b, c int64
	mustT(t, s.Update(func(tx *Tx) error {
		var err error
		for i, p := range []*int64{&a, &b, &c} {
			chat := fmt.Sprintf("c%d", i)
			if *p, err = tx.CreateConversation(NewConversation{Label: fmt.Sprintf("s%03d", i), Kind: KindWork, Channel: "teams", ExtChatID: chat}); err != nil {
				return err
			}
		}
		// a: one queued turn; b: a turn that finished; c: none.
		if _, err := tx.AdmitWindow("c0", a, []InboundMsg{{ExtMsgID: "m1", ExtTime: 1, Sender: SenderOwner, Kind: InPrompt, Body: "hi"}}, "cur"); err != nil {
			return err
		}
		ad, err := tx.AdmitWindow("c1", b, []InboundMsg{{ExtMsgID: "m2", ExtTime: 1, Sender: SenderOwner, Kind: InPrompt, Body: "hi"}}, "cur")
		if err != nil {
			return err
		}
		return tx.FinishTurn(ad[0].TurnID, TurnCancelled, "test", nil)
	}))
	got, err := s.QueuedTurnConvs()
	if err != nil || len(got) != 1 || got[0] != a {
		t.Fatalf("QueuedTurnConvs = %v %v, want [%d]", got, err, a)
	}
	rows, err := s.r.Query(`EXPLAIN QUERY PLAN SELECT DISTINCT conv_id FROM turns
WHERE state IN ('queued','starting','running') AND state = 'queued' ORDER BY conv_id`)
	mustT(t, err)
	defer rows.Close()
	var plan string
	for rows.Next() {
		var detail string
		mustT(t, rows.Scan(new(int), new(int), new(int), &detail))
		plan += detail + "; "
	}
	mustT(t, rows.Err())
	if !strings.Contains(plan, "turns_open") {
		t.Errorf("query plan %q does not use turns_open", plan)
	}
}

func TestQueuedKinds(t *testing.T) {
	s, _ := openTest(t)
	var a, b int64
	mustT(t, s.Update(func(tx *Tx) error {
		var err error
		if a, err = tx.CreateConversation(NewConversation{Label: "s001", Kind: KindWork, Channel: "teams", ExtChatID: "c1"}); err != nil {
			return err
		}
		if b, err = tx.CreateConversation(NewConversation{Label: "s002", Kind: KindWork, Channel: "teams", ExtChatID: "c2"}); err != nil {
			return err
		}
		if _, err := tx.Enqueue(a, []OutMsg{out("p1", OutProgress), out("f1", OutFinal), out("p2", OutProgress)}); err != nil {
			return err
		}
		_, err = tx.Enqueue(b, []OutMsg{out("m1", OutMirror)})
		return err
	}))
	var got map[int64][]string
	mustT(t, s.Update(func(tx *Tx) (err error) { got, err = tx.QueuedKinds(); return }))
	if len(got) != 2 || strings.Join(got[a], ",") != OutFinal+","+OutProgress || strings.Join(got[b], ",") != OutMirror {
		t.Fatalf("QueuedKinds = %v", got)
	}
	// A row being sent is not queued.
	mustT(t, s.Update(func(tx *Tx) error {
		rows, err := tx.PendingOutbox(b)
		if err != nil {
			return err
		}
		_, err = tx.BeginSend(rows[0].ID)
		return err
	}))
	mustT(t, s.Update(func(tx *Tx) (err error) { got, err = tx.QueuedKinds(); return }))
	if _, ok := got[b]; ok || len(got) != 1 {
		t.Fatalf("QueuedKinds after a send began = %v", got)
	}
}
