package store

import "testing"

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
