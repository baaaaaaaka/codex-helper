package store

import (
	"errors"
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
	if open, err := s.OpenTurnCounts(); err != nil || open[TurnQueued] != 1 || len(open) != 1 {
		t.Errorf("OpenTurnCounts %v %v", open, err)
	}
	// The status publisher runs it after commits: it must not scan the
	// whole turns table.
	var id, parent, unused int
	var plan string
	if err := s.r.QueryRow(`EXPLAIN QUERY PLAN SELECT state, COUNT(*) FROM turns WHERE state IN ('queued','starting','running') GROUP BY state`).Scan(&id, &parent, &unused, &plan); err != nil || !strings.Contains(plan, "turns_open") {
		t.Errorf("OpenTurnCounts plan %q %v", plan, err)
	}
	if oc, err := s.OutboxCounts(); err != nil || oc[OutSending] != 1 || oc[OutQueued] != 1 || len(oc) != 2 {
		t.Errorf("OutboxCounts %v %v", oc, err)
	}
	if ids, err := s.CodexTurnIDs(); err != nil || len(ids) != 0 {
		t.Errorf("CodexTurnIDs %v %v", ids, err)
	}
}

func TestLatestQuestionMarkerSkipsUserMirrorsAndHelperNotices(t *testing.T) {
	s, _ := openTest(t)
	var conv int64
	mustT(t, s.Update(func(tx *Tx) error {
		var err error
		conv, err = tx.CreateConversation(NewConversation{Label: "s001", Kind: KindWork, Channel: "teams", ExtChatID: "chat"})
		if err != nil {
			return err
		}
		_, err = tx.Enqueue(conv, []OutMsg{
			{DeliveryKey: "final:1", Kind: OutFinal, Body: "agent answer"},
			{DeliveryKey: "mirror:user", Kind: OutMirror, Body: `<strong>🧑‍💻 User:</strong> user prompt`},
			{DeliveryKey: "question:1:body:1", Kind: OutNotice, Body: "agent question"},
			{DeliveryKey: "mirror:agent", Kind: OutMirror, Body: `<strong>🤖 ✅ Codex answer:</strong> local answer`},
			{DeliveryKey: "helper:notice", Kind: OutNotice, Body: "helper notice"},
		})
		if err != nil {
			return err
		}
		rows, err := tx.PendingOutbox(conv)
		if err != nil {
			return err
		}
		for _, row := range rows {
			token, err := tx.BeginSend(row.ID)
			if err != nil {
				return err
			}
			_ = token
			if err := tx.MarkSent(row.ID, row.DeliveryKey); err != nil {
				return err
			}
		}
		latest, ok, err := tx.LatestQuestionMarkerMessage(conv)
		if err != nil || !ok || latest.DeliveryKey != "mirror:agent" {
			t.Fatalf("latest question message %+v, %v, %v", latest, ok, err)
		}
		_, err = tx.Enqueue(conv, []OutMsg{
			{DeliveryKey: "mirror:user-later", Kind: OutMirror, Body: `<strong>🧑‍💻 User:</strong> later user prompt`},
			{DeliveryKey: "notice:ordinary", Kind: OutNotice, Body: "not an agent message"},
		})
		if err != nil {
			return err
		}
		rows, err = tx.PendingOutbox(conv)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if _, err := tx.BeginSend(row.ID); err != nil {
				return err
			}
			if err := tx.MarkSent(row.ID, row.DeliveryKey); err != nil {
				return err
			}
		}
		latest, ok, err = tx.LatestQuestionMarkerMessage(conv)
		if err != nil || !ok || latest.DeliveryKey != "mirror:agent" {
			t.Fatalf("helper notice became marker target: %+v, %v, %v", latest, ok, err)
		}
		return nil
	}))
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

// UpsertSources (W1b) does what UpsertSource does for each source in
// turn, with one prepared statement: same ids, same rows, unchanged rows
// not rewritten, a negative offset refused.
func TestUpsertSourcesIsUpsertSourceInBulk(t *testing.T) {
	one, _ := openTest(t)
	bulk, _ := openTest(t)
	batches := [][]TranscriptSource{
		{
			{Path: "/r/a.jsonl", ThreadID: "th-a", State: SrcBaseline, Offset: 10, Size: 10, MTime: 1, Inode: 7},
			{Path: "/r/b.jsonl", ThreadID: "th-b", State: SrcIgnored, Size: 5, MTime: 2},
			{Path: "/r/c.jsonl", State: SrcBaseline, Offset: 3, Size: 3, MTime: 3},
		},
		{
			{Path: "/r/a.jsonl", ThreadID: "th-a", State: SrcBaseline, Offset: 10, Size: 10, MTime: 1, Inode: 7}, // unchanged
			{Path: "/r/b.jsonl", ThreadID: "th-b", State: SrcBaseline, Offset: 9, Size: 9, MTime: 4},             // changed; offset kept
			{Path: "/r/d.jsonl", ThreadID: "th-d", State: SrcIgnored, Size: 1, MTime: 5},
		},
	}
	stamps := func(s *Store) map[string]int64 {
		srcs, err := s.Sources()
		mustT(t, err)
		out := map[string]int64{}
		for _, r := range srcs {
			out[r.Path] = r.UpdatedAt
		}
		return out
	}
	var before map[string]int64
	for i, b := range batches {
		var want, got []int64
		mustT(t, one.Update(func(tx *Tx) error {
			want = want[:0]
			for _, src := range b {
				id, err := tx.UpsertSource(src)
				if err != nil {
					return err
				}
				want = append(want, id)
			}
			return nil
		}))
		if i == 1 {
			before = stamps(bulk)
		}
		mustT(t, bulk.Update(func(tx *Tx) error {
			var err error
			got, err = tx.UpsertSources(b)
			return err
		}))
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("batch %d: ids %v, want %v", i, got, want)
		}
	}
	a, err := one.Sources()
	mustT(t, err)
	b, err := bulk.Sources()
	mustT(t, err)
	if len(a) != len(b) || len(a) != 4 {
		t.Fatalf("%d and %d sources, want 4", len(a), len(b))
	}
	for i := range a {
		a[i].UpdatedAt, b[i].UpdatedAt = 0, 0
		if a[i] != b[i] {
			t.Errorf("source %d: bulk %+v, one by one %+v", i, b[i], a[i])
		}
	}
	if after := stamps(bulk); after["/r/a.jsonl"] != before["/r/a.jsonl"] {
		t.Errorf("an unchanged source was rewritten")
	}
	err = bulk.Update(func(tx *Tx) error {
		_, err := tx.UpsertSources([]TranscriptSource{{Path: "/r/e.jsonl", State: SrcBaseline, Offset: -1}})
		return err
	})
	if !errors.Is(err, ErrState) {
		t.Fatalf("negative offset: %v, want ErrState", err)
	}
	if ids, err := bulk.Sources(); err != nil || len(ids) != 4 {
		t.Fatalf("after a refused batch: %d sources, %v", len(ids), err)
	}
}
