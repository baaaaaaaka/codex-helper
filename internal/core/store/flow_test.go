package store

import (
	"fmt"
	"testing"
)

func TestAdmitWindowOrderAndCursor(t *testing.T) {
	s, _ := openTest(t)
	a := newWork(t, s, "a")
	msgs := []InboundMsg{
		prompt("m3", 30), prompt("m1", 10),
		{ExtMsgID: "m2", ExtTime: 20, Sender: SenderOwner, Kind: InCommand, Body: "/status"},
		{ExtMsgID: "e1", ExtTime: 15, Sender: SenderSelf, Kind: InEcho},
		prompt("m0b", 10), // same time as m1: ext id breaks the tie
	}
	var adm []Admitted
	mustT(t, s.Update(func(tx *Tx) (err error) {
		adm, err = tx.AdmitWindow("teams:chat:a", a, msgs, "cur-1")
		return
	}))
	var order []string
	for _, x := range adm {
		order = append(order, x.ExtMsgID+":"+x.State)
	}
	if got, want := join(order), "m0b:queued,m1:queued,e1:ignored,m2:queued,m3:queued"; got != want {
		t.Fatalf("admitted %s, want %s", got, want)
	}
	// Ids ascend in admission order and prompts got turns in the same order.
	var ids string
	mustT(t, s.r.QueryRow(`SELECT group_concat(ext_msg_id, ',') FROM (SELECT ext_msg_id FROM inbound ORDER BY id)`).Scan(&ids))
	if ids != "m0b,m1,e1,m2,m3" {
		t.Fatalf("inbound id order %s", ids)
	}
	var turnsOrder string
	mustT(t, s.r.QueryRow(`SELECT group_concat(i.ext_msg_id, ',') FROM (SELECT * FROM turns ORDER BY id) t JOIN inbound i ON i.id = t.inbound_id`).Scan(&turnsOrder))
	if turnsOrder != "m0b,m1,m3" {
		t.Fatalf("turn order %s", turnsOrder)
	}
	mustT(t, s.Update(func(tx *Tx) error {
		st, ok, err := tx.Stream("teams:chat:a")
		if err != nil || !ok || st.Cursor != "cur-1" || !st.Seeded {
			t.Fatalf("stream %+v %v %v", st, ok, err)
		}
		return nil
	}))

	// Turn snapshots of model/effort; key format.
	mustT(t, s.Update(func(tx *Tx) error {
		tr, _, err := tx.Turn(1)
		if tr.Model != "m1" || tr.Effort != "high" || len(tr.TurnKey) > 64 || tr.TurnKey[:4] != "cxp-" {
			t.Fatalf("turn %+v", tr)
		}
		return err
	}))

	// A failing window commits nothing: neither rows nor the cursor.
	bad := []InboundMsg{prompt("n1", 40), {ExtMsgID: "n2", ExtTime: 41, Sender: "martian", Kind: InPrompt}}
	if err := s.Update(func(tx *Tx) error { _, err := tx.AdmitWindow("teams:chat:a", a, bad, "cur-2"); return err }); err == nil {
		t.Fatal("bad window admitted")
	}
	if n := countWhere(t, s, `SELECT count(*) FROM inbound WHERE ext_msg_id = 'n1'`); n != 0 {
		t.Fatal("partial window committed")
	}
	mustT(t, s.Update(func(tx *Tx) error {
		st, _, err := tx.Stream("teams:chat:a")
		if st.Cursor != "cur-1" {
			t.Fatalf("cursor moved on failure: %q", st.Cursor)
		}
		return err
	}))

	// Re-admitting the same window adds nothing.
	mustT(t, s.Update(func(tx *Tx) (err error) {
		adm, err = tx.AdmitWindow("teams:chat:a", a, msgs, "cur-1")
		return
	}))
	if len(adm) != 0 {
		t.Fatalf("re-admitted %d", len(adm))
	}
}

func join(v []string) string {
	s := ""
	for i, x := range v {
		if i > 0 {
			s += ","
		}
		s += x
	}
	return s
}

func TestOutboxOrderAndPerConversationBlocking(t *testing.T) {
	s, c := openTest(t)
	a := newWork(t, s, "a")
	b := newWork(t, s, "b")
	mustT(t, s.Update(func(tx *Tx) error {
		if _, err := tx.Enqueue(a, []OutMsg{out("a1", OutFinal), out("a2", OutFinal)}); err != nil {
			return err
		}
		_, err := tx.Enqueue(b, []OutMsg{out("b1", OutFinal), out("b2", OutFinal)})
		return err
	}))
	next := func() map[int64]string {
		m := map[int64]string{}
		mustT(t, s.Update(func(tx *Tx) error {
			rows, err := tx.NextSendable(c.now().UnixMilli())
			for _, o := range rows {
				m[o.ConvID] = o.DeliveryKey
			}
			return err
		}))
		return m
	}
	if m := next(); m[a] != "a1" || m[b] != "b1" || len(m) != 2 {
		t.Fatalf("heads %v", m)
	}
	// Only the head may be sent.
	if err := s.Update(func(tx *Tx) error { _, err := tx.BeginSend(2); return err }); err == nil {
		t.Fatal("non-head BeginSend succeeded")
	}
	// a1 sending blocks a only.
	mustT(t, s.Update(func(tx *Tx) error { _, err := tx.BeginSend(1); return err }))
	if m := next(); m[a] != "" || m[b] != "b1" {
		t.Fatalf("heads while a1 sending %v", m)
	}
	// unknown keeps blocking a and is never offered again.
	mustT(t, s.Update(func(tx *Tx) error { return tx.MarkUnknown(1, "timeout") }))
	for i := 0; i < 3; i++ {
		c.add(OwnerStaleAfter)
		if m := next(); m[a] != "" || m[b] != "b1" {
			t.Fatalf("heads while a1 unknown %v", m)
		}
	}
	// b drains independently.
	mustT(t, s.Update(func(tx *Tx) error {
		if _, err := tx.BeginSend(3); err != nil {
			return err
		}
		return tx.MarkSent(3, "mb1")
	}))
	if m := next(); m[b] != "b2" || m[a] != "" {
		t.Fatalf("heads after b1 sent %v", m)
	}
	// Resolution by read-back: a1 found -> sent; a2 becomes the head.
	mustT(t, s.Update(func(tx *Tx) error { return tx.MarkSent(1, "ma1") }))
	if m := next(); m[a] != "a2" {
		t.Fatalf("heads after a1 resolved %v", m)
	}
	// Backoff: a requeued row is not due before next_attempt_at.
	mustT(t, s.Update(func(tx *Tx) error {
		if _, err := tx.BeginSend(2); err != nil {
			return err
		}
		return tx.Requeue(2, c.now().UnixMilli()+5000)
	}))
	if m := next(); m[a] != "" {
		t.Fatalf("backed-off row offered %v", m)
	}
	c.add(5001 * 1e6)
	if m := next(); m[a] != "a2" {
		t.Fatalf("due row not offered %v", m)
	}
	// Own-message detection by ext id and body hash.
	mustT(t, s.Update(func(tx *Tx) error {
		own, err := tx.IsOwn(a, "ma1", nil, 0)
		if err != nil || !own {
			t.Fatalf("IsOwn by ext id %v %v", own, err)
		}
		own, err = tx.IsOwn(a, "zz", nil, 0)
		if err != nil || own {
			t.Fatalf("IsOwn of a foreign id %v %v", own, err)
		}
		return nil
	}))
}

func TestUnknownIsNeverResent(t *testing.T) {
	s, c := openTest(t)
	a := newWork(t, s, "a")
	mustT(t, s.Update(func(tx *Tx) error {
		_, err := tx.Enqueue(a, []OutMsg{out("x", OutControl)})
		return err
	}))
	mustT(t, s.Update(func(tx *Tx) error {
		if _, err := tx.BeginSend(1); err != nil {
			return err
		}
		return tx.MarkUnknown(1, "ambiguous")
	}))
	// BeginSend refuses it; NextSendable never returns it; restart keeps it.
	wantErr(t, s.Update(func(tx *Tx) error { _, err := tx.BeginSend(1); return err }), ErrState)
	path := s.Path()
	s.Close()
	s2 := openAt(t, path, c)
	c.add(OwnerStaleAfter * 2)
	_, err := s2.Acquire(testIdent(101))
	mustT(t, err)
	mustT(t, s2.Update(func(tx *Tx) error {
		rows, err := tx.NextSendable(c.now().UnixMilli() + 1e9)
		if len(rows) != 0 {
			t.Fatalf("unknown offered after restart: %+v", rows)
		}
		return err
	}))
	unk, err := s2.UnknownOutbox()
	mustT(t, err)
	if len(unk) != 1 || unk[0].DeliveryKey != "x" {
		t.Fatalf("UnknownOutbox %+v", unk)
	}
	// Only an explicit Requeue (read-back absence) makes it sendable again.
	mustT(t, s2.Update(func(tx *Tx) error { return tx.Requeue(1, 0) }))
	mustT(t, s2.Update(func(tx *Tx) error {
		rows, err := tx.NextSendable(c.now().UnixMilli())
		if len(rows) != 1 {
			t.Fatalf("requeued row not offered")
		}
		return err
	}))
}

func TestFinishTurnAtomic(t *testing.T) {
	s, _ := openTest(t)
	a := newWork(t, s, "a")
	mustT(t, s.Update(func(tx *Tx) error {
		_, err := tx.AdmitWindow("k", a, []InboundMsg{prompt("p1", 1)}, "c1")
		return err
	}))
	var tr Turn
	mustT(t, s.Update(func(tx *Tx) error {
		var ok bool
		var err error
		tr, ok, err = tx.NextRunnable(a)
		if err != nil || !ok {
			t.Fatalf("not runnable: %v", err)
		}
		if err := tx.MarkStarting(tr.ID, "th-a"); err != nil {
			return err
		}
		return tx.MarkRunning(tr.ID, "codex-1")
	}))
	// A failing output rolls back the turn transition too.
	bad := []OutMsg{out("f1", OutFinal), {DeliveryKey: "f2", Kind: "bogus"}}
	if err := s.Update(func(tx *Tx) error { return tx.FinishTurn(tr.ID, TurnCompleted, "", bad) }); err == nil {
		t.Fatal("bad outputs accepted")
	}
	mustT(t, s.Update(func(tx *Tx) error {
		got, _, err := tx.Turn(tr.ID)
		in, _, _ := tx.Inbound(got.InboundID)
		if got.State != TurnRunning || in.State != InQueued {
			t.Fatalf("partial finish: turn %s inbound %s", got.State, in.State)
		}
		return err
	}))
	if n := countWhere(t, s, `SELECT count(*) FROM outbox`); n != 0 {
		t.Fatalf("outputs leaked: %d", n)
	}
	mustT(t, s.Update(func(tx *Tx) error {
		return tx.FinishTurn(tr.ID, TurnCompleted, "", []OutMsg{out("f1", OutFinal), out("f2", OutFinal)})
	}))
	mustT(t, s.Update(func(tx *Tx) error {
		got, _, err := tx.Turn(tr.ID)
		in, _, _ := tx.Inbound(got.InboundID)
		if got.State != TurnCompleted || in.State != InDone || got.FinishedAt == 0 {
			t.Fatalf("finish: turn %+v inbound %s", got, in.State)
		}
		return err
	}))
	if n := countWhere(t, s, `SELECT count(*) FROM outbox WHERE conv_id = ?`, a); n != 2 {
		t.Fatalf("outputs %d", n)
	}
	// Finishing twice is refused; a non-terminal state is refused.
	wantErr(t, s.Update(func(tx *Tx) error { return tx.FinishTurn(tr.ID, TurnFailed, "", nil) }), ErrState)
	wantErr(t, s.Update(func(tx *Tx) error { return tx.FinishTurn(tr.ID, TurnRunning, "", nil) }), ErrState)
}

func TestAdvanceSourceAtomic(t *testing.T) {
	s, _ := openTest(t)
	a := newWork(t, s, "a")
	var id int64
	mustT(t, s.Update(func(tx *Tx) (err error) {
		id, err = tx.UpsertSource(TranscriptSource{Path: "/r/a.jsonl", ThreadID: "th-a", ConvID: a, State: SrcTracking, Size: 10, MTime: 1})
		return
	}))
	mustT(t, s.Update(func(tx *Tx) error {
		return tx.AdvanceSource(id, 100, 3, 100, 2, 7, []OutMsg{out("src-1", OutMirror)})
	}))
	bad := []OutMsg{out("src-2", OutMirror), {DeliveryKey: "src-3", Kind: "bogus"}}
	if err := s.Update(func(tx *Tx) error { return tx.AdvanceSource(id, 200, 5, 200, 3, 7, bad) }); err == nil {
		t.Fatal("bad outputs accepted")
	}
	mustT(t, s.Update(func(tx *Tx) error {
		src, _, err := tx.SourceByID(id)
		if src.Offset != 100 || src.Line != 3 {
			t.Fatalf("offset moved without its outputs: %+v", src)
		}
		return err
	}))
	if n := countWhere(t, s, `SELECT count(*) FROM outbox WHERE delivery_key = 'src-2'`); n != 0 {
		t.Fatal("output committed without offset")
	}
	// Never back to 0 after progress; negative refused.
	wantErr(t, s.Update(func(tx *Tx) error { return tx.AdvanceSource(id, 0, 0, 0, 3, 7, nil) }), ErrState)
	wantErr(t, s.Update(func(tx *Tx) error { return tx.AdvanceSource(id, -1, 0, 0, 3, 7, nil) }), ErrState)
	// UpsertSource never moves the offset.
	mustT(t, s.Update(func(tx *Tx) error {
		_, err := tx.UpsertSource(TranscriptSource{Path: "/r/a.jsonl", ThreadID: "th-a", ConvID: a, State: SrcTracking, Offset: 0, Size: 300, MTime: 9})
		return err
	}))
	mustT(t, s.Update(func(tx *Tx) error {
		src, _, err := tx.SourceByID(id)
		if src.Offset != 100 || src.Size != 300 {
			t.Fatalf("upsert moved offset: %+v", src)
		}
		return err
	}))
	// Outputs need a conversation.
	var orphan int64
	mustT(t, s.Update(func(tx *Tx) (err error) {
		orphan, err = tx.UpsertSource(TranscriptSource{Path: "/r/b.jsonl", State: SrcBaseline})
		return
	}))
	wantErr(t, s.Update(func(tx *Tx) error { return tx.AdvanceSource(orphan, 5, 1, 5, 1, 0, []OutMsg{out("o", OutMirror)}) }), ErrState)
}

func TestPausedHoldsPrompts(t *testing.T) {
	s, _ := openTest(t)
	a := newWork(t, s, "a")
	mustT(t, s.Update(func(tx *Tx) error { return tx.Pause(a, "breaker") }))
	cmd := InboundMsg{ExtMsgID: "c1", ExtTime: 2, Sender: SenderOwner, Kind: InCommand, Body: "/status"}
	var adm []Admitted
	mustT(t, s.Update(func(tx *Tx) (err error) {
		adm, err = tx.AdmitWindow("k", a, []InboundMsg{prompt("p1", 1), cmd, prompt("p2", 3)}, "c")
		return
	}))
	for _, x := range adm {
		want := InHeld
		if x.Kind == InCommand {
			want = InQueued // commands still run while paused (unpause, status)
		}
		if x.State != want || (x.Kind == InPrompt && x.TurnID != 0) {
			t.Fatalf("admitted %+v while paused", x)
		}
	}
	if n := countWhere(t, s, `SELECT count(*) FROM turns`); n != 0 {
		t.Fatalf("turns while paused: %d", n)
	}
	mustT(t, s.Update(func(tx *Tx) error {
		_, ok, err := tx.NextRunnable(a)
		if ok {
			t.Fatal("runnable while paused")
		}
		return err
	}))
	var released int
	mustT(t, s.Update(func(tx *Tx) (err error) { released, err = tx.Unpause(a); return }))
	if released != 2 {
		t.Fatalf("released %d", released)
	}
	var order string
	mustT(t, s.r.QueryRow(`SELECT group_concat(i.ext_msg_id, ',') FROM (SELECT * FROM turns ORDER BY id) t JOIN inbound i ON i.id = t.inbound_id`).Scan(&order))
	if order != "p1,p2" {
		t.Fatalf("release order %s", order)
	}
	// A running turn is not interrupted by Pause, but no next turn starts.
	mustT(t, s.Update(func(tx *Tx) error {
		tr, ok, err := tx.NextRunnable(a)
		if err != nil || !ok {
			t.Fatalf("not runnable after unpause")
		}
		if err := tx.MarkStarting(tr.ID, ""); err != nil {
			return err
		}
		if err := tx.FinishTurn(tr.ID, TurnCompleted, "", nil); err != nil {
			return err
		}
		if err := tx.Pause(a, "again"); err != nil {
			return err
		}
		_, ok, err = tx.NextRunnable(a)
		if ok {
			t.Fatal("runnable after re-pause")
		}
		return err
	}))
}

func TestNextRunnableBlocking(t *testing.T) {
	s, _ := openTest(t)
	a := newWork(t, s, "a")
	f := InboundMsg{ExtMsgID: "p1", ExtTime: 1, Sender: SenderOwner, Kind: InPrompt,
		Files: []InboundFile{{Idx: 0, Name: "x.png", ExtRef: "r", Kind: "image"}}}
	mustT(t, s.Update(func(tx *Tx) error {
		_, err := tx.AdmitWindow("k", a, []InboundMsg{f, prompt("p2", 2)}, "c")
		return err
	}))
	runnable := func() (Turn, bool) {
		var tr Turn
		var ok bool
		mustT(t, s.Update(func(tx *Tx) (err error) { tr, ok, err = tx.NextRunnable(a); return }))
		return tr, ok
	}
	if _, ok := runnable(); ok {
		t.Fatal("runnable with a pending attachment")
	}
	if ids, err := s.PendingInboundFiles(); err != nil || len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("pending files %v %v", ids, err)
	}
	mustT(t, s.Update(func(tx *Tx) error { return tx.SetInboundFile(1, 0, "ready", "/tmp/x.png", "") }))
	if ids, err := s.PendingInboundFiles(); err != nil || len(ids) != 0 {
		t.Fatalf("pending files after ready %v %v", ids, err)
	}
	tr, ok := runnable()
	if !ok || tr.ID != 1 {
		t.Fatalf("head not runnable: %+v", tr)
	}
	// A pending outbox row of the conversation blocks the next turn.
	mustT(t, s.Update(func(tx *Tx) error { _, err := tx.Enqueue(a, []OutMsg{out("n", OutNotice)}); return err }))
	if _, ok := runnable(); ok {
		t.Fatal("runnable with pending outbox")
	}
	mustT(t, s.Update(func(tx *Tx) error {
		if _, err := tx.BeginSend(1); err != nil {
			return err
		}
		return tx.MarkSent(1, "m")
	}))
	mustT(t, s.Update(func(tx *Tx) error { return tx.MarkStarting(1, "th") }))
	if _, ok := runnable(); ok {
		t.Fatal("second turn runnable while first is starting")
	}
	// Capacity retry keeps the first deadline.
	mustT(t, s.Update(func(tx *Tx) error { return tx.RequeueTurn(1, 111, "", "") }))
	mustT(t, s.Update(func(tx *Tx) error {
		tr, _, err := tx.Turn(1)
		if tr.CodexTurnID != "" {
			t.Fatalf("a turn Codex never started records no Codex turn: %+v", tr)
		}
		return err
	}))
	mustT(t, s.Update(func(tx *Tx) error { return tx.MarkStarting(1, "") }))
	// Codex accepted this attempt in a thread it created: both are kept
	// for the next attempt, which continues there.
	mustT(t, s.Update(func(tx *Tx) error { return tx.RequeueTurn(1, 222, "th-new", "ct-1") }))
	mustT(t, s.Update(func(tx *Tx) error {
		tr, _, err := tx.Turn(1)
		if tr.RetryDeadline != 111 || tr.Attempt != 2 || tr.State != TurnQueued ||
			tr.ThreadID != "th-new" || tr.CodexTurnID != "ct-1" {
			t.Fatalf("requeued turn %+v", tr)
		}
		return err
	}))
	// After progress the window starts again.
	mustT(t, s.Update(func(tx *Tx) error { return tx.MarkStarting(1, "th-new") }))
	// A later attempt Codex refused keeps what the accepted one recorded.
	mustT(t, s.Update(func(tx *Tx) error { return tx.RequeueTurnNewWindow(1, 333, "", "") }))
	mustT(t, s.Update(func(tx *Tx) error {
		tr, _, err := tx.Turn(1)
		if err != nil {
			return err
		}
		if tr.RetryDeadline != 333 || tr.State != TurnQueued || tr.ThreadID != "th-new" || tr.CodexTurnID != "ct-1" {
			t.Fatalf("requeued turn after progress %+v", tr)
		}
		return nil
	}))
}

// Only rows that must reach the chat before the next turn starts block it:
// an earlier turn's replies, final and interruption notice, and rows that
// belong to no turn. ACKs and the notices of the queued turns themselves
// (reminders, the start notice) do not (benchmark F4: 72 s of dead time
// waiting for ACKs of later prompts under paced writes).
func TestNextRunnableBlocksOnlyOnEarlierTurnRows(t *testing.T) {
	s, _ := openTest(t)
	a := newWork(t, s, "a")
	mustT(t, s.Update(func(tx *Tx) error {
		_, err := tx.AdmitWindow("k", a, []InboundMsg{prompt("p1", 1), prompt("p2", 2), prompt("p3", 3)}, "c")
		return err
	}))
	runnable := func() (Turn, bool) {
		var tr Turn
		var ok bool
		mustT(t, s.Update(func(tx *Tx) (err error) { tr, ok, err = tx.NextRunnable(a); return }))
		return tr, ok
	}
	enqueue := func(key, kind string) {
		mustT(t, s.Update(func(tx *Tx) error { _, err := tx.Enqueue(a, []OutMsg{out(key, kind)}); return err }))
	}
	// settle delivers the conversation's pending rows in order (the outbox
	// sends only its head) up to and including key.
	settle := func(key string) {
		mustT(t, s.Update(func(tx *Tx) error {
			pending, err := tx.PendingOutbox(a)
			if err != nil {
				return err
			}
			for _, o := range pending {
				if _, err := tx.BeginSend(o.ID); err != nil {
					return err
				}
				if err := tx.MarkSent(o.ID, "m-"+o.DeliveryKey); err != nil {
					return err
				}
				if o.DeliveryKey == key {
					return nil
				}
			}
			return fmt.Errorf("%s is not pending", key)
		}))
	}
	// The ACKs of all three prompts and the reminders of the queued ones
	// are pending: the head still starts.
	for _, k := range []string{"ack:1", "ack:2", "ack:3"} {
		enqueue(k, OutAck)
	}
	enqueue("turn:2:waiting:1", OutNotice)
	enqueue("turn:3:waiting:1", OutNotice)
	tr, ok := runnable()
	if !ok || tr.ID != 1 {
		t.Fatalf("head not runnable behind ACKs and queued-turn notices: %+v %v", tr, ok)
	}
	mustT(t, s.Update(func(tx *Tx) error {
		if err := tx.MarkStarting(tr.ID, "th"); err != nil {
			return err
		}
		return tx.MarkRunning(tr.ID, "codex-1")
	}))
	enqueue("turn:1:progress:1", OutProgress)
	mustT(t, s.Update(func(tx *Tx) error {
		return tx.FinishTurn(tr.ID, TurnCompleted, "", []OutMsg{out("turn:1:final:1", OutFinal)})
	}))
	// The first turn's progress and final are pending: the next waits.
	if _, ok := runnable(); ok {
		t.Fatal("next turn runnable before the earlier turn's final was sent")
	}
	settle("turn:1:progress:1")
	if _, ok := runnable(); ok {
		t.Fatal("next turn runnable before the earlier turn's final was sent")
	}
	settle("turn:1:final:1")
	tr, ok = runnable()
	if !ok || tr.ID != 2 {
		t.Fatalf("second turn not runnable once the first turn's rows were sent: %+v %v", tr, ok)
	}
	// An interruption notice of an earlier turn, and a row of no turn (a
	// command reply), block; a later turn's row (turn 12 > 2) does not.
	enqueue("turn:12:waiting:1", OutNotice)
	if _, ok := runnable(); !ok {
		t.Fatal("a later turn's notice blocked the head")
	}
	enqueue("turn:1:interrupted", OutNotice)
	if _, ok := runnable(); ok {
		t.Fatal("runnable before an earlier turn's interruption notice was sent")
	}
	settle("turn:1:interrupted")
	enqueue("cmd:7", OutNotice)
	if _, ok := runnable(); ok {
		t.Fatal("runnable before a pending row of no turn was sent")
	}
}

func TestGatesAndMeta(t *testing.T) {
	s, c := openTest(t)
	now := c.now().UnixMilli()
	mustT(t, s.Update(func(tx *Tx) error {
		if err := tx.SetGate("chat:a", now+1000, "429"); err != nil {
			return err
		}
		if err := tx.SetGate("chat:a", now+500, "shorter"); err != nil { // never shortens
			return err
		}
		return tx.SetGate("graph", now-1, "expired")
	}))
	g, err := s.Gates(now)
	mustT(t, err)
	if len(g) != 1 || g[0].Until != now+1000 || g[0].Reason != "429" {
		t.Fatalf("gates %+v", g)
	}
	// Expired gates are scheduling state: kept (overwritten by key), never deleted.
	if n := countWhere(t, s, `SELECT count(*) FROM gates`); n != 2 {
		t.Fatalf("gates rows %d", n)
	}
	mustT(t, s.Update(func(tx *Tx) error {
		if err := tx.SetMeta("temporary", "id"); err != nil {
			return err
		}
		return tx.DeleteMeta("temporary")
	}))
	if _, ok, err := s.Meta("temporary"); err != nil || ok {
		t.Fatalf("deleted meta still present: %v %v", ok, err)
	}
}
