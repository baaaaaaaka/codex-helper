package store

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"
)

// The model-based test drives the store and a pure-Go reference model with
// the same seeded random operations and compares the complete observable
// state after every operation.

type mInbound struct {
	ID      int64
	Conv    int64
	Ext     string
	ExtTime int64
	Kind    string
	State   string
	Turn    int64
}

type mTurn struct {
	ID      int64
	Conv    int64
	Inbound int64
	State   string
}

type mOutbox struct {
	ID    int64
	Conv  int64
	Key   string
	Seq   int64
	State string
	Next  int64
}

type mSource struct {
	ID     int64
	Offset int64
	Line   int64
}

type model struct {
	paused  map[int64]bool
	inbound []mInbound
	turns   []mTurn
	outbox  []mOutbox
	sources []mSource
	cursor  map[int64]string
}

func (m *model) inboundByExt(conv int64, ext string) *mInbound {
	for i := range m.inbound {
		if m.inbound[i].Conv == conv && m.inbound[i].Ext == ext {
			return &m.inbound[i]
		}
	}
	return nil
}

func (m *model) inboundByID(id int64) *mInbound { return &m.inbound[id-1] }
func (m *model) turn(id int64) *mTurn           { return &m.turns[id-1] }
func (m *model) out(id int64) *mOutbox          { return &m.outbox[id-1] }

func (m *model) newTurn(conv, inbound int64) int64 {
	id := int64(len(m.turns) + 1)
	m.turns = append(m.turns, mTurn{ID: id, Conv: conv, Inbound: inbound, State: TurnQueued})
	return id
}

func (m *model) admit(conv int64, msgs []InboundMsg, cursor string) {
	sorted := append([]InboundMsg(nil), msgs...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].ExtTime != sorted[j].ExtTime {
			return sorted[i].ExtTime < sorted[j].ExtTime
		}
		return sorted[i].ExtMsgID < sorted[j].ExtMsgID
	})
	for _, x := range sorted {
		if m.inboundByExt(conv, x.ExtMsgID) != nil {
			continue
		}
		st := InQueued
		switch {
		case x.Sender == SenderSelf || x.Kind == InSystem:
			st = InIgnored
		case x.Kind == InPrompt && m.paused[conv]:
			st = InHeld
		}
		id := int64(len(m.inbound) + 1)
		m.inbound = append(m.inbound, mInbound{ID: id, Conv: conv, Ext: x.ExtMsgID, ExtTime: x.ExtTime, Kind: x.Kind, State: st})
		if x.Kind == InPrompt && st == InQueued {
			m.inbound[id-1].Turn = m.newTurn(conv, id)
		}
	}
	m.cursor[conv] = cursor
}

func (m *model) unpause(conv int64) int {
	m.paused[conv] = false
	var held []*mInbound
	for i := range m.inbound {
		if in := &m.inbound[i]; in.Conv == conv && in.State == InHeld {
			held = append(held, in)
		}
	}
	sort.SliceStable(held, func(i, j int) bool {
		if held[i].ExtTime != held[j].ExtTime {
			return held[i].ExtTime < held[j].ExtTime
		}
		return held[i].ID < held[j].ID
	})
	for _, in := range held {
		if in.Kind == InPrompt {
			in.Turn = m.newTurn(conv, in.ID)
		}
		in.State = InQueued
	}
	return len(held)
}

func pendingOut(st string) bool { return st == OutQueued || st == OutSending || st == OutUnknown }
func openTurn(st string) bool   { return st == TurnQueued || st == TurnStarting || st == TurnRunning }

func (m *model) nextRunnable(conv int64) (int64, bool) {
	if m.paused[conv] {
		return 0, false
	}
	var head int64
	for _, tr := range m.turns {
		if tr.Conv != conv || !openTurn(tr.State) {
			continue
		}
		if tr.State != TurnQueued {
			return 0, false
		}
		if head == 0 {
			head = tr.ID
		}
	}
	if head == 0 {
		return 0, false
	}
	for _, o := range m.outbox {
		if o.Conv == conv && pendingOut(o.State) {
			return 0, false
		}
	}
	return head, true
}

func (m *model) enqueue(conv int64, keys []string) {
	for _, k := range keys {
		dup := false
		for _, o := range m.outbox {
			if o.Key == k {
				dup = true
			}
		}
		if dup {
			continue
		}
		var seq int64
		for _, o := range m.outbox {
			if o.Conv == conv && o.Seq > seq {
				seq = o.Seq
			}
		}
		m.outbox = append(m.outbox, mOutbox{ID: int64(len(m.outbox) + 1), Conv: conv, Key: k, Seq: seq + 1, State: OutQueued})
	}
}

func (m *model) head(conv int64) *mOutbox {
	var h *mOutbox
	for i := range m.outbox {
		o := &m.outbox[i]
		if o.Conv == conv && pendingOut(o.State) && (h == nil || o.Seq < h.Seq) {
			h = o
		}
	}
	return h
}

func (m *model) sendable(convs []int64, now int64) []string {
	var keys []string
	for _, c := range convs {
		if h := m.head(c); h != nil && h.State == OutQueued && (h.Next == 0 || h.Next <= now) {
			keys = append(keys, h.Key)
		}
	}
	return keys
}

// snapshot reads the store state into model form.
func snapshot(t *testing.T, s *Store, convs []int64) model {
	t.Helper()
	m := model{paused: map[int64]bool{}, cursor: map[int64]string{}}
	mustT(t, s.View(func(q Querier) error {
		rows, err := q.QueryContext(bg, `SELECT id,conv_id,ext_msg_id,ext_time,kind,state,COALESCE(turn_id,0) FROM inbound ORDER BY id`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var x mInbound
			if err := rows.Scan(&x.ID, &x.Conv, &x.Ext, &x.ExtTime, &x.Kind, &x.State, &x.Turn); err != nil {
				return err
			}
			m.inbound = append(m.inbound, x)
		}
		rows.Close()
		rows, err = q.QueryContext(bg, `SELECT id,conv_id,COALESCE(inbound_id,0),state FROM turns ORDER BY id`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var x mTurn
			if err := rows.Scan(&x.ID, &x.Conv, &x.Inbound, &x.State); err != nil {
				return err
			}
			m.turns = append(m.turns, x)
		}
		rows.Close()
		rows, err = q.QueryContext(bg, `SELECT id,conv_id,delivery_key,seq,state,COALESCE(next_attempt_at,0) FROM outbox ORDER BY id`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var x mOutbox
			if err := rows.Scan(&x.ID, &x.Conv, &x.Key, &x.Seq, &x.State, &x.Next); err != nil {
				return err
			}
			m.outbox = append(m.outbox, x)
		}
		rows.Close()
		rows, err = q.QueryContext(bg, `SELECT id,offset,line FROM transcript_sources ORDER BY id`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var x mSource
			if err := rows.Scan(&x.ID, &x.Offset, &x.Line); err != nil {
				return err
			}
			m.sources = append(m.sources, x)
		}
		rows.Close()
		rows, err = q.QueryContext(bg, `SELECT id, paused_at IS NOT NULL FROM conversations ORDER BY id`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id int64
			var p bool
			if err := rows.Scan(&id, &p); err != nil {
				return err
			}
			m.paused[id] = p
		}
		rows.Close()
		rows, err = q.QueryContext(bg, `SELECT conv_id, COALESCE(cursor,'') FROM streams`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id int64
			var c string
			if err := rows.Scan(&id, &c); err != nil {
				return err
			}
			m.cursor[id] = c
		}
		return rows.Close()
	}))
	return m
}

func TestModelBased(t *testing.T) {
	seeds, steps := 12, 400
	if testing.Short() {
		seeds, steps = 3, 200
	}
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) { runModel(t, seed, steps) })
	}
}

func runModel(t *testing.T, seed int64, steps int) {
	rng := rand.New(rand.NewSource(seed))
	s, clk := openTest(t)
	convs := []int64{newWork(t, s, "a"), newWork(t, s, "b"), newWork(t, s, "c")}
	m := &model{paused: map[int64]bool{}, cursor: map[int64]string{}}
	for _, c := range convs {
		m.paused[c] = false
		var id int64
		mustT(t, s.Update(func(tx *Tx) (err error) {
			id, err = tx.UpsertSource(TranscriptSource{Path: fmt.Sprint("/r/", c), ConvID: c, State: SrcTracking})
			return
		}))
		m.sources = append(m.sources, mSource{ID: id})
	}
	extN, keyN := 0, 0
	pick := func(n int) int { return rng.Intn(n) }
	conv := func() int64 { return convs[pick(len(convs))] }
	var log []string
	fail := func(format string, a ...any) {
		t.Helper()
		start := len(log) - 15
		if start < 0 {
			start = 0
		}
		t.Fatalf("seed %d step %d: %s\nlast ops:\n%v", seed, len(log), fmt.Sprintf(format, a...), log[start:])
	}
	newKeys := func() []string {
		var ks []string
		for i := pick(4); i > 0; i-- {
			if keyN > 0 && pick(4) == 0 {
				ks = append(ks, fmt.Sprint("k", pick(keyN))) // reuse: must be a no-op
			} else {
				ks = append(ks, fmt.Sprint("k", keyN))
				keyN++
			}
		}
		return ks
	}
	toOut := func(ks []string) []OutMsg {
		var o []OutMsg
		for _, k := range ks {
			o = append(o, out(k, OutFinal))
		}
		return o
	}

	for step := 0; step < steps; step++ {
		c := conv()
		now := clk.now().UnixMilli()
		var err error
		switch op := pick(12); op {
		case 0, 1: // receive window
			var msgs []InboundMsg
			for i := pick(5); i > 0; i-- {
				if len(m.inbound) > 0 && pick(4) == 0 {
					e := m.inbound[pick(len(m.inbound))]
					if e.Conv == c {
						msgs = append(msgs, InboundMsg{ExtMsgID: e.Ext, ExtTime: e.ExtTime, Sender: SenderOwner, Kind: e.Kind})
						continue
					}
				}
				x := InboundMsg{ExtMsgID: fmt.Sprint("e", extN), ExtTime: int64(pick(50)), Sender: SenderOwner, Kind: InPrompt}
				extN++
				switch pick(6) {
				case 0:
					x.Kind = InCommand
				case 1:
					x.Kind, x.Sender = InEcho, SenderSelf
				case 2:
					x.Kind = InSystem
				}
				msgs = append(msgs, x)
			}
			cursor := fmt.Sprint("c", pick(3))
			log = append(log, fmt.Sprintf("admit %d %d msgs cursor %s", c, len(msgs), cursor))
			err = s.Update(func(tx *Tx) error { _, err := tx.AdmitWindow(fmt.Sprint("s", c), c, msgs, cursor); return err })
			m.admit(c, msgs, cursor)
		case 2: // pause / unpause
			if m.paused[c] {
				log = append(log, fmt.Sprintf("unpause %d", c))
				var n int
				err = s.Update(func(tx *Tx) (err error) { n, err = tx.Unpause(c); return })
				if want := m.unpause(c); err == nil && n != want {
					fail("unpause released %d, model %d", n, want)
				}
			} else {
				log = append(log, fmt.Sprintf("pause %d", c))
				err = s.Update(func(tx *Tx) error { return tx.Pause(c, "x") })
				m.paused[c] = true
			}
		case 3: // start the next runnable turn
			want, wok := m.nextRunnable(c)
			log = append(log, fmt.Sprintf("start %d (model %d %v)", c, want, wok))
			err = s.Update(func(tx *Tx) error {
				tr, ok, err := tx.NextRunnable(c)
				if err != nil {
					return err
				}
				if ok != wok || tr.ID != want {
					fail("NextRunnable(%d) = %d %v, model %d %v", c, tr.ID, ok, want, wok)
				}
				if !ok {
					return nil
				}
				return tx.MarkStarting(tr.ID, "th")
			})
			if wok {
				m.turn(want).State = TurnStarting
			}
		case 4, 5: // advance or finish an open turn
			var open []int64
			for _, tr := range m.turns {
				if tr.State == TurnStarting || tr.State == TurnRunning {
					open = append(open, tr.ID)
				}
			}
			if len(open) == 0 {
				continue
			}
			id := open[pick(len(open))]
			tr := m.turn(id)
			switch {
			case tr.State == TurnStarting && pick(2) == 0:
				log = append(log, fmt.Sprintf("running %d", id))
				err = s.Update(func(tx *Tx) error { return tx.MarkRunning(id, "ct") })
				tr.State = TurnRunning
			case pick(5) == 0:
				log = append(log, fmt.Sprintf("requeue turn %d", id))
				err = s.Update(func(tx *Tx) error { return tx.RequeueTurn(id, now+1000) })
				tr.State = TurnQueued
			default:
				final := []string{TurnCompleted, TurnFailed, TurnInterrupted}[pick(3)]
				ks := newKeys()
				log = append(log, fmt.Sprintf("finish %d %s %v", id, final, ks))
				err = s.Update(func(tx *Tx) error { return tx.FinishTurn(id, final, "", toOut(ks)) })
				tr.State = final
				if in := m.inboundByID(tr.Inbound); in.State == InQueued || in.State == InHeld {
					in.State = InDone
				}
				m.enqueue(tr.Conv, ks)
			}
		case 6: // execute a queued command
			var cmds []int64
			for _, in := range m.inbound {
				if in.Kind == InCommand && in.State == InQueued {
					cmds = append(cmds, in.ID)
				}
			}
			if len(cmds) == 0 {
				continue
			}
			id := cmds[pick(len(cmds))]
			ks := newKeys()
			log = append(log, fmt.Sprintf("command %d %v", id, ks))
			cv := m.inboundByID(id).Conv
			err = s.Update(func(tx *Tx) error {
				if err := tx.SetInbound(id, InDone, 0); err != nil {
					return err
				}
				_, err := tx.Enqueue(cv, toOut(ks))
				return err
			})
			m.inboundByID(id).State = InDone
			m.enqueue(cv, ks)
		case 7, 8: // send
			want := m.sendable(convs, now)
			var got []string
			var rows []Outbox
			err = s.Update(func(tx *Tx) (err error) { rows, err = tx.NextSendable(now); return })
			for _, o := range rows {
				got = append(got, o.DeliveryKey)
			}
			if !reflect.DeepEqual(got, want) {
				fail("NextSendable = %v, model %v", got, want)
			}
			if len(rows) == 0 {
				continue
			}
			o := rows[pick(len(rows))]
			mo := m.out(o.ID)
			outcome := pick(6)
			at := now + 1 + int64(pick(3000))
			log = append(log, fmt.Sprintf("send %s outcome %d", o.DeliveryKey, outcome))
			err = s.Update(func(tx *Tx) error {
				if _, err := tx.BeginSend(o.ID); err != nil {
					return err
				}
				switch outcome {
				case 0, 1, 2:
					return tx.MarkSent(o.ID, "x"+o.DeliveryKey)
				case 3:
					return tx.MarkUnknown(o.ID, "?")
				case 4:
					return tx.Requeue(o.ID, at)
				default:
					return tx.Drop(o.ID, "gone")
				}
			})
			switch outcome {
			case 0, 1, 2:
				mo.State, mo.Next = OutSent, 0
			case 3:
				mo.State = OutUnknown
			case 4:
				mo.State = OutQueued
				mo.Next = at
			default:
				mo.State = OutDropped
			}
		case 9: // resolve an unknown row by read-back
			var unk []int64
			for _, o := range m.outbox {
				if o.State == OutUnknown {
					unk = append(unk, o.ID)
				}
			}
			if len(unk) == 0 {
				continue
			}
			id := unk[pick(len(unk))]
			found := pick(2) == 0
			log = append(log, fmt.Sprintf("resolve %d found=%v", id, found))
			err = s.Update(func(tx *Tx) error {
				if found {
					return tx.MarkSent(id, "rb")
				}
				return tx.Requeue(id, 0)
			})
			if found {
				m.out(id).State, m.out(id).Next = OutSent, 0
			} else {
				m.out(id).State, m.out(id).Next = OutQueued, 0
			}
		case 10: // transcript progress with outputs, sometimes a replay
			i := pick(len(m.sources))
			src := &m.sources[i]
			off, line := src.Offset+int64(1+pick(100)), src.Line+1
			ks := []string{fmt.Sprintf("src%d-%d", src.ID, line)}
			if src.Line > 0 && pick(3) == 0 { // replay of an already-published record
				off, line = src.Offset, src.Line
				ks = []string{fmt.Sprintf("src%d-%d", src.ID, src.Line)}
			}
			log = append(log, fmt.Sprintf("advance %d -> %d %v", src.ID, off, ks))
			err = s.Update(func(tx *Tx) error { return tx.AdvanceSource(src.ID, off, line, off, 1, 0, toOut(ks)) })
			src.Offset, src.Line = off, line
			m.enqueue(convs[i], ks)
		case 11: // time passes; illegal transitions are refused and change nothing
			clk.add(time.Duration(pick(4000)) * time.Millisecond)
			if len(m.outbox) > 0 {
				o := m.outbox[pick(len(m.outbox))]
				if o.State == OutSent || o.State == OutDropped || o.State == OutQueued {
					log = append(log, fmt.Sprintf("illegal MarkUnknown %d (%s)", o.ID, o.State))
					e := s.Update(func(tx *Tx) error { return tx.MarkUnknown(o.ID, "x") })
					if !errors.Is(e, ErrState) {
						fail("MarkUnknown on %s row: %v", o.State, e)
					}
				}
			}
		}
		if err != nil {
			fail("op error: %v", err)
		}
		got := snapshot(t, s, convs)
		norm := func(x []mInbound) []mInbound {
			if x == nil {
				return []mInbound{}
			}
			return x
		}
		if !reflect.DeepEqual(norm(got.inbound), norm(m.inbound)) {
			fail("inbound differs:\nstore %+v\nmodel %+v", got.inbound, m.inbound)
		}
		if !eqTurns(got.turns, m.turns) {
			fail("turns differ:\nstore %+v\nmodel %+v", got.turns, m.turns)
		}
		if !eqOut(got.outbox, m.outbox) {
			fail("outbox differs:\nstore %+v\nmodel %+v", got.outbox, m.outbox)
		}
		if !reflect.DeepEqual(got.sources, m.sources) {
			fail("sources differ:\nstore %+v\nmodel %+v", got.sources, m.sources)
		}
		for _, c := range convs {
			if got.paused[c] != m.paused[c] {
				fail("paused[%d] = %v, model %v", c, got.paused[c], m.paused[c])
			}
			if got.cursor[c] != m.cursor[c] {
				fail("cursor[%d] = %q, model %q", c, got.cursor[c], m.cursor[c])
			}
		}
	}
	// Global invariants at the end of the run.
	for _, c := range convs {
		var last int64
		for _, o := range m.outbox {
			if o.Conv == c {
				if o.Seq != last+1 {
					t.Fatalf("seq gap in conv %d", c)
				}
				last = o.Seq
			}
		}
	}
	var fk int
	mustT(t, s.w.QueryRow(`SELECT count(*) FROM pragma_foreign_key_check`).Scan(&fk))
	if fk != 0 {
		t.Fatalf("%d foreign key violations", fk)
	}
}

func eqTurns(a, b []mTurn) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return reflect.DeepEqual(a, b)
}

func eqOut(a, b []mOutbox) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return reflect.DeepEqual(a, b)
}
