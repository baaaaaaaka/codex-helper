package store

import (
	"fmt"
	"testing"
	"time"
)

func TestUnconfirmedUnderPrefersDroppedParts(t *testing.T) {
	s, _ := openTest(t)
	conv := newWork(t, s, "partial-answer")
	mustT(t, s.Update(func(tx *Tx) error {
		if _, err := tx.Enqueue(conv, []OutMsg{out("answer:1", OutFinal), out("answer:2", OutFinal)}); err != nil {
			return err
		}
		if _, err := tx.BeginSend(1); err != nil {
			return err
		}
		if err := tx.MarkUnknown(1, "lost response"); err != nil {
			return err
		}
		if state, err := tx.UnconfirmedUnder("answer:", OutFinal); err != nil || state != OutUnknown {
			t.Fatalf("unknown answer: %q %v", state, err)
		}
		if err := tx.Drop(2, "rejected part"); err != nil {
			return err
		}
		if state, err := tx.UnconfirmedUnder("answer:", OutFinal); err != nil || state != OutDropped {
			t.Fatalf("incomplete answer: %q %v", state, err)
		}
		if state, err := tx.UnconfirmedUnder("answer:", OutProgress); err != nil || state != "" {
			t.Fatalf("kind filter: %q %v", state, err)
		}
		return nil
	}))
}

func TestUnknownRowsDoNotBlockActiveWork(t *testing.T) {
	for _, kind := range []string{OutProgress, OutMirror, OutFinal, OutControl, OutNotice, OutAck} {
		t.Run(kind, func(t *testing.T) {
			s, c := openTest(t)
			conv := newWork(t, s, kind)
			mustT(t, s.Update(func(tx *Tx) error {
				_, err := tx.AdmitWindow("k", conv, []InboundMsg{prompt("p", 1)}, "c")
				if err != nil {
					return err
				}
				_, err = tx.Enqueue(conv, []OutMsg{out("first", kind), out("second", kind), out("third", kind)})
				return err
			}))
			for id := int64(1); id <= 2; id++ {
				mustT(t, s.Update(func(tx *Tx) error {
					if _, err := tx.BeginSend(id); err != nil {
						return err
					}
					if _, ok, err := tx.NextRunnable(conv); err != nil || ok && kind != OutAck {
						t.Fatalf("sending output did not block: %v %v", ok, err)
					}
					return tx.MarkUnknown(id, "lost response")
				}))
			}
			mustT(t, s.Update(func(tx *Tx) error {
				heads, err := tx.NextSendable(c.now().UnixMilli())
				if err != nil {
					return err
				}
				if len(heads) != 1 || heads[0].DeliveryKey != "third" {
					t.Fatalf("wrong head: %+v", heads)
				}
				if _, err := tx.BeginSend(3); err != nil {
					return err
				}
				if _, err := tx.BeginSend(1); err == nil {
					t.Fatal("unknown was sent again")
				}
				// A late confirmation of an older attempt must not disturb the active sender.
				if err := tx.MarkSent(1, "late"); err != nil {
					return err
				}
				if rows, err := tx.NextSendable(tx.Now()); err != nil || len(rows) != 0 {
					t.Fatalf("second sender allowed: %+v %v", rows, err)
				}
				if err := tx.MarkUnknown(3, "lost again"); err != nil {
					return err
				}
				if _, ok, err := tx.NextRunnable(conv); err != nil || !ok {
					t.Fatalf("unknown blocked task: %v %v", ok, err)
				}
				active, unknown, err := tx.DeliveryCounts(conv)
				if active != 0 || unknown != 2 {
					t.Fatalf("counts: %d %d", active, unknown)
				}
				rows, err := tx.ActiveOutbox(conv)
				if err != nil || len(rows) != 0 {
					t.Fatalf("active rows: %+v %v", rows, err)
				}
				return err
			}))
			if yes, err := s.PendingAtLeast(conv, 1); err != nil || yes {
				t.Fatalf("unknown backpressure: %v %v", yes, err)
			}
			if yes, err := s.PendingProgress(conv); err != nil || yes {
				t.Fatalf("unknown hid working notice: %v %v", yes, err)
			}
			rows, err := s.UnknownOutbox()
			mustT(t, err)
			if len(rows) != 2 || rows[0].AttemptToken == "" {
				t.Fatalf("unknown evidence lost: %+v", rows)
			}
		})
	}
}

// Large retained history must not be loaded as message bodies by active queries.
func TestActiveQueriesWithLargeUnknownHistory(t *testing.T) {
	s, _ := openTest(t)
	conv := newWork(t, s, "large-unknown")
	mustT(t, s.Update(func(tx *Tx) error {
		_, err := tx.exec(`WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<100000)
 INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,created_at)
 SELECT ?, 'unknown:'||x, x, 'progress', 'retained body', 'unknown', ? FROM n`, conv, tx.Now())
		if err != nil {
			return err
		}
		_, err = tx.Enqueue(conv, []OutMsg{out("active-final", OutFinal)})
		return err
	}))
	var before, after int64
	mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&before))
	start := time.Now()
	for i := 0; i < 10; i++ {
		mustT(t, s.Update(func(tx *Tx) error {
			rows, err := tx.NextSendable(tx.Now())
			if err != nil {
				return err
			}
			if len(rows) != 1 || rows[0].DeliveryKey != "active-final" {
				t.Fatalf("active head lost: %+v", rows)
			}
			rows, err = tx.ActiveOutbox(conv)
			if len(rows) != 1 {
				t.Fatalf("active query returned history: %d", len(rows))
			}
			return err
		}))
	}
	t.Logf("100000 retained unknown rows, 10 active passes: %s", time.Since(start))
	mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&after))
	if after != before {
		t.Fatalf("read-only active queries wrote %d rows", after-before)
	}
	checkHotPlans(t, s)
	mustT(t, s.Update(func(tx *Tx) error {
		active, unknown, err := tx.DeliveryCounts(conv)
		if active != 1 || unknown != 100000 {
			return fmt.Errorf("wrong counts: %d %d", active, unknown)
		}
		return err
	}))
}
