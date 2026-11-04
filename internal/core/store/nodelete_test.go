package store

import (
	"testing"
	"time"
)

// TestNothingIsDeleted: no retention (§6). After a year and a Vacuum, every
// row and every body is still there, including terminal turns, sent outbox
// rows, done inbound rows and expired gates.
func TestNothingIsDeleted(t *testing.T) {
	s, c := openTest(t)
	a := newWork(t, s, "a")
	for i, ext := range []string{"m1", "m2", "m3"} {
		mustT(t, s.Update(func(tx *Tx) error {
			adm, err := tx.AdmitWindow("k"+ext, a, []InboundMsg{prompt(ext, int64(i+1))}, ext)
			if err != nil {
				return err
			}
			if err := tx.MarkStarting(adm[0].TurnID, ""); err != nil {
				return err
			}
			return tx.FinishTurn(adm[0].TurnID, TurnCompleted, "", []OutMsg{out("o-"+ext, OutFinal)})
		}))
		mustT(t, s.Update(func(tx *Tx) error {
			rows, err := tx.NextSendable(c.now().UnixMilli())
			for _, o := range rows {
				if _, err := tx.BeginSend(o.ID); err != nil {
					return err
				}
				if err := tx.MarkSent(o.ID, "x"+o.DeliveryKey); err != nil {
					return err
				}
			}
			return err
		}))
	}
	mustT(t, s.Update(func(tx *Tx) error { return tx.SetGate("graph", c.now().UnixMilli()-1, "expired") }))
	counts := func() [5]int {
		return [5]int{
			countWhere(t, s, `SELECT count(*) FROM inbound WHERE body IS NOT NULL`),
			countWhere(t, s, `SELECT count(*) FROM turns`),
			countWhere(t, s, `SELECT count(*) FROM outbox WHERE body IS NOT NULL AND state = 'sent'`),
			countWhere(t, s, `SELECT count(*) FROM gates`),
			countWhere(t, s, `SELECT count(*) FROM streams`),
		}
	}
	before := counts()
	if before[0] != 3 || before[1] != 3 || before[2] != 3 || before[3] != 1 {
		t.Fatalf("setup counts %v", before)
	}
	c.add(365 * 24 * time.Hour)
	mustT(t, s.Vacuum())
	if after := counts(); after != before {
		t.Fatalf("rows changed after a year + Vacuum: %v -> %v", before, after)
	}
}
