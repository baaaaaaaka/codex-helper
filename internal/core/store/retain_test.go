package store

import (
	"testing"
	"time"
)

func TestRetention(t *testing.T) {
	s, c := openTest(t)
	a := newWork(t, s, "a")
	h := newWork(t, s, "h") // becomes 'hold' (migrated staging fork)
	mustT(t, s.Update(func(tx *Tx) error {
		_, err := tx.exec(`UPDATE conversations SET state = 'hold' WHERE id = ?`, h)
		return err
	}))
	runTurn := func(conv int64, ext string, ts int64) {
		mustT(t, s.Update(func(tx *Tx) error {
			adm, err := tx.AdmitWindow("k"+ext, conv, []InboundMsg{prompt(ext, ts)}, ext)
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
	runTurn(a, "old1", 1)
	runTurn(a, "old2", 2)
	runTurn(h, "hold1", 3)
	// An old open (unknown) outbox row and an old held prompt are kept.
	mustT(t, s.Update(func(tx *Tx) error {
		if _, err := tx.Enqueue(a, []OutMsg{out("stuck", OutNotice)}); err != nil {
			return err
		}
		rows, _ := tx.NextSendable(c.now().UnixMilli())
		if _, err := tx.BeginSend(rows[0].ID); err != nil {
			return err
		}
		return tx.MarkUnknown(rows[0].ID, "?")
	}))

	// 8 days: bodies of terminal rows go, rows stay.
	c.add(8 * 24 * time.Hour)
	_, err := s.Retain(c.now())
	mustT(t, err)
	if n := countWhere(t, s, `SELECT count(*) FROM inbound i JOIN conversations c ON c.id=i.conv_id WHERE c.state <> 'hold' AND body IS NOT NULL`); n != 0 {
		t.Fatalf("%d inbound bodies kept after 8 days", n)
	}
	if n := countWhere(t, s, `SELECT count(*) FROM outbox WHERE state = 'sent' AND conv_id = ? AND body IS NOT NULL`, a); n != 0 {
		t.Fatalf("%d sent bodies kept", n)
	}
	if n := countWhere(t, s, `SELECT count(*) FROM outbox WHERE delivery_key = 'stuck' AND body IS NOT NULL`); n != 1 {
		t.Fatal("open row lost its body")
	}
	if n := countWhere(t, s, `SELECT count(*) FROM inbound WHERE conv_id = ? AND body IS NOT NULL`, h); n != 1 {
		t.Fatal("hold conversation touched")
	}

	// A fresh turn after the window.
	c.add(115 * 24 * time.Hour)
	runTurn(a, "new1", 4)
	_, err = s.Retain(c.now())
	mustT(t, err)
	var keys string
	mustT(t, s.r.QueryRow(`SELECT group_concat(delivery_key, ',') FROM (SELECT delivery_key FROM outbox ORDER BY id)`).Scan(&keys))
	if keys != "o-hold1,stuck,o-new1" {
		t.Fatalf("outbox after 123 days: %s", keys)
	}
	var exts string
	mustT(t, s.r.QueryRow(`SELECT group_concat(ext_msg_id, ',') FROM (SELECT ext_msg_id FROM inbound ORDER BY id)`).Scan(&exts))
	if exts != "hold1,new1" {
		t.Fatalf("inbound after 123 days: %s", exts)
	}
	// The latest terminal turn of each conversation stays ("retry last").
	if n := countWhere(t, s, `SELECT count(*) FROM turns WHERE conv_id = ?`, a); n != 1 {
		t.Fatalf("turns of a: %d", n)
	}
	c.add(200 * 24 * time.Hour)
	_, err = s.Retain(c.now())
	mustT(t, err)
	if n := countWhere(t, s, `SELECT count(*) FROM turns WHERE conv_id = ?`, a); n != 1 {
		t.Fatal("latest terminal turn deleted")
	}
	if n := countWhere(t, s, `SELECT count(*) FROM outbox WHERE delivery_key = 'stuck'`); n != 1 {
		t.Fatal("unknown row deleted")
	}
	var fk int
	mustT(t, s.w.QueryRow(`SELECT count(*) FROM pragma_foreign_key_check`).Scan(&fk))
	if fk != 0 {
		t.Fatalf("%d foreign key violations", fk)
	}
}
