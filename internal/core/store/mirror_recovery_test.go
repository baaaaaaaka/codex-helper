package store

import (
	"errors"
	"fmt"
	"testing"
)

func TestRecoveryBatchYieldsWithoutLosingMembers(t *testing.T) {
	s, _ := openTest(t)
	conv := newWork(t, s, "recovery")
	var rows []Outbox
	mustT(t, s.Update(func(tx *Tx) error {
		var msgs []OutMsg
		for i := 0; i < 40; i++ {
			m := out(fmt.Sprint("old-", i), OutMirror)
			m.ProgressGroup = RecoveryGroupPrefix + "source"
			msgs = append(msgs, m)
		}
		if _, err := tx.Enqueue(conv, msgs); err != nil {
			return err
		}
		var err error
		rows, err = tx.PendingOutbox(conv)
		return err
	}))
	rollback := errors.New("crash")
	wantErr(t, s.Update(func(tx *Tx) error {
		if err := tx.FreezeProgressBatch(rows[:32], "frozen originals", []byte{1}); err != nil {
			return err
		}
		if _, err := tx.BeginSend(rows[0].ID); err != nil {
			return err
		}
		return rollback
	}), rollback)
	mustT(t, s.Update(func(tx *Tx) error {
		r, _, err := tx.Outbox(rows[0].ID)
		if err != nil {
			return err
		}
		if r.BatchHead != 0 || r.State != OutQueued {
			t.Fatal("batch partially committed")
		}
		if err := tx.FreezeProgressBatch(rows[:32], "frozen originals", []byte{1}); err != nil {
			return err
		}
		if _, err := tx.BeginSend(rows[0].ID); err != nil {
			return err
		}
		if _, err := tx.Enqueue(conv, []OutMsg{out("current-progress", OutProgress), out("current-final", OutFinal)}); err != nil {
			return err
		}
		heads, err := tx.NextSendable(tx.Now())
		if err != nil {
			return err
		}
		if len(heads) != 0 {
			t.Fatal("bypassed in-flight recovery")
		}
		return tx.Requeue(rows[0].ID, tx.Now()+100000)
	}))
	// Even a delayed frozen recovery must yield, without growing or changing.
	for _, key := range []string{"current-progress", "current-final"} {
		mustT(t, s.Update(func(tx *Tx) error {
			heads, err := tx.NextSendable(tx.Now())
			if err != nil {
				return err
			}
			if len(heads) != 1 || heads[0].DeliveryKey != key {
				t.Fatalf("head=%+v want %s", heads, key)
			}
			if _, err := tx.BeginSend(heads[0].ID); err != nil {
				return err
			}
			return tx.MarkSent(heads[0].ID, "live-"+key)
		}))
	}
	mustT(t, s.Update(func(tx *Tx) error {
		heads, err := tx.NextSendable(tx.Now() + 200000)
		if err != nil {
			return err
		}
		if len(heads) != 1 || heads[0].Body != "frozen originals" {
			t.Fatalf("frozen batch changed: %+v", heads)
		}
		if _, err := tx.BeginSend(heads[0].ID); err != nil {
			return err
		}
		return tx.MarkUnknown(heads[0].ID, "response lost")
	}))
	// Simulate restart with persisted unknown batch and reconcile late.
	unknown, err := s.UnknownOutbox()
	mustT(t, err)
	if len(unknown) != 1 || unknown[0].Body != "frozen originals" {
		t.Fatal(unknown)
	}
	mustT(t, s.Update(func(tx *Tx) error {
		if err := tx.MarkSent(rows[0].ID, "recovered-post"); err != nil {
			return err
		}
		for i, old := range rows {
			r, _, err := tx.Outbox(old.ID)
			if err != nil {
				return err
			}
			if i < 32 {
				if r.State != OutSent || r.ExtMsgID != "recovered-post" {
					t.Fatal("member not confirmed", r)
				}
			} else if r.State != OutQueued {
				t.Fatal("unrelated row settled", r)
			}
		}
		heads, err := tx.NextSendable(tx.Now())
		if err != nil {
			return err
		}
		if len(heads) != 1 || heads[0].ID != rows[32].ID {
			t.Fatal("remaining recovery lost", heads)
		}
		return nil
	}))
}

func TestRecoveryBatchDoesNotCrossBoundaries(t *testing.T) {
	for _, boundary := range []string{"live", "source", "split", "file", "delay", "held", "reply", "mention", "frozen"} {
		t.Run(boundary, func(t *testing.T) {
			s, _ := openTest(t)
			conv := newWork(t, s, "recovery")
			a, b := out("a", OutMirror), out("b", OutMirror)
			a.ProgressGroup = RecoveryGroupPrefix + "one"
			b.ProgressGroup = a.ProgressGroup
			switch boundary {
			case "live":
				b.ProgressGroup = ""
			case "source":
				b.ProgressGroup = RecoveryGroupPrefix + "two"
			case "split":
				b.Part, b.Parts = 1, 2
			case "file":
				b.File = &OutFile{LocalPath: "/not-opened", Size: 1}
			case "reply":
				b.ReplyTo = "external"
			case "mention":
				b.Mention = true
			}
			mustT(t, s.Update(func(tx *Tx) error {
				if _, err := tx.Enqueue(conv, []OutMsg{a, b}); err != nil {
					return err
				}
				rows, err := tx.PendingOutbox(conv)
				if err != nil {
					return err
				}
				switch boundary {
				case "delay":
					_, err = tx.exec(`UPDATE outbox SET next_attempt_at=? WHERE id=?`, tx.Now()+1000, rows[1].ID)
				case "held":
					_, err = tx.exec(`UPDATE outbox SET hold_turn=123 WHERE id=?`, rows[1].ID)
				case "frozen":
					_, err = tx.exec(`UPDATE outbox SET batch_head=id,batch_body='fixed' WHERE id=?`, rows[1].ID)
				}
				if err != nil {
					return err
				}
				candidates, err := tx.ProgressBatchCandidates(rows[0], tx.Now())
				if err != nil {
					return err
				}
				if len(candidates) != 1 {
					t.Fatalf("crossed %s: %d", boundary, len(candidates))
				}
				return nil
			}))
		})
	}
}

// Retained history must not turn sender polling into a full history scan or a
// write. The fixture includes both historical and pending recovery records.
func TestRecoverySchedulingReadsDoNotWrite(t *testing.T) {
	s, _ := openTest(t)
	conv := newWork(t, s, "history")
	mustT(t, s.Update(func(tx *Tx) error {
		_, err := tx.exec(`WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<20000)
   INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,created_at)
   SELECT ?,'retained-'||x,x,'mirror','retained','sent',1 FROM n`, conv)
		if err != nil {
			return err
		}
		for i := 0; i < 256; i++ {
			m := out(fmt.Sprint("recovery-", i), OutMirror)
			m.ProgressGroup = RecoveryGroupPrefix + "source"
			if _, err := tx.Enqueue(conv, []OutMsg{m}); err != nil {
				return err
			}
		}
		_, err = tx.Enqueue(conv, []OutMsg{out("live", OutFinal)})
		return err
	}))
	var before, after int64
	mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&before))
	for i := 0; i < 20; i++ {
		mustT(t, s.Update(func(tx *Tx) error {
			rows, err := tx.NextSendable(tx.Now())
			if err != nil {
				return err
			}
			if len(rows) != 1 || rows[0].DeliveryKey != "live" {
				t.Fatal("live head lost")
			}
			return nil
		}))
	}
	mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&after))
	if before != after {
		t.Fatalf("poll wrote %d rows", after-before)
	}
}

func BenchmarkRecoveryNextSendable(b *testing.B) {
	s, err := Open(b.TempDir() + "/core.db")
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	if _, err = s.Acquire(testIdent(100)); err != nil {
		b.Fatal(err)
	}
	var conv int64
	err = s.Update(func(tx *Tx) error {
		var err error
		conv, err = tx.CreateConversation(NewConversation{Label: "bench", Kind: KindWork, Channel: "teams", ExtChatID: "bench"})
		if err != nil {
			return err
		}
		_, err = tx.exec(`WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<20000)
   INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,created_at)
   SELECT ?,'retained-'||x,x,'mirror','retained','sent',1 FROM n`, conv)
		if err != nil {
			return err
		}
		for i := 0; i < 256; i++ {
			m := out(fmt.Sprint("recovery-", i), OutMirror)
			m.ProgressGroup = RecoveryGroupPrefix + "source"
			if _, err = tx.Enqueue(conv, []OutMsg{m}); err != nil {
				return err
			}
		}
		_, err = tx.Enqueue(conv, []OutMsg{out("live", OutFinal)})
		return err
	})
	if err != nil {
		b.Fatal(err)
	}
	oldQuery := `SELECT ` + outboxCols + ` FROM outbox WHERE id IN (SELECT (SELECT p.id FROM outbox p WHERE p.conv_id=h.conv_id AND p.state IN ('queued','sending') AND p.hold_turn IS NULL ORDER BY p.seq LIMIT 1) FROM (SELECT DISTINCT conv_id FROM outbox WHERE state IN ('queued','sending')) h) AND state='queued' AND (next_attempt_at IS NULL OR next_attempt_at<=?) ORDER BY conv_id`
	for _, workload := range []string{"recovery", "ordinary"} {
		if workload == "ordinary" {
			if err := s.Update(func(tx *Tx) error {
				_, err := tx.exec(`UPDATE outbox SET progress_group=NULL WHERE state='queued'`)
				return err
			}); err != nil {
				b.Fatal(err)
			}
		}
		b.Run(workload, func(b *testing.B) {
			for _, name := range []string{"previous", "current"} {
				b.Run(name, func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						if err := s.Read(func(tx *Tx) error {
							if name == "previous" {
								_, err := collect(scanOutbox)(tx.rows(oldQuery, tx.Now()))
								return err
							}
							_, err := tx.NextSendable(tx.Now())
							return err
						}); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

func TestRecoveryFairnessDoesNotStarveEitherClass(t *testing.T) {
	s, c := openTest(t)
	conv := newWork(t, s, "fair")
	mustT(t, s.Update(func(tx *Tx) error {
		old := out("old", OutMirror)
		old.ProgressGroup = RecoveryGroupPrefix + "source"
		_, err := tx.Enqueue(conv, []OutMsg{old, out("current", OutFinal)})
		return err
	}))
	mustT(t, s.Update(func(tx *Tx) error {
		rows, err := tx.NextSendableWithRecovery(tx.Now())
		if err != nil {
			return err
		}
		if len(rows) != 1 || rows[0].DeliveryKey != "current" {
			t.Fatal("young recovery delayed live content")
		}
		return nil
	}))
	c.add(3 * 60 * 1000000000)
	mustT(t, s.Update(func(tx *Tx) error {
		ordinary, err := tx.NextSendable(tx.Now())
		if err != nil {
			return err
		}
		if ordinary[0].DeliveryKey != "current" {
			t.Fatal("default scheduling must prefer current")
		}
		if _, err := tx.exec(`UPDATE outbox SET next_attempt_at=? WHERE delivery_key='old'`, tx.Now()+60000); err != nil {
			return err
		}
		delayed, err := tx.NextSendableWithRecovery(tx.Now())
		if err != nil {
			return err
		}
		if len(delayed) != 1 || delayed[0].DeliveryKey != "current" {
			t.Fatal("delayed aged recovery blocked current")
		}
		if _, err := tx.exec(`UPDATE outbox SET next_attempt_at=NULL WHERE delivery_key='old'`); err != nil {
			return err
		}
		fair, err := tx.NextSendableWithRecovery(tx.Now())
		if err != nil {
			return err
		}
		if len(fair) != 1 || fair[0].DeliveryKey != "old" {
			t.Fatal("recovery starved")
		}
		if _, err := tx.BeginSend(fair[0].ID); !errors.Is(err, ErrState) {
			t.Fatal("ordinary send bypassed current message", err)
		}
		if _, err := tx.BeginRecoverySend(fair[0].ID); err != nil {
			return err
		}
		if _, err := tx.BeginSend(ordinary[0].ID); !errors.Is(err, ErrState) {
			t.Fatal("in-flight recovery bypassed", err)
		}
		return tx.MarkSent(fair[0].ID, "recovery-first")
	}))
	mustT(t, s.Update(func(tx *Tx) error {
		rows, err := tx.NextSendable(tx.Now())
		if err != nil {
			return err
		}
		if len(rows) != 1 || rows[0].DeliveryKey != "current" {
			t.Fatal("current message lost after fairness")
		}
		return nil
	}))
}

func TestRecoveryFrozenBatchSurvivesReopen(t *testing.T) {
	s, c := openTest(t)
	conv := newWork(t, s, "durable")
	path := s.Path()
	var head int64
	var token string
	mustT(t, s.Update(func(tx *Tx) error {
		var msgs []OutMsg
		for i := 0; i < 10; i++ {
			m := out(fmt.Sprint("old-", i), OutMirror)
			m.ProgressGroup = RecoveryGroupPrefix + "source"
			msgs = append(msgs, m)
		}
		if _, err := tx.Enqueue(conv, msgs); err != nil {
			return err
		}
		rows, err := tx.PendingOutbox(conv)
		if err != nil {
			return err
		}
		head = rows[0].ID
		if err = tx.FreezeProgressBatch(rows, "exact frozen body", []byte{7}); err != nil {
			return err
		}
		token, err = tx.BeginSend(head)
		return err
	}))
	mustT(t, s.Release())
	mustT(t, s.Close())
	s = openAt(t, path, c)
	_, err := s.Acquire(testIdent(100))
	mustT(t, err)
	mustT(t, s.Update(func(tx *Tx) error {
		row, _, err := tx.Outbox(head)
		if err != nil {
			return err
		}
		if row.Body != "exact frozen body" || row.AttemptToken != token || row.State != OutSending {
			t.Fatal("send evidence changed across reopen", row)
		}
		if err = tx.MarkUnknown(head, "crash after POST"); err != nil {
			return err
		}
		_, err = tx.Enqueue(conv, []OutMsg{out("new-final", OutFinal)})
		return err
	}))
	unknown, err := s.UnknownOutbox()
	mustT(t, err)
	if len(unknown) != 1 || unknown[0].AttemptToken != token {
		t.Fatal("batch split across restart", unknown)
	}
	mustT(t, s.Update(func(tx *Tx) error {
		rows, err := tx.NextSendable(tx.Now())
		if err != nil {
			return err
		}
		if len(rows) != 1 || rows[0].DeliveryKey != "new-final" {
			t.Fatal("unknown recovery blocked live tail", rows)
		}
		if err = tx.MarkSent(head, "found-by-readback"); err != nil {
			return err
		}
		originals, err := collect(scanOutbox)(tx.rows(`SELECT `+outboxCols+` FROM outbox WHERE batch_head=? ORDER BY seq`, head))
		if err != nil {
			return err
		}
		if len(originals) != 10 {
			t.Fatal("lost batch members")
		}
		for _, r := range originals {
			if r.State != OutSent || r.ExtMsgID != "found-by-readback" {
				t.Fatal("member not confirmed", r)
			}
		}
		return nil
	}))
}
