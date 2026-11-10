package store

import (
	"errors"
	"fmt"
	"testing"
)

func TestProgressBatchAtomicRetryAndResolution(t *testing.T) {
	s, _ := openTest(t)
	conv := newWork(t, s, "batch")
	var rows []Outbox
	mustT(t, s.Update(func(tx *Tx) error {
		var msgs []OutMsg
		for i := 0; i < 3; i++ {
			m := out(fmt.Sprint("p", i), OutProgress)
			m.ProgressGroup = "turn:one"
			msgs = append(msgs, m)
		}
		msgs = append(msgs, out("final", OutFinal))
		if _, err := tx.Enqueue(conv, msgs); err != nil {
			return err
		}
		var err error
		rows, err = tx.PendingOutbox(conv)
		return err
	}))
	rollback := errors.New("crash before commit")
	wantErr(t, s.Update(func(tx *Tx) error {
		if err := tx.FreezeProgressBatch(rows[:3], "all three", []byte{7}); err != nil {
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
			t.Fatalf("partial batch committed: %+v", r)
		}
		if err := tx.FreezeProgressBatch(rows[:3], "all three", []byte{7}); err != nil {
			return err
		}
		tok, err := tx.BeginSend(rows[0].ID)
		if err != nil {
			return err
		}
		for _, row := range rows[:3] {
			r, _, err := tx.Outbox(row.ID)
			if err != nil {
				return err
			}
			if r.State != OutSending || r.AttemptToken != tok || r.BatchHead != rows[0].ID {
				t.Fatalf("member not sending together: %+v", r)
			}
		}
		return tx.Requeue(rows[0].ID, 0)
	}))
	// New commentary may arrive after a throttle; it must not change the payload.
	mustT(t, s.Update(func(tx *Tx) error {
		if _, err := tx.Enqueue(conv, []OutMsg{{DeliveryKey: "late", Kind: OutProgress, Body: "late", ProgressGroup: "turn:one"}}); err != nil {
			return err
		}
		r, _, err := tx.Outbox(rows[0].ID)
		if err != nil {
			return err
		}
		if r.Body != "all three" {
			t.Fatal("frozen body changed")
		}
		candidates, err := tx.ProgressBatchCandidates(r, tx.Now())
		if err != nil {
			return err
		}
		if len(candidates) != 0 {
			t.Fatal("frozen batch offered for merging")
		}
		if _, err := tx.BeginSend(r.ID); err != nil {
			return err
		}
		return tx.MarkUnknown(r.ID, "response lost")
	}))
	unknown, err := s.UnknownOutbox()
	mustT(t, err)
	if len(unknown) != 1 || unknown[0].Body != "all three" {
		t.Fatalf("unknown heads: %+v", unknown)
	}
	mustT(t, s.Update(func(tx *Tx) error {
		heads, err := tx.NextSendable(tx.Now())
		if err != nil {
			return err
		}
		if len(heads) != 1 || heads[0].DeliveryKey != "final" {
			t.Fatalf("unknown batch blocked final: %+v", heads)
		}
		if _, err := tx.BeginSend(heads[0].ID); err != nil {
			return err
		}
		return tx.MarkSent(heads[0].ID, "final-one")
	}))
	mustT(t, s.Update(func(tx *Tx) error {
		if err := tx.MarkSent(rows[0].ID, "external-one"); err != nil {
			return err
		}
		for _, row := range rows[:3] {
			r, _, err := tx.Outbox(row.ID)
			if err != nil {
				return err
			}
			if r.State != OutSent || r.ExtMsgID != "external-one" {
				t.Fatalf("member lost resolution: %+v", r)
			}
		}
		heads, err := tx.NextSendable(tx.Now())
		if err != nil {
			return err
		}
		if len(heads) != 1 || heads[0].DeliveryKey != "late" {
			t.Fatalf("late batch resolution changed next output: %+v", heads)
		}
		return nil
	}))
}

func TestRecordedPartsDoesNotHideMissingSplitPart(t *testing.T) {
	s, _ := openTest(t)
	conv := newWork(t, s, "parts")
	mustT(t, s.Update(func(tx *Tx) error {
		a := out("record:1", OutMirror)
		a.Part, a.Parts = 1, 2
		b := out("record:2", OutMirror)
		b.Part, b.Parts = 2, 2
		if _, err := tx.Enqueue(conv, []OutMsg{a}); err != nil {
			return err
		}
		yes, err := tx.RecordedParts([]OutMsg{a, b})
		if err != nil {
			return err
		}
		if yes {
			t.Fatal("one part hid missing part")
		}
		if n, err := tx.Enqueue(conv, []OutMsg{a, b}); err != nil || n != 1 {
			t.Fatalf("repair missing part: n=%d err=%v", n, err)
		}
		yes, err = tx.RecordedParts([]OutMsg{a, b})
		if err != nil {
			return err
		}
		if !yes {
			t.Fatal("pending parts would be duplicated")
		}
		a.AlternateKey, a.DeliveryKey = a.DeliveryKey, "stable:1"
		if n, err := tx.Enqueue(conv, []OutMsg{a}); err != nil || n != 0 {
			t.Fatalf("legacy identity duplicated: %d %v", n, err)
		}
		return nil
	}))
}

func TestProgressBatchBoundaries(t *testing.T) {
	for _, boundary := range []string{"final", "execution", "split", "reply", "mention", "delay", "limit"} {
		t.Run(boundary, func(t *testing.T) {
			s, _ := openTest(t)
			conv := newWork(t, s, "bounds")
			mustT(t, s.Update(func(tx *Tx) error {
				var msgs []OutMsg
				for i := 0; i < 30; i++ {
					msgs = append(msgs, OutMsg{DeliveryKey: fmt.Sprint(i), Kind: OutProgress, Body: "same", ProgressGroup: "one"})
				}
				switch boundary {
				case "final":
					msgs[1].Kind = OutFinal
				case "execution":
					msgs[1].ProgressGroup = "two"
				case "split":
					msgs[1].Part, msgs[1].Parts = 1, 2
				case "reply":
					msgs[1].ReplyTo = "quote"
				case "mention":
					msgs[1].Mention = true
				}
				if _, err := tx.Enqueue(conv, msgs); err != nil {
					return err
				}
				rows, err := tx.PendingOutbox(conv)
				if err != nil {
					return err
				}
				if boundary == "delay" {
					if _, err := tx.exec(`UPDATE outbox SET next_attempt_at=? WHERE id=?`, tx.Now()+100, rows[1].ID); err != nil {
						return err
					}
				}
				got, err := tx.ProgressBatchCandidates(rows[0], tx.Now())
				if err != nil {
					return err
				}
				want := 1
				if boundary == "limit" {
					want = MaxProgressBatch
				}
				if len(got) != want {
					t.Fatalf("batch crossed %s: %d want %d", boundary, len(got), want)
				}
				return nil
			}))
		})
	}
}

func TestBatchOwnEchoMatchesTransportNotUnsentMember(t *testing.T) {
	s, _ := openTest(t)
	conv := newWork(t, s, "echo")
	mustT(t, s.Update(func(tx *Tx) error {
		msgs := []OutMsg{{DeliveryKey: "a", Kind: OutProgress, Body: "a", BodyHash: []byte{1}, ProgressGroup: "g"}, {DeliveryKey: "b", Kind: OutProgress, Body: "b", BodyHash: []byte{2}, ProgressGroup: "g"}}
		if _, err := tx.Enqueue(conv, msgs); err != nil {
			return err
		}
		rows, err := tx.PendingOutbox(conv)
		if err != nil {
			return err
		}
		if err := tx.FreezeProgressBatch(rows, "a and b", []byte{3}); err != nil {
			return err
		}
		if _, err := tx.BeginSend(rows[0].ID); err != nil {
			return err
		}
		for _, c := range []struct {
			hash byte
			want bool
		}{{1, false}, {2, false}, {3, true}} {
			yes, err := tx.IsOwn(conv, "", []byte{c.hash}, 0)
			if err != nil {
				return err
			}
			if yes != c.want {
				t.Fatalf("hash %d own=%v want %v", c.hash, yes, c.want)
			}
			hits, err := tx.OutboxByHash(conv, []byte{c.hash}, 0)
			if err != nil {
				return err
			}
			if (len(hits) > 0) != c.want {
				t.Fatalf("echo query uses original unsent member: hash %d %+v", c.hash, hits)
			}
		}
		return nil
	}))
}

func TestRepresentedRecordRequiresEveryDeclaredPart(t *testing.T) {
	s, _ := openTest(t)
	conv := newWork(t, s, "represented")
	mustT(t, s.Update(func(tx *Tx) error {
		_, err := tx.Enqueue(conv, []OutMsg{{DeliveryKey: "r:2", Kind: OutMirror, Part: 2, Parts: 2, Body: "second"}})
		return err
	}))
	yes, err := s.RepresentedRecord("r:")
	mustT(t, err)
	if yes {
		t.Fatal("last part alone hid a missing first part")
	}
	mustT(t, s.Update(func(tx *Tx) error {
		_, err := tx.Enqueue(conv, []OutMsg{{DeliveryKey: "r:1", Kind: OutMirror, Part: 1, Parts: 2, Body: "first"}})
		return err
	}))
	yes, err = s.RepresentedRecord("r:")
	mustT(t, err)
	if !yes {
		t.Fatal("already queued complete record would render again")
	}
}

func TestProgressReadOnlyChecksDoNotCommit(t *testing.T) {
	s, _ := openTest(t)
	conv := newWork(t, s, "read-only")
	// Consume the setup commit signal. Polling must not create new commits.
	select {
	case <-s.Changes():
	default:
	}
	for i := 0; i < 100; i++ {
		_, err := s.PendingAtLeast(conv, 256)
		mustT(t, err)
		_, err = s.PendingProgress(conv)
		mustT(t, err)
		_, err = s.RepresentedRecord("missing:")
		mustT(t, err)
	}
	select {
	case <-s.Changes():
		t.Fatal("read-only progress checks committed a write transaction")
	default:
	}
}
