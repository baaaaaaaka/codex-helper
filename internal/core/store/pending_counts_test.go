package store

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestPendingCountsAgreeAcrossStatesAndRollback(t *testing.T) {
	s, _ := openTest(t)
	c := newWork(t, s, "counts")
	check := func() {
		t.Helper()
		got, err := s.OutboxCounts()
		mustT(t, err)
		want, err := s.countByState(`SELECT state,COUNT(*) FROM outbox WHERE state IN ('queued','sending','unknown') GROUP BY state`)
		mustT(t, err)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("counts=%v want=%v", got, want)
		}
	}
	check()
	if err := s.Update(func(tx *Tx) error {
		for i := 0; i < 50; i++ {
			if _, err := tx.Enqueue(c, []OutMsg{{DeliveryKey: fmt.Sprint(i), Kind: OutProgress, Body: "body"}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	states := []string{OutSending, OutUnknown, OutSent, OutDropped, OutQueued}
	for i := 0; i < 10; i++ {
		for j, state := range states {
			if err := s.Update(func(tx *Tx) error {
				_, err := tx.exec(`UPDATE outbox SET state=? WHERE id%5=?`, state, (i+j)%5)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			check()
		}
	}
	abort := errors.New("rollback")
	if err := s.Update(func(tx *Tx) error {
		if _, err := tx.exec(`UPDATE outbox SET state='sent'`); err != nil {
			return err
		}
		return abort
	}); !errors.Is(err, abort) {
		t.Fatal(err)
	}
	check()
	if err := s.Update(func(tx *Tx) error { _, err := tx.exec(`UPDATE outbox SET state='sent'`); return err }); err != nil {
		t.Fatal(err)
	}
	check()
}
