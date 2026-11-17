package store

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// Run this inert store fixture under strace; phase markers delimit actual
// journal writes and sync calls from schema/fixture preparation.
func TestPendingIndexSyscallProbe(t *testing.T) {
	if os.Getenv("CXP_PENDING_INDEX_IO_PROBE") != "1" {
		t.Skip("opt-in syscall probe")
	}
	s, _ := openTest(t)
	c := newWork(t, s, "index-io")
	if err := s.Update(func(tx *Tx) error {
		_, err := tx.exec(`WITH RECURSIVE n(x) AS(SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<1000) INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,created_at) SELECT ?,'p-'||x,x,'progress',?,'queued',1 FROM n`, c, strings.Repeat("x", 1024))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"old", "cover"} {
		if mode == "old" {
			restoreOldPendingIndex(t, s)
		} else {
			_, err := s.w.Exec(`DROP INDEX outbox_pending; CREATE INDEX outbox_pending ON outbox(conv_id,seq,state) WHERE state IN ('queued','sending','unknown')`)
			mustT(t, err)
		}
		for _, batch := range []int{1, 100} {
			label := fmt.Sprintf("%s-state-%d", mode, batch)
			checkpointPendingProbe(t, s)
			fmt.Fprintln(os.Stderr, "CXP_PENDING_IO_PHASE", label+"-begin")
			for i := 0; i < 10; i++ {
				state := OutSending
				if i%2 == 1 {
					state = OutQueued
				}
				if err := s.Update(func(tx *Tx) error {
					_, err := tx.exec(`UPDATE outbox SET state=? WHERE id<=?`, state, batch)
					return err
				}); err != nil {
					t.Fatal(err)
				}
			}
			fmt.Fprintln(os.Stderr, "CXP_PENDING_IO_PHASE", label+"-end")
		}
		checkpointPendingProbe(t, s)
		fmt.Fprintln(os.Stderr, "CXP_PENDING_IO_PHASE", mode+"-enqueue-begin")
		for i := 0; i < 10; i++ {
			if err := s.Update(func(tx *Tx) error {
				_, err := tx.Enqueue(c, []OutMsg{{DeliveryKey: fmt.Sprintf("io-%s-%d", mode, i), Kind: OutProgress, Body: strings.Repeat("x", 1024)}})
				return err
			}); err != nil {
				t.Fatal(err)
			}
		}
		fmt.Fprintln(os.Stderr, "CXP_PENDING_IO_PHASE", mode+"-enqueue-end")
	}
}

func checkpointPendingProbe(t *testing.T, s *Store) {
	t.Helper()
	var busy, log, done int
	mustT(t, s.w.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &log, &done))
	if busy != 0 {
		t.Fatal("checkpoint busy")
	}
}
