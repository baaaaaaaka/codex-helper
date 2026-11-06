package store

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"
)

// TestUnchangedPollAndScanWriteNothing: re-admitting a window that brings
// nothing new, re-upserting an unchanged source and re-advancing it to the
// same position must not write. It is measured three ways: total_changes()
// of the writer connection, PRAGMA data_version seen from another
// connection, and the WAL file size.
func TestUnchangedPollAndScanWriteNothing(t *testing.T) {
	s, c := openTest(t)
	a := newWork(t, s, "a")
	msgs := []InboundMsg{prompt("p1", 1), prompt("p2", 2), {ExtMsgID: "e", ExtTime: 3, Sender: SenderSelf, Kind: InEcho}}
	src := TranscriptSource{Path: "/r/a.jsonl", ThreadID: "th-a", ConvID: a, State: SrcTracking, Size: 500, MTime: 77, Inode: 9}
	var srcID int64
	outputs := []OutMsg{out("m-1", OutMirror), out("m-2", OutMirror)}
	poll := func() {
		mustT(t, s.Update(func(tx *Tx) error {
			_, err := tx.AdmitWindow("teams:chat:a", a, msgs, "cursor-1")
			return err
		}))
	}
	scan := func() {
		mustT(t, s.Update(func(tx *Tx) error {
			id, err := tx.UpsertSource(src)
			if err != nil {
				return err
			}
			srcID = id
			return tx.AdvanceSource(id, 500, 4, 500, 77, 9, outputs)
		}))
	}
	poll()
	scan()
	mustT(t, s.Update(func(tx *Tx) error { return tx.SetMeta("k", "v") }))
	// Let the WAL settle: a checkpoint so later growth is visible.
	_, err := s.w.Exec(`PRAGMA wal_checkpoint(PASSIVE)`)
	mustT(t, err)

	obs, err := sql.Open("sqlite", dsn(s.Path(), true))
	mustT(t, err)
	defer obs.Close()
	conn, err := obs.Conn(context.Background())
	mustT(t, err)
	defer conn.Close()
	dataVersion := func() int64 {
		var v int64
		mustT(t, conn.QueryRowContext(context.Background(), `PRAGMA data_version`).Scan(&v))
		return v
	}
	totalChanges := func() int64 {
		var v int64
		mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&v))
		return v
	}
	walSize := func() int64 {
		st, err := os.Stat(s.Path() + "-wal")
		if err != nil {
			return -1
		}
		return st.Size()
	}

	dv0, tc0, wal0 := dataVersion(), totalChanges(), walSize()
	for i := 0; i < 50; i++ {
		c.add(time.Second)
		poll()
		scan()
		mustT(t, s.Update(func(tx *Tx) error { return tx.SetMeta("k", "v") }))
		mustT(t, s.Update(func(tx *Tx) error {
			_, err := tx.NextSendable(c.now().UnixMilli())
			return err
		}))
	}
	dv1, tc1, wal1 := dataVersion(), totalChanges(), walSize()
	if dv1 != dv0 || tc1 != tc0 || wal1 != wal0 {
		t.Fatalf("unchanged work wrote: data_version %d->%d total_changes %d->%d wal %d->%d", dv0, dv1, tc0, tc1, wal0, wal1)
	}

	// Sanity: a real change is visible to all three probes.
	mustT(t, s.Update(func(tx *Tx) error {
		_, err := tx.AdmitWindow("teams:chat:a", a, append(msgs, prompt("p3", 4)), "cursor-2")
		return err
	}))
	if dataVersion() == dv1 || totalChanges() == tc1 {
		t.Fatal("probes did not see a real write")
	}
	_ = srcID
}
