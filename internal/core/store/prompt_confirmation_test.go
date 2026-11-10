package store

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPromptConfirmationMigrationKeepsHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v6.sqlite")
	db, err := sql.Open("sqlite", path)
	mustT(t, err)
	for _, migration := range migrations[:6] {
		for _, q := range migration {
			_, err = db.Exec(q)
			mustT(t, err)
		}
	}
	_, err = db.Exec(fmt.Sprintf("PRAGMA application_id=%d", ApplicationID))
	mustT(t, err)
	_, err = db.Exec("PRAGMA user_version=6")
	mustT(t, err)
	_, err = db.Exec(`INSERT INTO conversations(id,label,kind,channel,state,created_at,updated_at) VALUES(1,'old','work','teams','active',1,1)`)
	mustT(t, err)
	body := strings.Repeat("old history ", 150)
	_, err = db.Exec(`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<20000)
 INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,created_at)
 SELECT 1,'old:'||i,i,'mirror',?,'sent',1 FROM n`, body)
	mustT(t, err)
	mustT(t, db.Close())
	before, err := os.Stat(path)
	mustT(t, err)
	s, err := Open(path)
	mustT(t, err)
	var intact int
	mustT(t, s.r.QueryRow(`SELECT count(*) FROM outbox WHERE append_to IS NULL AND body=? AND state='sent'`, body).Scan(&intact))
	if intact != 20000 {
		t.Fatalf("changed history: %d", intact)
	}
	_, err = s.w.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
	mustT(t, err)
	mustT(t, s.Close())
	after, err := os.Stat(path)
	mustT(t, err)
	if growth := after.Size() - before.Size(); growth > 64<<10 {
		t.Fatalf("metadata-only migration rewrote history: %d bytes", growth)
	}
	t.Logf("migration: %d history rows, %d bytes growth", intact, after.Size()-before.Size())
}

func TestPromptConfirmationTargetAndFallbackPersist(t *testing.T) {
	s, _ := openTest(t)
	c := newWork(t, s, "confirmation")
	mustT(t, s.Update(func(tx *Tx) error {
		_, err := tx.Enqueue(c, []OutMsg{{DeliveryKey: "ack:1", Kind: OutAck, Body: "accepted", AppendTo: "original"}})
		if err != nil {
			return err
		}
		_, err = tx.BeginSend(1)
		return err
	}))
	mustT(t, s.Read(func(tx *Tx) error {
		row, _, err := tx.Outbox(1)
		if row.AppendTo != "original" {
			t.Errorf("target lost: %+v", row)
		}
		return err
	}))
	abort := errors.New("abort fallback")
	if err := s.Update(func(tx *Tx) error {
		if err := tx.SeparateConfirmation(1); err != nil {
			return err
		}
		return abort
	}); !errors.Is(err, abort) {
		t.Fatal(err)
	}
	mustT(t, s.Read(func(tx *Tx) error {
		row, _, err := tx.Outbox(1)
		if row.AppendTo != "original" {
			t.Error("rolled back fallback was retained")
		}
		return err
	}))
	mustT(t, s.Update(func(tx *Tx) error { return tx.SeparateConfirmation(1) }))
	mustT(t, s.Read(func(tx *Tx) error {
		row, _, err := tx.Outbox(1)
		if row.AppendTo != "" || row.State != OutSending {
			t.Errorf("fallback lost: %+v", row)
		}
		return err
	}))
	// A separate post owns only its own external id.
	mustT(t, s.Update(func(tx *Tx) error { return tx.MarkSent(1, "separate-post") }))
	mustT(t, s.Read(func(tx *Tx) error {
		own, err := tx.IsOwn(c, "original", nil, 0)
		if own {
			t.Error("original marked as helper post")
		}
		return err
	}))
}
