package store

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestParkPolicyDefaultPersistenceAndNoOpWrites(t *testing.T) {
	s, clock := openTest(t)
	id := newWork(t, s, "one")
	var before Conversation
	mustT(t, s.Read(func(tx *Tx) error { before, _, _ = tx.Conversation(id); return nil }))
	if before.AutoParkOff {
		t.Fatal("new conversation disabled automatic parking")
	}
	clock.add(72 * time.Hour)
	mustT(t, s.Update(func(tx *Tx) error { return tx.SetAutoPark(id, false) }))
	var changes int64
	mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&changes))
	for i := 0; i < 10; i++ {
		mustT(t, s.Update(func(tx *Tx) error { return tx.SetAutoPark(id, false) }))
		mustT(t, s.Read(func(tx *Tx) error {
			c, _, err := tx.Conversation(id)
			if !c.AutoParkOff || c.UpdatedAt != before.UpdatedAt || c.State != ConvActive {
				t.Fatalf("policy changed activity/state: %+v", c)
			}
			return err
		}))
	}
	var after int64
	mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&after))
	if after != changes {
		t.Fatalf("unchanged policy/read wrote %d rows", after-changes)
	}
	// Both transaction orders: off before maintenance prevents park; park
	// before off remains parked, and manual park is always permitted.
	mustT(t, s.Update(func(tx *Tx) error {
		did, err := tx.AutoPark(id)
		if did {
			t.Fatal("off chat auto parked")
		}
		return err
	}))
	mustT(t, s.Update(func(tx *Tx) error { return tx.Park(id) }))
	mustT(t, s.Update(func(tx *Tx) error { return tx.SetAutoPark(id, true) }))
	mustT(t, s.Update(func(tx *Tx) error { return tx.Wake(id) }))
	mustT(t, s.Update(func(tx *Tx) error {
		did, err := tx.AutoPark(id)
		if !did {
			t.Fatal("on chat did not auto park")
		}
		return err
	}))
	mustT(t, s.Update(func(tx *Tx) error { return tx.SetAutoPark(id, false) }))
	failed := errors.New("rollback")
	wantErr(t, s.Update(func(tx *Tx) error {
		if err := tx.SetAutoPark(id, true); err != nil {
			return err
		}
		return failed
	}), failed)
	mustT(t, s.Release())
	mustT(t, s.Close())
	reopened := openAt(t, s.Path(), clock)
	mustT(t, reopened.Read(func(tx *Tx) error {
		c, _, err := tx.Conversation(id)
		if !c.AutoParkOff || c.State != ConvParked {
			t.Fatalf("restart/rollback lost policy or woke chat: %+v", c)
		}
		return err
	}))
}

func TestParkPolicyMigrationDefaultsExistingChatsOn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v7.sqlite")
	db, err := sql.Open("sqlite", path)
	mustT(t, err)
	for _, migration := range migrations[:7] {
		for _, q := range migration {
			_, err = db.Exec(q)
			mustT(t, err)
		}
	}
	_, err = db.Exec(fmt.Sprintf("PRAGMA application_id=%d", ApplicationID))
	mustT(t, err)
	_, err = db.Exec("PRAGMA user_version=7")
	mustT(t, err)
	_, err = db.Exec(`INSERT INTO conversations(label,kind,channel,state,created_at,updated_at) VALUES('old','work','teams','parked',1,2)`)
	mustT(t, err)
	mustT(t, db.Close())
	s := openAt(t, path, newClock())
	mustT(t, s.Read(func(tx *Tx) error {
		c, ok, err := tx.Conversation(1)
		if !ok || c.AutoParkOff || c.State != ConvParked || c.UpdatedAt != 2 {
			t.Fatalf("migration changed chat: %+v", c)
		}
		return err
	}))
}
