package store

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestPendingIndexMigrationRollbackRetryAndCounts(t *testing.T) {
	s, _ := openTest(t)
	c := newWork(t, s, "pending-migration")
	for i, state := range []string{OutQueued, OutSending, OutUnknown, OutSent, OutDropped} {
		_, err := s.w.Exec(`INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,body_hash,state,created_at) VALUES(?,?,?,'final','unchanged',x'010203',?,1)`, c, fmt.Sprint(i), i+1, state)
		mustT(t, err)
	}
	restoreOldPendingIndex(t, s)
	path := s.Path()
	mustT(t, s.Close())
	db, err := sql.Open("sqlite", path)
	mustT(t, err)
	defer db.Close()
	_, err = db.Exec(`PRAGMA user_version=12`)
	mustT(t, err)
	// Fail after DROP and CREATE, while still inside the version transaction.
	old := migrations[12]
	migrations[12] = append(append([]string{}, old...), `SELECT no_such_function()`)
	t.Cleanup(func() { migrations[12] = old })
	broken, err := Open(path)
	if err == nil {
		broken.Close()
		t.Fatal("injected migration failure was ignored")
	}
	var version int
	var definition string
	mustT(t, db.QueryRow(`PRAGMA user_version`).Scan(&version))
	mustT(t, db.QueryRow(`SELECT sql FROM sqlite_master WHERE name='outbox_pending'`).Scan(&definition))
	if version != 12 || strings.Contains(definition, "seq,state") {
		t.Fatalf("migration did not roll back: version=%d index=%s", version, definition)
	}
	migrations[12] = old
	upgraded, err := Open(path)
	mustT(t, err)
	defer upgraded.Close()
	mustT(t, upgraded.w.QueryRow(`PRAGMA user_version`).Scan(&version))
	if version != SchemaVersion || countWhere(t, upgraded, `SELECT COUNT(*) FROM outbox WHERE body='unchanged' AND body_hash=x'010203'`) != 5 {
		t.Fatal("migration changed business data or version")
	}
	counts, err := upgraded.OutboxCounts()
	mustT(t, err)
	for _, state := range []string{OutQueued, OutSending, OutUnknown, OutSent, OutDropped} {
		if countWhere(t, upgraded, `SELECT COUNT(*) FROM outbox WHERE state=?`, state) != 1 {
			t.Fatal("migration changed state", state)
		}
		if state != OutSent && state != OutDropped && counts[state] != 1 {
			t.Fatal("incorrect pending counts", counts)
		}
	}
	rows, err := upgraded.w.Query(`EXPLAIN QUERY PLAN ` + pendingOutboxCountsSQL)
	mustT(t, err)
	defer rows.Close()
	plan := ""
	for rows.Next() {
		var id, parent, unused int
		var detail string
		mustT(t, rows.Scan(&id, &parent, &unused, &detail))
		plan += detail + "\n"
	}
	mustT(t, rows.Err())
	if !strings.Contains(plan, "COVERING INDEX outbox_pending") || strings.Contains(plan, "TEMP B-TREE") {
		t.Fatal(plan)
	}
	var integrity string
	mustT(t, upgraded.w.QueryRow(`PRAGMA integrity_check`).Scan(&integrity))
	if integrity != "ok" {
		t.Fatal(integrity)
	}
	foreign, err := upgraded.w.Query(`PRAGMA foreign_key_check`)
	mustT(t, err)
	if foreign.Next() {
		t.Fatal("foreign-key violation")
	}
	mustT(t, foreign.Err())
	mustT(t, foreign.Close())
	_, err = db.Exec(fmt.Sprintf(`PRAGMA user_version=%d`, SchemaVersion+1))
	mustT(t, err)
	if future, err := Open(path); !errors.Is(err, ErrUnknownVersion) {
		if future != nil {
			future.Close()
		}
		t.Fatalf("future schema was accepted: %v", err)
	}
}
