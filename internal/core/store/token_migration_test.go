package store

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"
)

func TestTokenIndexConversionRollbackAndRetryPreservesDelivery(t *testing.T) {
	s, _ := openTest(t)
	conv := newWork(t, s, "old")
	for i, state := range []string{OutQueued, OutSending, OutUnknown, OutSent, OutDropped} {
		_, err := s.w.Exec(`INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,attempt_token,created_at) VALUES(?,?,?,'final','original JSON payload',?,'old-token',1)`, conv, fmt.Sprint(i), i+1, state)
		mustT(t, err)
	}
	path := s.Path()
	mustT(t, s.Close())
	db, err := sql.Open("sqlite", path)
	mustT(t, err)
	defer db.Close()
	_, err = db.Exec(`DROP INDEX outbox_token; PRAGMA user_version=11; CREATE TABLE outbox_token(id INTEGER)`)
	mustT(t, err)
	if broken, err := Open(path); err == nil {
		broken.Close()
		t.Fatal("conversion unexpectedly accepted a name collision")
	}
	var version, intact int
	mustT(t, db.QueryRow(`PRAGMA user_version`).Scan(&version))
	mustT(t, db.QueryRow(`SELECT COUNT(*) FROM outbox WHERE body='original JSON payload' AND attempt_token='old-token'`).Scan(&intact))
	if version != 11 || intact != 5 {
		t.Fatalf("failed conversion changed data: version=%d intact=%d", version, intact)
	}
	_, err = db.Exec(`DROP TABLE outbox_token`)
	mustT(t, err)
	upgraded, err := Open(path)
	mustT(t, err)
	defer upgraded.Close()
	mustT(t, upgraded.w.QueryRow(`PRAGMA user_version`).Scan(&version))
	if version != SchemaVersion {
		t.Fatal(version)
	}
	for _, state := range []string{OutQueued, OutSending, OutUnknown, OutSent, OutDropped} {
		if countWhere(t, upgraded, `SELECT COUNT(*) FROM outbox WHERE state=? AND attempt_token='old-token'`, state) != 1 {
			t.Fatalf("delivery state %s changed", state)
		}
	}
	// Future schemas must be rejected rather than silently downgraded.
	_, err = db.Exec(fmt.Sprintf(`PRAGMA user_version=%d`, SchemaVersion+1))
	mustT(t, err)
	if newer, err := Open(path); !errors.Is(err, ErrUnknownVersion) {
		if newer != nil {
			newer.Close()
		}
		t.Fatalf("future schema: %v", err)
	}
}
