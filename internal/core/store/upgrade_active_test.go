package store

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

func TestActiveIndexMigrationPreservesUnconfirmedEvidence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v4.sqlite")
	db, err := sql.Open("sqlite", path)
	mustT(t, err)
	for _, migration := range migrations[:4] {
		for _, q := range migration {
			_, err = db.Exec(q)
			mustT(t, err)
		}
	}
	_, err = db.Exec(fmt.Sprintf("PRAGMA application_id=%d", ApplicationID))
	mustT(t, err)
	_, err = db.Exec("PRAGMA user_version=4")
	mustT(t, err)
	_, err = db.Exec(`INSERT INTO conversations(id,label,kind,channel,state,created_at,updated_at) VALUES(1,'old','work','teams','active',1,1)`)
	mustT(t, err)
	for i, state := range []string{OutUnknown, OutQueued, OutSending, OutSent, OutDropped} {
		_, err = db.Exec(`INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,body_hash,state,attempt_token,created_at,note) VALUES(1,?,?,'final','exact original content',X'010203',?,'original-token',1,'original note')`, fmt.Sprint(i), i+1, state)
		mustT(t, err)
	}
	mustT(t, db.Close())
	s, err := Open(path)
	mustT(t, err)
	defer s.Close()
	var intact, version, index int
	mustT(t, s.w.QueryRow(`SELECT COUNT(*) FROM outbox WHERE body='exact original content' AND body_hash=X'010203' AND attempt_token='original-token' AND note='original note'`).Scan(&intact))
	mustT(t, s.w.QueryRow(`PRAGMA user_version`).Scan(&version))
	mustT(t, s.w.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='outbox_active'`).Scan(&index))
	if intact != 5 || version != SchemaVersion || index != 1 {
		t.Fatalf("migration: intact=%d version=%d index=%d", intact, version, index)
	}
	var unknown int
	mustT(t, s.w.QueryRow(`SELECT COUNT(*) FROM outbox WHERE state='unknown'`).Scan(&unknown))
	if unknown != 1 {
		t.Fatal("migration settled or retried unknown content")
	}
}
