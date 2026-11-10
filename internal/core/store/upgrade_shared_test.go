package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSharedSchemaUpgradeKeepsExistingQueuedRequests(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.sqlite")
	db, err := sql.Open("sqlite", path)
	mustT(t, err)
	for _, q := range schemaV1 {
		_, err = db.Exec(q)
		mustT(t, err)
	}
	_, err = db.Exec(fmt.Sprintf("PRAGMA application_id=%d", ApplicationID))
	mustT(t, err)
	_, err = db.Exec("PRAGMA user_version=1")
	mustT(t, err)
	_, err = db.Exec(`INSERT INTO conversations(id,label,kind,channel,state,created_at,updated_at) VALUES(1,'old','work','teams','active',1,1)`)
	mustT(t, err)
	_, err = db.Exec(`INSERT INTO inbound(id,conv_id,ext_msg_id,ext_time,received_at,sender,kind,state,body) VALUES(1,1,'old',1,1,'owner','prompt','queued','old queued input')`)
	mustT(t, err)
	_, err = db.Exec(`INSERT INTO turns(id,conv_id,inbound_id,turn_key,state,queued_at) VALUES(1,1,1,'cxp-old-1','queued',1)`)
	mustT(t, err)
	mustT(t, db.Close())
	s, err := Open(path)
	mustT(t, err)
	defer s.Close()
	tr, ok, err := one(s.r.QueryRow(`SELECT `+turnCols+` FROM turns WHERE id=1`), scanTurn)
	mustT(t, err)
	if !ok || !tr.Queue || tr.Steering || tr.TurnKey != "cxp-old-1" {
		t.Fatalf("old request changed: %+v", tr)
	}
	in, ok, err := one(s.r.QueryRow(`SELECT `+inboundCols+` FROM inbound WHERE id=1`), scanInbound)
	mustT(t, err)
	if !ok || !in.Queue || in.Body != "old queued input" {
		t.Fatalf("old input changed: %+v", in)
	}
}

// Check the migration's disk volume on a history-sized table. This catches
// an accidental table rebuild, rather than relying on a machine-speed limit.
func TestSharedSchemaMigrationDoesNotRewriteHistory(t *testing.T) {
	empty := filepath.Join(t.TempDir(), "empty-v1.sqlite")
	legacy, err := sql.Open("sqlite", empty)
	mustT(t, err)
	for _, q := range schemaV1 {
		_, err = legacy.Exec(q)
		mustT(t, err)
	}
	_, err = legacy.Exec(fmt.Sprintf("PRAGMA application_id=%d", ApplicationID))
	mustT(t, err)
	_, err = legacy.Exec("PRAGMA user_version=1")
	mustT(t, err)
	mustT(t, legacy.Close())
	e0, err := os.Stat(empty)
	mustT(t, err)
	upgraded, err := Open(empty)
	mustT(t, err)
	mustT(t, upgraded.Close())
	e1, err := os.Stat(empty)
	mustT(t, err)
	fixedGrowth := e1.Size() - e0.Size()
	path := filepath.Join(t.TempDir(), "large-v1.sqlite")
	db, err := sql.Open("sqlite", path)
	mustT(t, err)
	for _, q := range schemaV1 {
		_, err = db.Exec(q)
		mustT(t, err)
	}
	_, err = db.Exec(fmt.Sprintf("PRAGMA application_id=%d", ApplicationID))
	mustT(t, err)
	_, err = db.Exec("PRAGMA user_version=1")
	mustT(t, err)
	_, err = db.Exec(`INSERT INTO conversations(id,label,kind,channel,state,created_at,updated_at) VALUES(1,'old','work','teams','active',1,1)`)
	mustT(t, err)
	_, err = db.Exec(`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<20000)
 INSERT INTO inbound(conv_id,ext_msg_id,ext_time,received_at,sender,kind,state,body) SELECT 1,CAST(i AS TEXT),i,i,'owner','prompt','done',? FROM n`, strings.Repeat("history", 180))
	mustT(t, err)
	mustT(t, db.Close())
	before, err := os.Stat(path)
	mustT(t, err)
	start := time.Now()
	s, err := Open(path)
	mustT(t, err)
	_, err = s.w.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
	mustT(t, err)
	mustT(t, s.Close())
	after, err := os.Stat(path)
	mustT(t, err)
	t.Logf("migration: %d bytes history, %s, file growth %d bytes", before.Size(), time.Since(start), after.Size()-before.Size())
	if delta := after.Size() - before.Size(); delta-fixedGrowth > 64<<10 {
		t.Fatalf("migration grew by %d bytes beyond %d bytes fixed schema allocation", delta-fixedGrowth, fixedGrowth)
	}
}
