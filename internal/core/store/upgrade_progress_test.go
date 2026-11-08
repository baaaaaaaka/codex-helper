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

func TestProgressMigrationPreservesHistoryAndHasSmallDiskGrowth(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v3.sqlite")
	db, err := sql.Open("sqlite", path)
	mustT(t, err)
	for _, migration := range migrations[:3] {
		for _, q := range migration {
			_, err = db.Exec(q)
			mustT(t, err)
		}
	}
	_, err = db.Exec(fmt.Sprintf("PRAGMA application_id=%d", ApplicationID))
	mustT(t, err)
	_, err = db.Exec("PRAGMA user_version=3")
	mustT(t, err)
	_, err = db.Exec(`INSERT INTO conversations(id,label,kind,channel,state,created_at,updated_at) VALUES(1,'old','work','teams','active',1,1)`)
	mustT(t, err)
	_, err = db.Exec(`INSERT INTO turns(id,conv_id,turn_key,state,queued_at) VALUES(1,1,'cxp-old-1','completed',1)`)
	mustT(t, err)
	body := strings.Repeat("history ", 150)
	_, err = db.Exec(`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<20000)
INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,created_at)
SELECT 1,'old:'||i,i,'mirror',?,'sent',1 FROM n`, body)
	mustT(t, err)
	mustT(t, db.Close())
	before, err := os.Stat(path)
	mustT(t, err)
	started := time.Now()
	s, err := Open(path)
	mustT(t, err)
	var count, unchanged int
	mustT(t, s.w.QueryRow(`SELECT COUNT(*),SUM(body=? AND state='sent' AND progress_group IS NULL AND batch_head IS NULL AND batch_body IS NULL AND hold_turn IS NULL) FROM outbox`, body).Scan(&count, &unchanged))
	if count != 20000 || unchanged != count {
		t.Fatalf("history changed during migration: %d/%d intact", unchanged, count)
	}
	var progress, done bool
	mustT(t, s.w.QueryRow(`SELECT transcript_progress,transcript_done FROM turns WHERE id=1`).Scan(&progress, &done))
	if progress || done {
		t.Fatal("old helper execution became eligible for mass replay")
	}
	_, err = s.w.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
	mustT(t, err)
	mustT(t, s.Close())
	after, err := os.Stat(path)
	mustT(t, err)
	t.Logf("migration over %d bytes outbox: %s, disk growth %d bytes", before.Size(), time.Since(started), after.Size()-before.Size())
	if after.Size()-before.Size() > 128<<10 {
		t.Fatal("migration rewrote historical bodies or built a full-history index")
	}
}
