package store

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

func TestUpstreamSchemaUpgradePreservesGoalsAndOutbox(t *testing.T) {
	for _, initial := range []int{10, 11} {
		t.Run(fmt.Sprint(initial), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "upstream.sqlite")
			db, err := sql.Open("sqlite", path)
			mustT(t, err)
			for _, migration := range migrations[:initial] {
				for _, q := range migration {
					_, err = db.Exec(q)
					mustT(t, err)
				}
			}
			_, err = db.Exec(fmt.Sprintf("PRAGMA application_id=%d", ApplicationID))
			mustT(t, err)
			_, err = db.Exec(fmt.Sprintf("PRAGMA user_version=%d", initial))
			mustT(t, err)
			_, err = db.Exec(`INSERT INTO conversations(id,label,kind,channel,state,created_at,updated_at) VALUES(1,'old','work','teams','active',1,1)`)
			mustT(t, err)
			_, err = db.Exec(`INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,attempt_token,created_at) VALUES(1,'delivery',1,'final','original','unknown','token',1)`)
			mustT(t, err)
			if initial == 11 {
				_, err = db.Exec(`INSERT INTO goal_controls(conv_id,inbound_id,thread_id,cwd,turn_id,native_turn,pending) VALUES(1,2,'thread','cwd',3,'native',1)`)
				mustT(t, err)
			}
			mustT(t, db.Close())
			s, err := Open(path)
			mustT(t, err)
			defer s.Close()
			var version, indexes, rows int
			mustT(t, s.w.QueryRow(`PRAGMA user_version`).Scan(&version))
			mustT(t, s.w.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name IN ('outbox_token','outbox_pending','goal_controls_pending')`).Scan(&indexes))
			mustT(t, s.w.QueryRow(`SELECT COUNT(*) FROM outbox WHERE body='original' AND state='unknown' AND attempt_token='token'`).Scan(&rows))
			if version != SchemaVersion || indexes != 3 || rows != 1 {
				t.Fatalf("version=%d indexes=%d outbox=%d", version, indexes, rows)
			}
			if initial == 11 && countWhere(t, s, `SELECT COUNT(*) FROM goal_controls WHERE native_turn='native' AND pending=1`) != 1 {
				t.Fatal("goal control was lost")
			}
		})
	}
}
