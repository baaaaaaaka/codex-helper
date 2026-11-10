package asr

import (
	"database/sql"
	"os"
	"path/filepath"

	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

type serverRecordsDB struct{ *sql.DB }
type serverRegistration struct {
	db  *serverRecordsDB
	rec serverRecord
}

func openServerRecords(root string) (*serverRecordsDB, error) {
	dir := filepath.Join(root, "run")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	db, err := sqlitestate.Open(filepath.Join(dir, "servers.sqlite"), false)
	if err != nil {
		return nil, err
	}
	if err = migrateServerRecords(db, dir); err != nil {
		db.Close()
		return nil, err
	}
	return &serverRecordsDB{db}, nil
}

const serverRecordPut = `INSERT INTO servers(host,pid,identity,owner_pid,owner_identity) VALUES(?,?,?,?,?)
 ON CONFLICT(host,pid) DO UPDATE SET identity=excluded.identity,owner_pid=excluded.owner_pid,owner_identity=excluded.owner_identity
 WHERE (identity,owner_pid,owner_identity) IS NOT (excluded.identity,excluded.owner_pid,excluded.owner_identity)`

func (s *serverRecordsDB) put(r serverRecord) error {
	_, err := s.Exec(serverRecordPut, r.Host, r.PID, r.ID, r.OwnerPID, r.OwnerID)
	return err
}
func (s *serverRecordsDB) remove(r serverRecord) error {
	_, err := s.Exec(`DELETE FROM servers WHERE host=? AND pid=? AND identity=? AND owner_pid=? AND owner_identity=?`, r.Host, r.PID, r.ID, r.OwnerPID, r.OwnerID)
	return err
}
func (s *serverRecordsDB) page(host string, after int) ([]serverRecord, error) {
	rows, err := s.Query(`SELECT host,pid,identity,owner_pid,owner_identity FROM servers WHERE host=? AND pid>? ORDER BY pid LIMIT 128`, host, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []serverRecord
	for rows.Next() {
		var r serverRecord
		if err = rows.Scan(&r.Host, &r.PID, &r.ID, &r.OwnerPID, &r.OwnerID); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}
