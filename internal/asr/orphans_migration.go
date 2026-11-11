package asr

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func migrateServerRecords(db *sql.DB, dir string) error {
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var v int
	if err = tx.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		return err
	}
	if v == 1 {
		return sqlitestate.CommitSchema(tx, db)
	}
	if v != 0 {
		return fmt.Errorf("unsupported ASR server database version %d", v)
	}
	if _, err = tx.Exec(`CREATE TABLE servers(host TEXT NOT NULL,pid INTEGER NOT NULL CHECK(pid>0),identity TEXT NOT NULL CHECK(identity<>''),owner_pid INTEGER NOT NULL CHECK(owner_pid>0),owner_identity TEXT NOT NULL CHECK(owner_identity<>''),PRIMARY KEY(host,pid)); CREATE TABLE server_imports(source_path TEXT PRIMARY KEY)`); err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO servers VALUES(?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var imported []string
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		var rec *serverRecord
		if err = decodeASRLegacy(path, &rec); err != nil {
			return fmt.Errorf("ASR process migration %s: %w", path, err)
		}
		if rec == nil {
			return fmt.Errorf("null ASR server record: %s", path)
		}
		if _, err = stmt.Exec(rec.Host, rec.PID, rec.ID, rec.OwnerPID, rec.OwnerID); err != nil {
			return err
		}
		if _, err = tx.Exec(`INSERT INTO server_imports VALUES(?)`, path); err != nil {
			return err
		}
		imported = append(imported, path)
	}
	if _, err = tx.Exec(`PRAGMA user_version=1`); err != nil {
		return err
	}
	if err = sqlitestate.CommitSchema(tx, db); err != nil {
		return err
	}
	for _, path := range imported {
		if err = archiveASRLegacy(path); err != nil {
			return err
		}
	}
	return nil
}
func decodeASRLegacy(path string, out any) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("legacy ASR state is not regular: %s", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	if err = d.Decode(out); err != nil {
		return err
	}
	if _, err = d.Token(); err != io.EOF {
		return fmt.Errorf("trailing ASR legacy data: %s", path)
	}
	return nil
}
func archiveASRLegacy(path string) error {
	if _, err := os.Lstat(path + ".migrated"); err == nil {
		return fmt.Errorf("ASR migration committed; archive exists: %s", path)
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.Rename(path, path+".migrated")
}
