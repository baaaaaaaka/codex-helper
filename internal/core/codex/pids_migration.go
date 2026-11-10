package codex

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
	"io"
	"os"
	"strings"
)

func legacyPIDPath(path string) string { return strings.TrimSuffix(path, ".sqlite") }
func migratePIDDatabase(db *sql.DB, path string) error {
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
		return fmt.Errorf("unsupported process database version %d", v)
	}
	if _, err = tx.Exec(`CREATE TABLE app_servers(pid INTEGER PRIMARY KEY CHECK(pid>0),start_time TEXT NOT NULL,server_key TEXT NOT NULL,boot_id TEXT NOT NULL,pid_namespace TEXT NOT NULL);CREATE INDEX app_servers_key ON app_servers(server_key); CREATE TABLE process_imports(source_path TEXT PRIMARY KEY)`); err != nil {
		return err
	}
	legacy := legacyPIDPath(path)
	info, err := os.Lstat(legacy)
	imported := err == nil
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if imported {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("legacy PID record is not regular")
		}
		f, err := os.Open(legacy)
		if err != nil {
			return err
		}
		defer f.Close()
		d := json.NewDecoder(f)
		d.DisallowUnknownFields()
		token, err := d.Token()
		// Empty old files existed before the first successful server start.
		if err != io.EOF {
			if err != nil || token != json.Delim('[') {
				return fmt.Errorf("invalid legacy process array")
			}
			stmt, err := tx.Prepare(`INSERT INTO app_servers VALUES(?,?,?,?,?)`)
			if err != nil {
				return err
			}
			defer stmt.Close()
			for d.More() {
				var r pidRecord
				if err = d.Decode(&r); err != nil {
					return err
				}
				if _, err = stmt.Exec(r.PID, r.Start, r.Key, r.Boot, r.NS); err != nil {
					return err
				}
			}
			if _, err = d.Token(); err != nil {
				return err
			}
			if _, err = d.Token(); err != io.EOF {
				return fmt.Errorf("trailing process record data")
			}
		}
		if _, err = tx.Exec(`INSERT INTO process_imports VALUES(?)`, legacy); err != nil {
			return err
		}
		if err = f.Close(); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(`PRAGMA user_version=1`); err != nil {
		return err
	}
	if err = sqlitestate.CommitSchema(tx, db); err != nil {
		return err
	}
	if imported {
		if _, err = os.Lstat(legacy + ".migrated"); err == nil {
			return fmt.Errorf("process migration committed; archive exists")
		} else if !os.IsNotExist(err) {
			return err
		}
		return os.Rename(legacy, legacy+".migrated")
	}
	return nil
}
