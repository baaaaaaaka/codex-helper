package managedinstall

import (
	"bufio"
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

func legacyRecordPath(path string) string {
	return strings.TrimSuffix(path, filepath.Ext(path)) + ".json"
}
func migrateRecord(db *sql.DB, path string) error {
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var version int
	if err = tx.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return err
	}
	if version == 1 {
		return sqlitestate.CommitSchema(tx, db)
	}
	if version != 0 {
		return fmt.Errorf("unsupported install database version %d", version)
	}
	if _, err = tx.Exec(recordDDL); err != nil {
		return err
	}
	legacy := legacyRecordPath(path)
	info, err := os.Lstat(legacy)
	imported := err == nil
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if imported {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("legacy install record is not regular: %s", legacy)
		}
		f, err := os.Open(legacy)
		if err != nil {
			return err
		}
		defer f.Close()
		reader := bufio.NewReader(f)
		if b, _ := reader.Peek(3); string(b) == "\xef\xbb\xbf" {
			reader.Discard(3)
		}
		d := json.NewDecoder(reader)
		d.DisallowUnknownFields()
		var r *Record
		if err = d.Decode(&r); err != nil {
			return err
		}
		if r == nil {
			return fmt.Errorf("null install record")
		}
		if _, err = d.Token(); err != io.EOF {
			return fmt.Errorf("trailing install record data")
		}
		if err = putRecord(tx, *r, nil); err != nil {
			return err
		}
		if _, err = tx.Exec(`INSERT INTO install_imports VALUES(?)`, legacy); err != nil {
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
			return fmt.Errorf("install migration committed; archive exists: %s", legacy)
		} else if !os.IsNotExist(err) {
			return err
		}
		return os.Rename(legacy, legacy+".migrated")
	}
	return nil
}
