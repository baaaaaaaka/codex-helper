package cli

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
	"os"
	"path/filepath"
)

func migrateWindowsAppState(db *sql.DB, root string) error {
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
		return fmt.Errorf("unsupported Windows app state database version %d", v)
	}
	if _, err = tx.Exec(windowsAppStateDDL); err != nil {
		return err
	}
	legacy := filepath.Join(root, "current.json")
	var state *codexWindowsManagedInstallState
	err = decodeLegacyService(legacy, &state)
	imported := err == nil
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if imported {
		if state == nil {
			return fmt.Errorf("null Windows app install state")
		}
		if _, err = tx.Exec(windowsAppStatePut, windowsAppStateArgs(*state)...); err != nil {
			return err
		}
		if _, err = tx.Exec(`INSERT INTO app_imports VALUES(?)`, legacy); err != nil {
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
		return archiveLegacyServiceFile(legacy)
	}
	return nil
}
