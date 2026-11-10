package cli

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

const windowsAppStateDDL = `CREATE TABLE installed_app(id INTEGER PRIMARY KEY CHECK(id=1),package_name TEXT NOT NULL,package_version TEXT NOT NULL,architecture TEXT NOT NULL,publisher TEXT NOT NULL,package_sha256 TEXT NOT NULL,executable_sha256 TEXT NOT NULL,runtime_relative TEXT NOT NULL,installed_at TEXT NOT NULL); CREATE TABLE app_imports(source_path TEXT PRIMARY KEY)`
const windowsAppStatePut = `INSERT INTO installed_app VALUES(1,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET package_name=excluded.package_name,package_version=excluded.package_version,architecture=excluded.architecture,publisher=excluded.publisher,package_sha256=excluded.package_sha256,executable_sha256=excluded.executable_sha256,runtime_relative=excluded.runtime_relative,installed_at=excluded.installed_at WHERE (package_name,package_version,architecture,publisher,package_sha256,executable_sha256,runtime_relative,installed_at) IS NOT (excluded.package_name,excluded.package_version,excluded.architecture,excluded.publisher,excluded.package_sha256,excluded.executable_sha256,excluded.runtime_relative,excluded.installed_at)`

func openWindowsAppState(root string) (*sql.DB, error) {
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	db, err := sqlitestate.Open(filepath.Join(root, codexWindowsManagedCurrentState), false)
	if err != nil {
		return nil, err
	}
	if err = migrateWindowsAppState(db, root); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}
func loadWindowsAppState(root string) (codexWindowsManagedInstallState, bool, error) {
	path := filepath.Join(root, codexWindowsManagedCurrentState)
	if _, err := sqlitestate.Inspect(path); os.IsNotExist(err) {
		if _, err = os.Lstat(filepath.Join(root, "current.json")); os.IsNotExist(err) {
			return codexWindowsManagedInstallState{}, false, nil
		} else if err != nil {
			return codexWindowsManagedInstallState{}, false, err
		}
		db, err := openWindowsAppState(root)
		if err != nil {
			return codexWindowsManagedInstallState{}, false, err
		}
		db.Close()
	} else if err != nil {
		return codexWindowsManagedInstallState{}, false, err
	}
	db, err := sqlitestate.Open(path, true)
	if err != nil {
		return codexWindowsManagedInstallState{}, false, err
	}
	defer db.Close()
	var v int
	if err = db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		return codexWindowsManagedInstallState{}, false, err
	}
	if v == 0 {
		db.Close()
		migrated, err := openWindowsAppState(root)
		if err != nil {
			return codexWindowsManagedInstallState{}, false, err
		}
		migrated.Close()
		return loadWindowsAppState(root)
	}
	if v != 1 {
		return codexWindowsManagedInstallState{}, false, fmt.Errorf("unsupported Windows app state database version %d", v)
	}
	var s codexWindowsManagedInstallState
	err = db.QueryRow(`SELECT package_name,package_version,architecture,publisher,package_sha256,executable_sha256,runtime_relative,installed_at FROM installed_app WHERE id=1`).Scan(&s.PackageName, &s.PackageVersion, &s.Architecture, &s.Publisher, &s.PackageSHA256, &s.ExecutableSHA256, &s.RuntimeRelative, &s.InstalledAt)
	if errors.Is(err, sql.ErrNoRows) {
		return s, false, nil
	}
	return s, err == nil, err
}
func windowsAppStateArgs(s codexWindowsManagedInstallState) []any {
	return []any{s.PackageName, s.PackageVersion, s.Architecture, s.Publisher, s.PackageSHA256, s.ExecutableSHA256, s.RuntimeRelative, s.InstalledAt}
}
func writeCodexWindowsManagedState(root string, s codexWindowsManagedInstallState) error {
	db, err := openWindowsAppState(root)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec(windowsAppStatePut, windowsAppStateArgs(s)...)
	return err
}
