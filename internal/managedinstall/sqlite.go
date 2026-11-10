package managedinstall

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

const recordDDL = `CREATE TABLE install_record(id INTEGER PRIMARY KEY CHECK(id=1),schema_version INTEGER NOT NULL,target_path TEXT NOT NULL,target_source TEXT NOT NULL,target_state TEXT NOT NULL,repo TEXT NOT NULL,version TEXT NOT NULL,goos TEXT NOT NULL,goarch TEXT NOT NULL,updated_at TEXT NOT NULL,has_shims INTEGER NOT NULL);
 CREATE TABLE install_shims(position INTEGER PRIMARY KEY,path TEXT NOT NULL);
 CREATE TABLE install_imports(source_path TEXT PRIMARY KEY);`
const recordPut = `INSERT INTO install_record VALUES(1,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET schema_version=excluded.schema_version,target_path=excluded.target_path,target_source=excluded.target_source,target_state=excluded.target_state,repo=excluded.repo,version=excluded.version,goos=excluded.goos,goarch=excluded.goarch,updated_at=excluded.updated_at,has_shims=excluded.has_shims WHERE (schema_version,target_path,target_source,target_state,repo,version,goos,goarch,updated_at,has_shims) IS NOT (excluded.schema_version,excluded.target_path,excluded.target_source,excluded.target_state,excluded.repo,excluded.version,excluded.goos,excluded.goarch,excluded.updated_at,excluded.has_shims)`

func recordPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		var err error
		path, err = DefaultRecordPath()
		if err != nil {
			return "", err
		}
	}
	if strings.EqualFold(filepath.Ext(path), ".json") {
		path = strings.TrimSuffix(path, filepath.Ext(path)) + ".sqlite"
	}
	return filepath.Abs(path)
}
func openRecord(path string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	db, err := sqlitestate.Open(path, false)
	if err != nil {
		return nil, err
	}
	if err = migrateRecord(db, path); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}
func LoadRecord(path string) (Record, error) {
	path, err := recordPath(path)
	if err != nil {
		return Record{}, err
	}
	if _, err = sqlitestate.Inspect(path); os.IsNotExist(err) {
		if _, err = os.Lstat(legacyRecordPath(path)); err != nil {
			return Record{}, err
		}
		db, err := openRecord(path)
		if err != nil {
			return Record{}, err
		}
		db.Close()
	} else if err != nil {
		return Record{}, err
	}
	db, err := sqlitestate.Open(path, true)
	if err != nil {
		return Record{}, err
	}
	defer db.Close()
	var version int
	if err = db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return Record{}, err
	}
	if version == 0 {
		// Open has already rejected a sealed schema-zero database. A failed
		// first migration is unsealed and must re-enter its sole importer.
		db.Close()
		migrated, err := openRecord(path)
		if err != nil {
			return Record{}, err
		}
		migrated.Close()
		return LoadRecord(path)
	}
	if version != 1 {
		return Record{}, fmt.Errorf("unsupported install database version %d", version)
	}
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Record{}, err
	}
	defer tx.Rollback()
	record, err := readRecord(tx)
	if err != nil {
		return record, err
	}
	return record, tx.Commit()
}
func readRecord(tx *sql.Tx) (Record, error) {
	var r Record
	var shims bool
	err := tx.QueryRow(`SELECT schema_version,target_path,target_source,target_state,repo,version,goos,goarch,updated_at,has_shims FROM install_record WHERE id=1`).Scan(&r.SchemaVersion, &r.TargetPath, &r.TargetSource, &r.TargetState, &r.Repo, &r.Version, &r.GOOS, &r.GOARCH, &r.UpdatedAt, &shims)
	if errors.Is(err, sql.ErrNoRows) {
		return r, os.ErrNotExist
	}
	if err != nil {
		return r, err
	}
	if r.SchemaVersion != RecordSchemaVersion {
		return r, fmt.Errorf("unsupported install record schema %d", r.SchemaVersion)
	}
	if shims {
		r.Shims = []string{}
	}
	rows, err := tx.Query(`SELECT path FROM install_shims ORDER BY position`)
	if err != nil {
		return r, err
	}
	defer rows.Close()
	for rows.Next() {
		var path string
		if err = rows.Scan(&path); err != nil {
			return r, err
		}
		r.Shims = append(r.Shims, path)
	}
	return r, rows.Err()
}
func sameRecord(a, b Record) bool {
	return a.SchemaVersion == b.SchemaVersion && a.TargetPath == b.TargetPath && a.TargetSource == b.TargetSource && a.TargetState == b.TargetState && a.Repo == b.Repo && a.Version == b.Version && a.GOOS == b.GOOS && a.GOARCH == b.GOARCH && slices.Equal(a.Shims, b.Shims) && (a.Shims == nil) == (b.Shims == nil)
}
func putRecord(tx *sql.Tx, r Record, old *Record) error {
	if r.SchemaVersion == 0 {
		r.SchemaVersion = RecordSchemaVersion
	}
	if r.SchemaVersion != RecordSchemaVersion {
		return fmt.Errorf("unsupported install record schema %d", r.SchemaVersion)
	}
	if r.UpdatedAt == "" {
		if old != nil && sameRecord(r, *old) {
			r.UpdatedAt = old.UpdatedAt
		} else {
			r.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
		}
	}
	if _, err := tx.Exec(recordPut, r.SchemaVersion, r.TargetPath, r.TargetSource, r.TargetState, r.Repo, r.Version, r.GOOS, r.GOARCH, r.UpdatedAt, r.Shims != nil); err != nil {
		return err
	}
	if old != nil && slices.Equal(old.Shims, r.Shims) {
		return nil
	}
	if _, err := tx.Exec(`DELETE FROM install_shims`); err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO install_shims VALUES(?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for i, path := range r.Shims {
		if _, err = stmt.Exec(i, path); err != nil {
			return err
		}
	}
	return nil
}
func SaveRecord(path string, r Record) error {
	path, err := recordPath(path)
	if err != nil {
		return err
	}
	db, err := openRecord(path)
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	old, err := readRecord(tx)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	var previous *Record
	if err == nil {
		previous = &old
	}
	if err = putRecord(tx, r, previous); err != nil {
		return err
	}
	return tx.Commit()
}
