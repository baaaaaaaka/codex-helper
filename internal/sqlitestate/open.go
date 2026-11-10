// Package sqlitestate opens CXP's small configuration and peripheral state
// databases. Core and delegation databases retain their own lease/WAL policy.
package sqlitestate

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	_ "modernc.org/sqlite"
)

// Open returns one reusable connection. Rollback journals support independent
// processes without WAL shared memory; shared filesystems must still provide
// working SQLite file locks. Callers own schema checks and business transactions.
func Open(path string, readonly bool) (*sql.DB, error) {
	mode := "rwc"
	if readonly {
		mode = "ro"
	}
	return open(path, mode)
}

// OpenExisting permits updates but never creates a replacement for missing
// state that has already been published by its owner.
func OpenExisting(path string) (*sql.DB, error) { return open(path, "rw") }

func open(path, mode string) (*sql.DB, error) {
	readonly := mode == "ro"
	if _, err := Inspect(path); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if mode == "rwc" {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err == nil {
			err = f.Close()
		}
		if err != nil && !os.IsExist(err) {
			return nil, err
		}
	}
	info, err := Inspect(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("SQLite state is not a regular file: %s", path)
	}
	// Windows Chmod controls the read-only bit, not the file's DACL. Repeating
	// it cannot establish Unix permissions and would dirty metadata on open.
	if !readonly && runtime.GOOS != "windows" && info.Mode().Perm() != 0600 {
		if err = os.Chmod(path, 0600); err != nil {
			return nil, err
		}
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	slash := filepath.ToSlash(abs)
	u := url.URL{Scheme: "file", Path: slash}
	if len(slash) >= 2 && slash[1] == ':' {
		u.Path = "/" + slash
	}
	if strings.HasPrefix(slash, "//") {
		u.Host, u.Path, _ = strings.Cut(strings.TrimPrefix(slash, "//"), "/")
		u.Path = "/" + u.Path
	}
	q := url.Values{"_txlock": {"immediate"}, "_pragma": {"busy_timeout(5000)", "foreign_keys(1)", "synchronous(FULL)"}}
	q.Set("mode", mode)
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	var journal string
	if err = db.QueryRow(`PRAGMA journal_mode`).Scan(&journal); err == nil && journal != "delete" {
		err = fmt.Errorf("unsupported peripheral state journal mode %q: %s", journal, path)
	}
	if err == nil {
		err = CheckPublishedSchema(db)
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}
