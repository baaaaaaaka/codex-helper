package skills

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// A manifest is an immutable part of an exported directory, not a mutable
// subscription database. DELETE journaling leaves one portable file after
// Close; the existing directory publisher can still replace it atomically.
func manifestURL(path string, readonly bool) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
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
	q := url.Values{}
	if readonly {
		q.Set("mode", "ro")
	} else {
		q.Add("_pragma", "synchronous(FULL)")
		q.Add("_pragma", "journal_mode(DELETE)")
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}
func writeManifest(path string, v exportManifest) error { return publishManifest(path, v, true) }
func publishManifest(path string, v exportManifest, replace bool) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".cxp-manifest-*.sqlite")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Close(); err != nil {
		return err
	}
	uri, err := manifestURL(tmp, false)
	if err != nil {
		return err
	}
	db, err := sql.Open("sqlite", uri)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, q := range []string{
		`CREATE TABLE manifest(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,source_id TEXT NOT NULL,source_name TEXT NOT NULL,remote_url TEXT NOT NULL,ref TEXT NOT NULL,commit_id TEXT NOT NULL,skill_name TEXT NOT NULL,source_path TEXT NOT NULL,export_name TEXT NOT NULL,exported_at TEXT NOT NULL)`,
		`CREATE TABLE files(position INTEGER PRIMARY KEY,rel_path TEXT NOT NULL,sha256 TEXT NOT NULL,size INTEGER NOT NULL,mode INTEGER NOT NULL)`,
		`PRAGMA user_version=1`,
	} {
		if _, err = tx.Exec(q); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(`INSERT INTO manifest VALUES(1,?,?,?,?,?,?,?,?,?,?)`, v.Version, v.SourceID, v.SourceName, v.RemoteURL, v.Ref, v.Commit, v.SkillName, v.SourcePath, v.ExportName, skillTime(v.ExportedAt)); err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO files VALUES(?,?,?,?,?)`)
	if err != nil {
		return err
	}
	for i, f := range v.Files {
		if _, err = stmt.Exec(i, f.RelPath, f.SHA256, f.Size, f.Mode); err != nil {
			stmt.Close()
			return err
		}
	}
	if err = stmt.Close(); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if err = db.Close(); err != nil {
		return err
	}
	if replace {
		return replaceSkillFile(tmp, path)
	}
	return os.Link(tmp, path)
}
func readExportManifest(path string) (exportManifest, bool, error) {
	var v exportManifest
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		// Migration is the only reader of the old format. After publication of
		// the native file, a leftover old file can never override it.
		if err = migrateExportManifest(path); err != nil {
			return v, false, err
		}
		if _, err = os.Stat(path); errors.Is(err, os.ErrNotExist) {
			return v, false, nil
		} else if err != nil {
			return v, false, err
		}
	} else if err != nil {
		return v, false, err
	}
	uri, err := manifestURL(path, true)
	if err != nil {
		return v, false, err
	}
	db, err := sql.Open("sqlite", uri)
	if err != nil {
		return v, false, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	var version int
	if err = db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return v, false, err
	}
	if version != 1 {
		return v, false, fmt.Errorf("unsupported skill manifest database version %d", version)
	}
	var at string
	err = db.QueryRow(`SELECT version,source_id,source_name,remote_url,ref,commit_id,skill_name,source_path,export_name,exported_at FROM manifest WHERE id=1`).Scan(&v.Version, &v.SourceID, &v.SourceName, &v.RemoteURL, &v.Ref, &v.Commit, &v.SkillName, &v.SourcePath, &v.ExportName, &at)
	if err != nil {
		return v, false, err
	}
	v.ExportedAt, err = parseSkillTime(at)
	if err != nil {
		return v, false, err
	}
	rows, err := db.Query(`SELECT rel_path,sha256,size,mode FROM files ORDER BY position`)
	if err != nil {
		return v, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var f FileManifest
		if err = rows.Scan(&f.RelPath, &f.SHA256, &f.Size, &f.Mode); err != nil {
			return v, false, err
		}
		v.Files = append(v.Files, f)
	}
	if err = rows.Err(); err != nil {
		return v, false, err
	}
	return v, true, nil
}
