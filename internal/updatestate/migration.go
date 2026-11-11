package updatestate

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

const ddl = `CREATE TABLE auto_checks(id INTEGER PRIMARY KEY CHECK(id=1),next TEXT NOT NULL,failures INTEGER NOT NULL CHECK(failures BETWEEN 0 AND 6),last_error TEXT NOT NULL);
 CREATE TABLE pending_updates(id INTEGER PRIMARY KEY CHECK(id=1),schema_version INTEGER NOT NULL CHECK(schema_version=1),target_version TEXT NOT NULL,target_sha256 TEXT NOT NULL,previous_active TEXT NOT NULL,target_active TEXT NOT NULL,runtime_root TEXT NOT NULL,entry_path TEXT NOT NULL,record_path TEXT NOT NULL,request_id TEXT NOT NULL);
 CREATE TABLE activation_owners(pending_path TEXT PRIMARY KEY,version INTEGER NOT NULL CHECK(version=1),kind TEXT NOT NULL,target_version TEXT NOT NULL,created_at TEXT NOT NULL);
 CREATE TABLE activation_results(pending_path TEXT PRIMARY KEY,version INTEGER NOT NULL CHECK(version=1),status TEXT NOT NULL,message TEXT NOT NULL,source TEXT NOT NULL,dest TEXT NOT NULL,want TEXT NOT NULL,updated_at TEXT NOT NULL);
 CREATE TABLE update_imports(source_path TEXT PRIMARY KEY);`

func legacyNames(root string) ([]string, error) {
	f, err := os.Open(root)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var paths []string
	for {
		entries, err := f.ReadDir(128)
		if err != nil && err != io.EOF {
			return nil, err
		}
		for _, e := range entries {
			n := e.Name()
			if n == "auto-check.json" || n == "pending-update.json" || strings.HasSuffix(n, ".teams-activation.json") || strings.HasSuffix(n, ".activation.json") {
				paths = append(paths, filepath.Join(root, n))
			}
		}
		if err == io.EOF {
			return paths, nil
		}
	}
}
func hasLegacy(root string) (bool, error) {
	paths, err := legacyNames(root)
	return len(paths) != 0, err
}
func decodeLegacy(path string, out any) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("legacy update state is not regular: %s", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	if b, _ := r.Peek(3); string(b) == "\xef\xbb\xbf" {
		r.Discard(3)
	}
	d := json.NewDecoder(r)
	if err = d.Decode(out); err != nil {
		return err
	}
	if _, err = d.Token(); err != io.EOF {
		return fmt.Errorf("trailing update state: %s", path)
	}
	return nil
}
func migrate(db *sql.DB, root string) error {
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
		return fmt.Errorf("unsupported update state database version %d", v)
	}
	if _, err = tx.Exec(ddl + invocationDDL); err != nil {
		return err
	}
	paths, err := legacyNames(root)
	if err != nil {
		return err
	}
	for _, path := range paths {
		switch name := filepath.Base(path); {
		case name == "auto-check.json":
			var r *Check
			if err = decodeLegacy(path, &r); err != nil {
				return err
			}
			if r == nil {
				return fmt.Errorf("null update check")
			}
			_, err = tx.Exec(checkPut, stamp(r.Next), r.Failures, r.LastError)
		case name == "pending-update.json":
			var p *Pending
			if err = decodeLegacy(path, &p); err != nil {
				return err
			}
			if p == nil {
				return fmt.Errorf("null pending update")
			}
			_, err = tx.Exec(pendingPut, pendingArgs(*p)...)
		case strings.HasSuffix(name, ".teams-activation.json"):
			var r *ActivationOwner
			if err = decodeLegacy(path, &r); err != nil {
				return err
			}
			if r == nil {
				return fmt.Errorf("null activation owner")
			}
			_, err = tx.Exec(ownerPut, strings.TrimSuffix(path, ".teams-activation.json"), r.Version, r.Kind, r.TargetVersion, stamp(r.CreatedAt))
		case strings.HasSuffix(name, ".activation.json"):
			var r *ActivationResult
			if err = decodeLegacy(path, &r); err != nil {
				return err
			}
			if r == nil {
				return fmt.Errorf("null activation result")
			}
			_, err = tx.Exec(resultPut, strings.TrimSuffix(path, ".activation.json"), r.Version, r.Status, r.Message, r.Source, r.Dest, r.Want, stamp(r.UpdatedAt))
		}
		if err != nil {
			return fmt.Errorf("import update state %s: %w", path, err)
		}
		if _, err = tx.Exec(`INSERT INTO update_imports VALUES(?)`, path); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(`PRAGMA user_version=1`); err != nil {
		return err
	}
	if err = sqlitestate.CommitSchema(tx, db); err != nil {
		return err
	}
	for _, path := range paths {
		if _, err = os.Lstat(path + ".migrated"); err == nil {
			return fmt.Errorf("update state migration committed; archive exists: %s", path)
		} else if !os.IsNotExist(err) {
			return err
		}
		if err = os.Rename(path, path+".migrated"); err != nil {
			return err
		}
	}
	return nil
}
