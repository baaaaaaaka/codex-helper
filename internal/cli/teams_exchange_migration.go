package cli

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

func decodeLegacyTeamsExchange(path string, v any) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("legacy handoff is not a regular file: %s", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	if err = d.Decode(v); err != nil {
		return err
	}
	if _, err = d.Token(); err != io.EOF {
		return fmt.Errorf("trailing legacy handoff data: %s", path)
	}
	return nil
}
func migrateTeamsExchange(db *sql.DB, anchor string) error {
	tx, err := db.Begin()
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
		return fmt.Errorf("unsupported CLI exchange schema %d", v)
	}
	if _, err = tx.Exec(exchangeDDL); err != nil {
		return err
	}
	// Legacy filenames are migration sources only. Runtime scans indexed rows.
	var sources []string
	dir := teamsCLIRequestDir(anchor)
	f, err := os.Open(dir)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil {
		defer f.Close()
		for {
			entries, e := f.ReadDir(128)
			if e != nil && e != io.EOF {
				return e
			}
			for _, ent := range entries {
				name := ent.Name()
				if !strings.HasSuffix(name, ".json") {
					continue
				}
				path := filepath.Join(dir, name)
				if strings.HasSuffix(name, ".result.json") {
					id := strings.TrimSuffix(name, ".result.json")
					var r *engine.CLIResult
					if err = decodeLegacyTeamsExchange(path, &r); err != nil {
						return err
					}
					if r == nil {
						return fmt.Errorf("null CLI result: %s", path)
					}
					info, err := ent.Info()
					if err != nil {
						return err
					}
					if err = exchangeResultInsert(tx, id, *r, info.ModTime().UnixMilli()); err != nil {
						return err
					}
				} else {
					id := strings.TrimSuffix(name, ".json")
					var r *engine.CLIRequest
					if err = decodeLegacyTeamsExchange(path, &r); err != nil {
						return err
					}
					if r == nil || r.ID != id {
						return fmt.Errorf("legacy CLI request identity mismatch: %s", path)
					}
					if _, err = tx.Exec(exchangeRequestInsert, exchangeRequestArgs(*r)...); err != nil {
						return err
					}
				}
				sources = append(sources, path)
			}
			if e == io.EOF {
				break
			}
		}
	}
	// A previously published result is authoritative, even if the old process
	// crashed before unlinking its request. Core tombstones still prevent replay.
	if _, err = tx.Exec(`DELETE FROM requests WHERE id IN(SELECT id FROM results)`); err != nil {
		return err
	}
	wf := filepath.Join(filepath.Dir(anchor), "workflow-request.json")
	if _, err = os.Lstat(wf); err == nil {
		var r *teamsWorkflowRequest
		if err = decodeLegacyTeamsExchange(wf, &r); err != nil {
			return err
		}
		if r == nil {
			return fmt.Errorf("null Workflow request")
		}
		if _, err = tx.Exec(`INSERT INTO workflow VALUES(1,'legacy',?,?)`, r.Enabled, r.WebhookURLFile); err != nil {
			return err
		}
		sources = append(sources, wf)
	} else if !os.IsNotExist(err) {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO exchange_imports VALUES(?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, path := range sources {
		if _, err = stmt.Exec(path); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(`PRAGMA user_version=1`); err != nil {
		return err
	}
	if err = sqlitestate.CommitSchema(tx, db); err != nil {
		return err
	}
	for _, path := range sources {
		if _, err = os.Lstat(path + ".migrated"); err == nil {
			return fmt.Errorf("handoff migration committed; archive exists: %s", path)
		} else if !os.IsNotExist(err) {
			return err
		}
		if err = os.Rename(path, path+".migrated"); err != nil {
			return err
		}
	}
	return nil
}
