package appgateway

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// No legacy format participates after the single authority-switch commit.
// JSON backups are used only by this importer, never to hide a corrupt native
// database or roll ownership back to a previous registration.
func migrateGatewayDatabase(db *sql.DB, dir string) error {
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
		return fmt.Errorf("unsupported gateway database version %d", version)
	}
	for _, q := range gatewaySchema {
		if _, err = tx.Exec(q); err != nil {
			return err
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var imported []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		v, err := readLegacyGatewayRegistration(path)
		if err != nil {
			backup, e := readLegacyGatewayRegistration(path + ".bak")
			if e != nil {
				return err
			}
			v = backup
			imported = append(imported, path+".bak")
		}
		if profileFilePart(v.ProfileID)+".json" != entry.Name() {
			return fmt.Errorf("%w: legacy registration filename/profile mismatch", ErrInvalidRegistration)
		}
		if err = putGatewayRegistration(tx, v); err != nil {
			return err
		}
		imported = append(imported, path)
	}
	// Import the previous log first, then the current log, one line at a time
	// with the same bounded retention as native event appends. Old releases
	// only appended to these diagnostic logs and never read them, so a torn
	// line from an interrupted append is skipped; the archive keeps its bytes.
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		for _, suffix := range []string{".1", ""} {
			path := filepath.Join(dir, entry.Name()+suffix)
			f, err := os.Open(path)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			r := bufio.NewReader(f)
			for {
				line, readErr := r.ReadBytes('\n')
				if readErr != nil && !errors.Is(readErr, io.EOF) {
					err = readErr
					break
				}
				var v *Event
				if len(bytes.TrimSpace(line)) != 0 && json.Unmarshal(line, &v) == nil {
					if v == nil || strings.TrimSpace(v.ProfileID) == "" || profileFilePart(v.ProfileID)+".jsonl" != entry.Name() {
						err = fmt.Errorf("invalid legacy gateway event scope")
						break
					}
					if err = appendGatewayEvent(tx, *v); err != nil {
						break
					}
				}
				if readErr != nil {
					break
				}
			}
			f.Close()
			if err != nil {
				return err
			}
			imported = append(imported, path)
		}
	}
	for _, path := range imported {
		if _, err = tx.Exec(`INSERT INTO gateway_imports VALUES(?)`, path); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(`PRAGMA user_version=1`); err != nil {
		return err
	}
	if err = sqlitestate.CommitSchema(tx, db); err != nil {
		return err
	}
	for _, path := range imported {
		if _, err = os.Lstat(path + ".migrated"); err == nil {
			return fmt.Errorf("gateway migration committed; archive already exists: %s", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err = os.Rename(path, path+".migrated"); err != nil {
			return fmt.Errorf("gateway migration committed; archive: %w", err)
		}
	}
	return nil
}

func readLegacyGatewayRegistration(path string) (Registration, error) {
	f, err := os.Open(path)
	if err != nil {
		return Registration{}, err
	}
	defer f.Close()
	var v *Registration
	d := json.NewDecoder(f)
	if err = d.Decode(&v); err != nil {
		return Registration{}, err
	}
	if v == nil {
		return Registration{}, fmt.Errorf("legacy gateway registration must be an object")
	}
	var extra any
	if err = d.Decode(&extra); !errors.Is(err, io.EOF) {
		return Registration{}, fmt.Errorf("trailing legacy gateway registration: %v", err)
	}
	return *v, Validate(*v)
}
