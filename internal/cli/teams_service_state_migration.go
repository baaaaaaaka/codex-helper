package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
)

type legacyServiceRestart struct{ Path, Host string }

func legacyRestartSources(dir string) ([]legacyServiceRestart, error) {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var sources []legacyServiceRestart
	for _, e := range entries {
		n := e.Name()
		if n != "service-restarts.json" && !(strings.HasPrefix(n, "service-restarts.") && strings.HasSuffix(n, ".json")) {
			continue
		}
		host := strings.TrimSuffix(strings.TrimPrefix(n, "service-restarts."), ".json")
		if n == "service-restarts.json" {
			host = ""
		}
		sources = append(sources, legacyServiceRestart{filepath.Join(dir, n), host})
	}
	return sources, nil
}
func serviceWatchdogLegacySources(path string) ([]string, error) {
	native := serviceNativePath(path)
	if strings.EqualFold(filepath.Ext(path), ".json") {
		return []string{path}, nil
	}
	standard, err := defaultTeamsServiceWatchdogStatePath()
	if err != nil {
		return nil, err
	}
	if filepath.Clean(native) != filepath.Clean(standard) {
		return []string{strings.TrimSuffix(native, ".sqlite") + ".json"}, nil
	}
	var sources []string
	current, err := appdirs.StatePath("teams", "service", "service-watchdog.json")
	if err != nil {
		return nil, err
	}
	sources = append(sources, current)
	if p, e := appdirs.LegacyCachePath("teams", "service-watchdog.json"); e == nil {
		sources = append(sources, p)
	}
	if p, e := appdirs.LegacyConfigPath("teams", "service-watchdog.json"); e == nil {
		sources = append(sources, p)
	}
	return sources, nil
}
func decodeLegacyService(path string, value any) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("legacy service state is not a regular file: %s", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	if err = d.Decode(value); err != nil {
		return err
	}
	if _, err = d.Token(); err != io.EOF {
		return fmt.Errorf("trailing or malformed service state: %s", path)
	}
	return nil
}
func migrateTeamsServiceState(db *sql.DB, path, host string, watchdog []string) error {
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
		return fmt.Errorf("unsupported service state version %d", v)
	}
	for _, q := range []string{
		`CREATE TABLE service_restarts(host TEXT PRIMARY KEY,streak INTEGER NOT NULL CHECK(streak>=0),last_start TEXT NOT NULL,wait_until TEXT NOT NULL)`,
		`CREATE TABLE service_watchdogs(host TEXT PRIMARY KEY,consecutive_stale INTEGER NOT NULL CHECK(consecutive_stale>=0),last_reason TEXT NOT NULL,last_action TEXT NOT NULL,last_action_at TEXT NOT NULL,updated_at TEXT NOT NULL)`,
		`CREATE TABLE service_imports(source_path TEXT PRIMARY KEY)`,
	} {
		if _, err = tx.Exec(q); err != nil {
			return err
		}
	}
	for _, q := range append([]string{SupervisorStatusDDL, ActivationDDL}, supervisorIdentitySchema...) {
		if _, err = tx.Exec(q); err != nil {
			return err
		}
	}
	restarts, err := legacyRestartSources(filepath.Dir(path))
	if err != nil {
		return err
	}
	var imported []string
	for _, source := range restarts {
		var rec *teamsServiceRestartRecord
		if err = decodeLegacyService(source.Path, &rec); err != nil {
			return err
		}
		if rec == nil {
			return fmt.Errorf("null service restart state: %s", source.Path)
		}
		if _, err = tx.Exec(`INSERT INTO service_restarts VALUES(?,?,?,?)`, source.Host, rec.Streak, serviceTime(rec.LastStart), serviceTime(rec.WaitUntil)); err != nil {
			return err
		}
		imported = append(imported, source.Path)
	}
	// Existing location order stays authoritative; each source is validated,
	// but an older cached copy cannot replace the preferred location.
	for _, source := range watchdog {
		var state *teamsServiceWatchdogState
		err = decodeLegacyService(source, &state)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if state == nil {
			return fmt.Errorf("null service watchdog state: %s", source)
		}
		if _, err = tx.Exec(`INSERT INTO service_watchdogs VALUES(?,?,?,?,?,?) ON CONFLICT(host) DO NOTHING`, host, state.ConsecutiveStale, state.LastReason, state.LastAction, serviceTime(state.LastActionAt), serviceTime(state.UpdatedAt)); err != nil {
			return err
		}
		imported = append(imported, source)
	}
	global, e := defaultTeamsServiceWatchdogStatePath()
	if e != nil {
		return e
	}
	if filepath.Clean(path) == filepath.Clean(global) {
		sources, e := importLegacySupervisorRuntime(tx, host)
		if e != nil {
			return e
		}
		imported = append(imported, sources...)
	}
	for _, source := range imported {
		if _, err = tx.Exec(`INSERT INTO service_imports VALUES(?)`, source); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(`PRAGMA user_version=1`); err != nil {
		return err
	}
	if err = sqlitestate.CommitSchema(tx, db); err != nil {
		return err
	}
	for _, source := range imported {
		if _, e := os.Lstat(source + ".migrated"); e == nil {
			return fmt.Errorf("service migration committed; archive exists: %s", source)
		} else if !os.IsNotExist(e) {
			return e
		}
		if err = os.Rename(source, source+".migrated"); err != nil {
			return err
		}
	}
	return nil
}
